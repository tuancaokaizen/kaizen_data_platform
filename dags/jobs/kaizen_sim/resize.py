"""One-off resize of the supply network to a new order volume, effective tomorrow:
1. BASELINE levels (s, S, safety stock) of shops and DCs are multiplied by `factor`;
2. each shop is topped up from its serving DC to s + (S - s) * U(0.3, 1) (same target as rebaseline);
3. what the DC keeps is cut to s + (S - s) * U(dc_lo, dc_hi), so purchase orders start within days and staggered;
   the rest goes back to the supplier.
All stock legs are ADJUST through inventory.apply_movement (ledger, balance and Outbox in one call).
Điều chỉnh một lần mạng cung ứng theo khối lượng đơn mới, hiệu lực từ ngày mai:
1. mức BASELINE (s, S, tồn an toàn) của shop và DC nhân với `factor`;
2. mỗi shop được DC phục vụ cấp thêm tới s + (S - s) * U(0,3, 1) (cùng mức đích với rebaseline);
3. phần DC giữ lại bị cắt về s + (S - s) * U(dc_lo, dc_hi), để đơn mua bắt đầu trong vài ngày và rải đều;
   phần còn lại trả nhà cung cấp.
Mọi chân kho là ADJUST qua inventory.apply_movement (sổ cái, tồn và Outbox trong một lần gọi).

Run in the tick pod / Chạy trong pod tick:
  kubectl exec -n compute deploy/kaizen-supply-tick -- python -m jobs.kaizen_sim.resize --factor 1.4286 [--mode apply]
"""
import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from . import db, rng
from .rebaseline import target_level
from .settings import ICT, PARAMS

REASON_TOPUP = "RESIZE"
REASON_RETURN = "RETURN_TO_SUPPLIER"


@dataclass(frozen=True)
class Level:
    reorder_point: int
    order_up_to: int
    safety_stock: int


def scale(level: Level, factor: float) -> Level:
    """Levels × factor, keeping 0 ≤ safety ≤ s ≤ S. / Mức × factor, giữ 0 ≤ an toàn ≤ s ≤ S."""
    s = max(1, round(level.reorder_point * factor))
    return Level(s, max(s, round(level.order_up_to * factor)), min(s, max(0, round(level.safety_stock * factor))))


def dc_target(dc: str, product_id: str, level: Level, lo: float, hi: float) -> int:
    u = rng.uniform(lo, hi, dc, product_id, "resize")
    return level.reorder_point + int((level.order_up_to - level.reorder_point) * u)


def plan(shop_on_hand: dict, dc_on_hand: dict, shop_levels: dict, dc_levels: dict, serving: dict,
         dc_lo: float, dc_hi: float, p=PARAMS) -> tuple:
    """(shop top-ups keyed (shop, product), DC cuts keyed (dc, product)) on already scaled levels.
    A DC short of stock splits what it has over its shops in proportion to their need.
    (lượng cấp thêm theo (shop, sản phẩm), lượng DC cắt theo (dc, sản phẩm)) trên mức đã nhân.
    DC thiếu hàng thì chia phần đang có cho các shop theo tỷ lệ nhu cầu.
    """
    need = {}
    for key, lv in sorted(shop_levels.items()):
        gap = target_level(key[0], key[1], lv.reorder_point, lv.order_up_to, p) - shop_on_hand.get(key, 0)
        if gap > 0:
            need[key] = gap
    asked = defaultdict(int)
    for (shop, pid), q in need.items():
        asked[(serving[shop], pid)] += q
    topups = {}
    for (shop, pid), q in need.items():
        key = (serving[shop], pid)
        ratio = min(1.0, dc_on_hand.get(key, 0) / asked[key])
        if int(q * ratio) > 0:
            topups[(shop, pid)] = int(q * ratio)
    given = defaultdict(int)
    for (shop, pid), q in topups.items():
        given[(serving[shop], pid)] += q
    cuts = {}
    for key, lv in sorted(dc_levels.items()):
        left = dc_on_hand.get(key, 0) - given[key]
        cut = left - dc_target(key[0], key[1], lv, dc_lo, dc_hi)
        if cut > 0:
            cuts[key] = cut
    return topups, cuts


def read_state(cur, t: date) -> tuple:
    cur.execute("""SELECT "LocationType", "LocationId", "ProductId", sum("OnHandQty")
                   FROM inventory."InventoryBalance" GROUP BY 1, 2, 3""")
    shop_on_hand, dc_on_hand = {}, {}
    for loc_type, loc, pid, qty in cur.fetchall():
        (shop_on_hand if loc_type == "SHOP" else dc_on_hand)[(loc, pid)] = int(qty)
    # Levels in force before T, so a re-run never scales twice / Mức hiệu lực trước T để chạy lại không nhân hai lần
    cur.execute("""SELECT "LocationType", "LocationId", "ProductId", "ReorderPoint", "OrderUpTo", "SafetyStock",
                          "ReviewPeriodDays", "PolicyType"
                   FROM inventory."ReplenishmentPolicy"
                   WHERE "Source" = 'BASELINE' AND "EffectiveFrom" < %s
                     AND ("EffectiveTo" IS NULL OR "EffectiveTo" >= %s)""", (t, t - timedelta(days=1)))
    policies = {(lt, loc, pid): (Level(int(rp), int(up), int(ss)), rv, pt)
                for lt, loc, pid, rp, up, ss, rv, pt in cur.fetchall()}
    cur.execute('SELECT "Code", "ServingWarehouseId" FROM shop."Shop"')
    serving = dict(cur.fetchall())
    return shop_on_hand, dc_on_hand, policies, serving


def write_policies(cur, policies: dict, factor: float, t: date) -> int:
    """Close the BASELINE rows on T - 1 and open the scaled ones on T. / Đóng dòng BASELINE ngày T - 1, mở dòng đã nhân từ T."""
    rows, closed = [], []
    for (lt, loc, pid), (lv, review, ptype) in sorted(policies.items()):
        new = scale(lv, factor)
        rows.append((f"RP-{loc}-{pid}-{t:%Y%m%d}", lt, loc, pid, ptype, review, new.reorder_point, new.order_up_to,
                     new.safety_stock, "BASELINE", None, t, None))
        closed.append((t - timedelta(days=1), lt, loc, pid, t))
    cur.executemany("""UPDATE inventory."ReplenishmentPolicy" SET "EffectiveTo" = %s, "ModifiedDate" = now()
                       WHERE "LocationType" = %s AND "LocationId" = %s AND "ProductId" = %s
                         AND "EffectiveTo" IS NULL AND "EffectiveFrom" < %s AND "Source" = 'BASELINE'""", closed)
    return db.upsert(cur, 'inventory."ReplenishmentPolicy"', ("Id",),
                     ("LocationType", "LocationId", "ProductId", "PolicyType", "ReviewPeriodDays", "ReorderPoint",
                      "OrderUpTo", "SafetyStock", "Source", "SolverRunId", "EffectiveFrom", "EffectiveTo"), rows)


def _lots(cur, location_type: str, location: str) -> dict:
    """Lot rows of one location, locked in post_sales_stock order. / Các lot của một vị trí, khoá theo thứ tự post_sales_stock."""
    cur.execute("""SELECT "ProductId", "LotId", "OnHandQty" FROM inventory."InventoryBalance"
                   WHERE "LocationType" = %s AND "LocationId" = %s
                   ORDER BY "LocationId", "ProductId", "LotId" FOR UPDATE""", (location_type, location))
    lots = defaultdict(list)
    for pid, lot, qty in cur.fetchall():
        lots[pid].append([lot, int(qty)])
    return lots


def _costs(cur) -> dict:
    cur.execute('SELECT "Id", "StockPrice" FROM product."Product"')
    return {pid: int(c) for pid, c in cur.fetchall()}


def apply_topups(cur, shop: str, dc: str, topups: dict, at: datetime, doc_id: str) -> int:
    """DC → shop legs for one shop, shop rows locked before DC rows (same order as tick deliveries).
    Hai chân DC → shop cho một shop, khoá dòng shop trước dòng DC (cùng thứ tự với giao hàng của tick).
    """
    _lots(cur, "SHOP", shop)
    dc_lots = _lots(cur, "DC", dc)
    cost = _costs(cur)
    moved = 0
    for pid, qty in sorted(topups.items()):
        for lot in sorted(dc_lots[pid], key=lambda x: x[0]):
            take = min(qty, lot[1])
            if take <= 0:
                continue
            line = f"{shop}:{pid}:{lot[0]}"
            db.apply_movement(cur, f"SM-{doc_id}-{dc}-{shop}-{pid}-{lot[0]}", "ADJUST", "DC", dc, pid, lot[0],
                              -take, cost[pid], "ADJ", doc_id, line, REASON_TOPUP, at)
            db.apply_movement(cur, f"SM-{doc_id}-{shop}-{pid}-{lot[0]}", "ADJUST", "SHOP", shop, pid, lot[0],
                              take, cost[pid], "ADJ", doc_id, line, REASON_TOPUP, at)
            lot[1] -= take
            qty -= take
            moved += take
    return moved


def apply_cuts(cur, dc: str, cuts: dict, at: datetime, doc_id: str) -> int:
    """Return DC surplus to the supplier, oldest lot first. / Trả hàng dư của DC cho nhà cung cấp, lot cũ trước."""
    dc_lots = _lots(cur, "DC", dc)
    cost = _costs(cur)
    returned = 0
    for pid, qty in sorted(cuts.items()):
        for lot in sorted(dc_lots[pid], key=lambda x: x[0]):
            take = min(qty, lot[1])
            if take <= 0:
                continue
            db.apply_movement(cur, f"SM-{doc_id}-{dc}-{pid}-{lot[0]}-RTS", "ADJUST", "DC", dc, pid, lot[0], -take,
                              cost[pid], "ADJ", doc_id, f"{dc}:{pid}:{lot[0]}", REASON_RETURN, at)
            qty -= take
            returned += take
    return returned


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("plan", "apply"), default="plan")
    parser.add_argument("--factor", type=float, required=True)
    parser.add_argument("--dc-lo", type=float, default=0.1)
    parser.add_argument("--dc-hi", type=float, default=0.8)
    args = parser.parse_args()
    now = datetime.now(ICT)
    t = now.date() + timedelta(days=1)
    doc_id = f"RESIZE-{t:%Y%m%d}"
    with db.connection() as conn:
        with conn.cursor() as cur:
            shop_on_hand, dc_on_hand, policies, serving = read_state(cur, t)
        conn.rollback()
        shop_levels = {(loc, pid): scale(v[0], args.factor) for (lt, loc, pid), v in policies.items() if lt == "SHOP"}
        dc_levels = {(loc, pid): scale(v[0], args.factor) for (lt, loc, pid), v in policies.items() if lt == "DC"}
        topups, cuts = plan(shop_on_hand, dc_on_hand, shop_levels, dc_levels, serving, args.dc_lo, args.dc_hi)
        by_dc = defaultdict(lambda: {"on_hand": 0, "to_shops": 0, "to_supplier": 0, "keep": 0})
        for (dc, pid), q in dc_on_hand.items():
            by_dc[dc]["on_hand"] += q
        for (shop, pid), q in topups.items():
            by_dc[serving[shop]]["to_shops"] += q
        for (dc, pid), q in cuts.items():
            by_dc[dc]["to_supplier"] += q
        for v in by_dc.values():
            v["keep"] = v["on_hand"] - v["to_shops"] - v["to_supplier"]
        result = {"mode": args.mode, "doc_id": doc_id, "effective_from": t, "factor": args.factor,
                  "policies": len(policies), "shop_on_hand": sum(shop_on_hand.values()),
                  "shop_topup_positions": len(topups), "shop_topup_units": sum(topups.values()),
                  "dc_cut_positions": len(cuts), "by_dc": dict(sorted(by_dc.items()))}
        if args.mode == "apply":
            if not db.run_started(conn, doc_id, "REBASELINE", now.date()):
                result["skipped"] = "already applied"
            else:
                done = {"policy_rows": db.retry(conn, lambda cur: write_policies(cur, policies, args.factor, t))}
                per_shop = defaultdict(dict)
                for (shop, pid), q in topups.items():
                    per_shop[shop][pid] = q
                done["to_shops"] = sum(
                    db.retry(conn, lambda cur, s=s: apply_topups(cur, s, serving[s], per_shop[s], now, doc_id))
                    for s in sorted(per_shop))
                per_dc = defaultdict(dict)
                for (dc, pid), q in cuts.items():
                    per_dc[dc][pid] = q
                done["to_supplier"] = sum(
                    db.retry(conn, lambda cur, d=d: apply_cuts(cur, d, per_dc[d], now, doc_id)) for d in sorted(per_dc))
                db.run_finished(conn, doc_id, "SUCCESS", done)
                result["applied"] = done
    print(json.dumps(result, ensure_ascii=False, indent=1, default=str))


if __name__ == "__main__":
    main()
