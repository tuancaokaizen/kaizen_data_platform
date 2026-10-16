"""One-off move of shop stock down to its policy level: each shop keeps s + (S - s) * U(lo, hi) per product
(staggered first orders) and the surplus becomes stock of its serving DC. Both legs are ADJUST, reason
REBASELINE, in one transaction per shop, so the network total never changes unless dc_cap_days drops it.
Chuyển một lần tồn shop xuống mức chính sách: mỗi shop giữ s + (S - s) * U(lo, hi) theo sản phẩm (lần đặt đầu
được rải ra) và phần dư thành tồn của DC phục vụ. Cả hai chân là ADJUST lý do REBASELINE, một transaction mỗi
shop, nên tổng toàn mạng không đổi trừ khi dc_cap_days cắt bớt.

Run locally / Chạy local:  python -m jobs.kaizen_sim.rebaseline [--mode plan|apply]
"""
import argparse
import json
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime

from . import db, rng
from .settings import ICT, OPENING_COVER_DAYS, PARAMS

REASON = "REBASELINE"


@dataclass(frozen=True)
class Move:
    shop: str
    dc: str
    product_id: str
    shop_out: int
    dc_in: int


def target_level(shop: str, product_id: str, reorder_point: int, order_up_to: int, p=PARAMS) -> int:
    u = rng.uniform(p.rebaseline_lo, p.rebaseline_hi, shop, product_id, "rebaseline")
    return reorder_point + int((order_up_to - reorder_point) * u)


def plan(on_hand: dict, levels: dict, serving: dict, dc_on_hand: dict, network_mu: dict, p=PARAMS) -> list:
    """Moves from current stock; on_hand and levels are keyed (shop, product), dc_on_hand and network_mu
    (dc, product). With dc_cap_days set, DC stock is capped at that many days of its shops' demand.
    Các lần chuyển từ tồn hiện tại; on_hand và levels theo (shop, sản phẩm), dc_on_hand và network_mu theo
    (dc, sản phẩm). Khi đặt dc_cap_days, tồn DC bị giới hạn bằng ngần ấy ngày nhu cầu của các shop nó phục vụ.
    """
    surplus = {}
    for key, qty in sorted(on_hand.items()):
        if key not in levels:
            continue
        s, big_s = levels[key]
        extra = qty - target_level(key[0], key[1], s, big_s, p)
        if extra > 0:
            surplus[key] = extra
    ratio = defaultdict(lambda: 1.0)
    if p.dc_cap_days is not None:
        totals = defaultdict(int)
        for (shop, pid), qty in surplus.items():
            totals[(serving[shop], pid)] += qty
        for key, total in totals.items():
            room = max(0.0, network_mu.get(key, 0.0) * p.dc_cap_days - dc_on_hand.get(key, 0))
            ratio[key] = min(1.0, room / total)
    return [Move(shop, serving[shop], pid, qty, int(qty * ratio[(serving[shop], pid)]))
            for (shop, pid), qty in sorted(surplus.items())]


def read_state(cur) -> tuple:
    cur.execute("""SELECT "LocationType", "LocationId", "ProductId", sum("OnHandQty")
                   FROM inventory."InventoryBalance" GROUP BY 1, 2, 3""")
    on_hand, dc_on_hand = {}, {}
    for loc_type, loc, pid, qty in cur.fetchall():
        (on_hand if loc_type == "SHOP" else dc_on_hand)[(loc, pid)] = int(qty)
    cur.execute("""SELECT "LocationId", "ProductId", "ReorderPoint", "OrderUpTo" FROM inventory."ReplenishmentPolicy"
                   WHERE "LocationType" = 'SHOP' AND "EffectiveTo" IS NULL""")
    levels = {(s, pid): (rp, up) for s, pid, rp, up in cur.fetchall()}
    cur.execute('SELECT "Code", "ServingWarehouseId" FROM shop."Shop"')
    serving = dict(cur.fetchall())
    cur.execute("""SELECT "LocationId", "ProductId", sum("Qty") FROM inventory."StockMovement"
                   WHERE "MovementType" = 'ADJUST' AND "Reason" = 'OPENING' AND "LocationType" = 'SHOP'
                   GROUP BY 1, 2""")
    network_mu = defaultdict(float)
    for shop, pid, qty in cur.fetchall():
        network_mu[(serving[shop], pid)] += int(qty) / OPENING_COVER_DAYS
    return on_hand, levels, serving, dc_on_hand, network_mu


def apply_shop(cur, shop: str, moves: list, at: datetime, doc_id: str) -> dict:
    """Both legs for one shop under its row locks; the surplus is re-read so concurrent sales are respected.
    Hai chân cho một shop dưới khoá dòng; phần dư được đọc lại để tôn trọng giao dịch bán đồng thời.
    """
    cur.execute("""SELECT 1 FROM inventory."StockMovement" WHERE "RefDocId" = %s AND "LocationId" = %s LIMIT 1""",
                (doc_id, shop))
    if cur.fetchone():
        return {"skipped": 1}
    # Same lock order as post_sales_stock / Cùng thứ tự khoá với post_sales_stock
    cur.execute("""SELECT b."ProductId", b."LotId", b."OnHandQty" FROM inventory."InventoryBalance" b
                   WHERE b."LocationType" = 'SHOP' AND b."LocationId" = %s
                   ORDER BY b."LocationId", b."ProductId", b."LotId" FOR UPDATE""", (shop,))
    lots = defaultdict(list)
    for pid, lot, qty in cur.fetchall():
        lots[pid].append((lot, qty))
    cur.execute('SELECT "Id", "StockPrice" FROM product."Product"')
    cost = dict(cur.fetchall())
    moved = {"shop_out": 0, "dc_in": 0}
    for m in sorted(moves, key=lambda m: m.product_id):
        held = sum(q for _, q in lots[m.product_id])
        out_qty = min(m.shop_out, held)
        dc_qty = min(m.dc_in, out_qty)
        for lot, qty in sorted(lots[m.product_id], key=lambda x: x[0]):
            if out_qty <= 0:
                break
            take = min(qty, out_qty)
            take_dc = min(take, dc_qty)
            line = f"{shop}:{m.product_id}:{lot}"
            db.apply_movement(cur, f"SM-{doc_id}-{shop}-{m.product_id}-{lot}", "ADJUST", "SHOP", shop, m.product_id,
                              lot, -take, cost[m.product_id], "ADJ", doc_id, line, REASON, at)
            if take_dc > 0:
                db.apply_movement(cur, f"SM-{doc_id}-{m.dc}-{shop}-{m.product_id}-{lot}", "ADJUST", "DC", m.dc,
                                  m.product_id, lot, take_dc, cost[m.product_id], "ADJ", doc_id, line, REASON, at)
            out_qty -= take
            dc_qty -= take_dc
            moved["shop_out"] += take
            moved["dc_in"] += take_dc
    return moved


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("plan", "apply"), default="plan")
    parser.add_argument("--dc-cap-days", type=float, default=None)
    args = parser.parse_args()
    params = replace(PARAMS, dc_cap_days=args.dc_cap_days)
    now = datetime.now(ICT)
    doc_id = f"{REASON}-{now:%Y%m%d}"
    with db.connection() as conn:
        with conn.cursor() as cur:
            state = read_state(cur)
        conn.rollback()
        moves = plan(*state, p=params)
        on_hand, levels, serving, dc_on_hand, network_mu = state
        by_dc = defaultdict(lambda: {"shop_out": 0, "dc_in": 0, "mu": 0.0})
        for m in moves:
            by_dc[m.dc]["shop_out"] += m.shop_out
            by_dc[m.dc]["dc_in"] += m.dc_in
        for (dc, _), mu in network_mu.items():
            by_dc[dc]["mu"] += mu
        report = {dc: {**v, "dc_cover_days": round(v["dc_in"] / v["mu"], 1) if v["mu"] else None}
                  for dc, v in sorted(by_dc.items())}
        result = {"mode": args.mode, "doc_id": doc_id, "moves": len(moves),
                  "shop_on_hand_before": sum(on_hand.values()), "by_dc": report}
        if args.mode == "apply":
            if not db.run_started(conn, doc_id, "REBASELINE", now.date()):
                result["skipped"] = "already applied"
            else:
                per_shop = defaultdict(list)
                for m in moves:
                    per_shop[m.shop].append(m)
                done = defaultdict(int)
                for shop in sorted(per_shop):
                    moved = db.retry(conn, lambda cur, s=shop: apply_shop(cur, s, per_shop[s], now, doc_id))
                    for k, v in moved.items():
                        done[k] += v
                db.run_finished(conn, doc_id, "SUCCESS", dict(done))
                result["applied"] = dict(done)
    print(json.dumps(result, ensure_ascii=False, indent=1, default=str))


if __name__ == "__main__":
    main()
