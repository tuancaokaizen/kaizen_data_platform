"""Daily close (supply_chain_forecast_optimization.md 4.2), run at ~00:10 ICT: closes business day D (expiry,
snapshot) and plans day T = D + 1 (weekly policy refresh, transfer orders, trips, purchase orders). Trip events
and stock moves are executed later by the tick. It also expires and tops up voucher codes (promotions.py). Every
step is idempotent and missed days are caught up.
Chốt sổ cuối ngày (supply_chain_forecast_optimization.md 4.2), chạy ~00:10 giờ VN: chốt ngày D (hết hạn,
snapshot) và lập kế hoạch ngày T = D + 1 (làm mới chính sách hằng tuần, lệnh điều chuyển, chuyến xe, đơn mua).
Sự kiện chuyến và biến động kho do tick thực hiện sau. Đồng thời cho hết hạn và phát bù mã voucher
(promotions.py). Mọi bước idempotent, ngày bị lỡ được chạy bù.
"""
import argparse
import json
import logging
import math
from collections import defaultdict
from datetime import date, datetime, time, timedelta

from . import db, geo, policy, promotions, routing
from .settings import (ICT, ON_DEMAND_CARRIER, PARAMS, STOP_SERVICE_MIN, VEHICLE_TYPES, WINDOW_CORE,
                       WINDOW_OTHER, SupplyParams)

log = logging.getLogger(__name__)


def start_of(d: date) -> datetime:
    return datetime.combine(d, time(0), ICT)


def end_of(d: date) -> datetime:
    return start_of(d + timedelta(days=1))


# ---------------------------------------------------------------------------------------------------------
# Close day D / Chốt ngày D
# ---------------------------------------------------------------------------------------------------------

def expire(cur, d: date) -> dict:
    """EXPIRE every lot whose expiry date is D or earlier, booked at the last second of D.
    EXPIRE mọi lô có hạn dùng từ D trở về trước, ghi vào giây cuối của D.
    """
    at = end_of(d) - timedelta(seconds=1)
    cur.execute("""SELECT b."LocationType", b."LocationId", b."ProductId", b."LotId", b."OnHandQty", p."StockPrice"
                   FROM inventory."InventoryBalance" b
                   JOIN inventory."InventoryLot" l ON l."Id" = b."LotId"
                   JOIN product."Product" p ON p."Id" = b."ProductId"
                   WHERE l."ExpiryDate" <= %s AND b."OnHandQty" > 0
                   ORDER BY b."LocationId", b."ProductId", b."LotId"
                   FOR UPDATE OF b""", (d,))
    n = qty = 0
    for loc_type, loc, pid, lot, on_hand, cost in cur.fetchall():
        db.apply_movement(cur, f"SM-EXPIRE-{d:%Y%m%d}-{loc}-{lot}", "EXPIRE", loc_type, loc, pid, lot, -on_hand,
                          cost, "ADJ", f"EXPIRE-{d:%Y%m%d}", lot, None, at)
        n += 1
        qty += on_hand
    return {"expired_lots": n, "expired_qty": qty}


SNAPSHOT_SQL = """
WITH pos AS (
    SELECT "LocationType" AS lt, "LocationId" AS loc, "ProductId" AS pid, sum("OnHandQty") AS qty
    FROM inventory."InventoryBalance" GROUP BY 1, 2, 3
    UNION
    SELECT "LocationType", "LocationId", "ProductId", 0 FROM inventory."ReplenishmentPolicy"
    WHERE "EffectiveFrom" <= %(d)s AND ("EffectiveTo" IS NULL OR "EffectiveTo" >= %(d)s)
), cur AS (
    SELECT lt, loc, pid, sum(qty) AS qty FROM pos GROUP BY 1, 2, 3
), mv AS (
    SELECT "LocationType" AS lt, "LocationId" AS loc, "ProductId" AS pid,
           sum("Qty") FILTER (WHERE "MovementTime" >= %(end)s) AS later,
           -sum("Qty") FILTER (WHERE "MovementType" = 'EXPIRE' AND "MovementTime" < %(end)s) AS expired,
           sum("Qty") FILTER (WHERE "MovementType" = 'ADJUST' AND "Reason" = 'OVERSELL'
                              AND "MovementTime" < %(end)s) AS oversell
    FROM inventory."StockMovement"
    WHERE "MovementTime" >= %(start)s
    GROUP BY 1, 2, 3
), transit AS (
    SELECT 'SHOP' AS lt, st."ShopCode" AS loc, si."ProductId" AS pid, sum(si."Qty") AS qty
    FROM logistics."ShipmentItem" si
    JOIN logistics."ShipmentStop" st ON st."Id" = si."ShipmentStopId"
    JOIN logistics."Shipment" sh ON sh."Id" = st."ShipmentId"
    WHERE sh."ShipmentDate" >= %(d)s - 14 AND sh."ActualDepartureTime" < %(end)s
      AND NOT EXISTS (SELECT 1 FROM logistics."ShipmentEvent" e
                      WHERE e."ShipmentStopId" = st."Id" AND e."EventType" IN ('DELIVERED', 'PARTIAL')
                        AND e."EventTime" < %(end)s)
    GROUP BY 1, 2, 3
), shop_order AS (
    SELECT 'SHOP' AS lt, t."ToShopCode" AS loc, i."ProductId" AS pid, sum(i."RequestedQty") AS qty
    FROM inventory."TransferOrder" t
    JOIN inventory."TransferOrderItem" i ON i."TransferOrderId" = t."Id"
    WHERE t."RequestedDate" >= %(d)s - 14 AND t."CreatedDate" < %(end)s AND t."Status" <> 'CANCELLED'
      AND NOT EXISTS (SELECT 1 FROM logistics."ShipmentStop" st
                      JOIN logistics."ShipmentEvent" e ON e."ShipmentId" = st."ShipmentId"
                      WHERE st."TransferOrderId" = t."Id" AND e."EventType" = 'PICKED' AND e."EventTime" < %(end)s)
    GROUP BY 1, 2, 3
), dc_order AS (
    SELECT 'DC' AS lt, po."WarehouseId" AS loc, i."ProductId" AS pid, sum(i."OrderedQty") AS qty
    FROM procurement."PurchaseOrder" po
    JOIN procurement."PurchaseOrderItem" i ON i."PurchaseOrderId" = po."Id"
    WHERE po."OrderDate" < %(end)s AND po."Status" <> 'CANCELLED'
      AND (po."ReceivedDate" IS NULL OR po."ReceivedDate" >= %(end)s)
    GROUP BY 1, 2, 3
)
SELECT c.lt, c.loc, c.pid, c.qty - coalesce(mv.later, 0), coalesce(t.qty, 0),
       coalesce(so.qty, 0) + coalesce(dor.qty, 0), coalesce(mv.expired, 0), coalesce(mv.oversell, 0)
FROM cur c
LEFT JOIN mv ON mv.lt = c.lt AND mv.loc = c.loc AND mv.pid = c.pid
LEFT JOIN transit t ON t.lt = c.lt AND t.loc = c.loc AND t.pid = c.pid
LEFT JOIN shop_order so ON so.lt = c.lt AND so.loc = c.loc AND so.pid = c.pid
LEFT JOIN dc_order dor ON dor.lt = c.lt AND dor.loc = c.loc AND dor.pid = c.pid
"""


def snapshot(cur, d: date) -> dict:
    """End-of-day stock per location and product: current balance minus movements after D.
    Tồn cuối ngày theo vị trí và sản phẩm: tồn hiện tại trừ biến động sau D.
    """
    cur.execute(SNAPSHOT_SQL, {"d": d, "start": start_of(d), "end": end_of(d)})
    # sum() comes back as Decimal / sum() trả về Decimal
    rows = [(lt, loc, pid, *map(int, nums)) for lt, loc, pid, *nums in cur.fetchall()]
    cur.execute('SELECT "Id", "UnitVolumeCm3", "IsColdChain" FROM product."Product"')
    vol = {pid: (v, cold) for pid, v, cold in cur.fetchall()}
    cur.execute("""SELECT "ShopCode", "StorageM3" FROM shop."ShopCapacity"
                   WHERE "EffectiveFrom" <= %s AND ("EffectiveTo" IS NULL OR "EffectiveTo" >= %s)""", (d, d))
    storage = {s: float(m3) for s, m3 in cur.fetchall()}
    used = defaultdict(float)
    for lt, loc, pid, on_hand, *_ in rows:
        if lt == "SHOP" and not vol.get(pid, (0, False))[1]:
            used[loc] += max(on_hand, 0) * vol.get(pid, (0, False))[0] / 1e6
    out = []
    for lt, loc, pid, on_hand, transit, on_order, expired, oversell in rows:
        over = lt == "SHOP" and loc in storage and used[loc] > storage[loc]
        out.append((d, lt, loc, pid, max(int(on_hand), 0), int(transit), int(on_order), int(expired),
                    on_hand <= 0 or oversell > 0, over))
    n = db.upsert(cur, 'inventory."InventorySnapshot"', ("SnapshotDate", "LocationType", "LocationId", "ProductId"),
                  ("OnHandQty", "InTransitQty", "OnOrderQty", "ExpiredQty", "StockoutFlag", "OverCapacityFlag"),
                  [(r[0], r[1], r[2], r[3]) + r[4:] for r in out])
    return {"snapshot_rows": len(out), "snapshot_changed": n,
            "stockout_positions": sum(1 for r in out if r[8] and r[1] == "SHOP"),
            "over_capacity_shops": sum(1 for s in used if s in storage and used[s] > storage[s])}


# ---------------------------------------------------------------------------------------------------------
# Plan day T / Lập kế hoạch ngày T
# ---------------------------------------------------------------------------------------------------------

def current_policies(cur, t: date, location_type: str) -> dict:
    cur.execute("""SELECT "LocationId", "ProductId", "ReorderPoint", "OrderUpTo", "SafetyStock", "ReviewPeriodDays",
                          "Source"
                   FROM inventory."ReplenishmentPolicy"
                   WHERE "LocationType" = %s AND "EffectiveFrom" <= %s AND ("EffectiveTo" IS NULL OR "EffectiveTo" >= %s)
                   ORDER BY "EffectiveFrom" """, (location_type, t, t))
    return {(loc, pid): {"levels": policy.Levels(rp, up, ss), "review": rv, "source": src}
            for loc, pid, rp, up, ss, rv, src in cur.fetchall()}


def refresh_policies(cur, t: date, p: SupplyParams) -> dict:
    """Monday refresh of BASELINE levels from the last `ma_window_days` of sales, once enough history exists.
    Làm mới mức BASELINE vào thứ Hai từ `ma_window_days` ngày bán gần nhất, khi đã đủ lịch sử.
    """
    cur.execute("""SELECT ("PostFrom" AT TIME ZONE 'Asia/Ho_Chi_Minh')::date FROM inventory."PostingCutover"
                   WHERE "Source" = 'SALES_STREAM'""")
    row = cur.fetchone()
    if row is None:
        return {"policy_refresh": "no cut-over"}
    first_full = row[0] + timedelta(days=1)
    window_from = max(first_full, t - timedelta(days=p.ma_window_days))
    days = (t - window_from).days
    if days < p.min_history_days:
        return {"policy_refresh": f"skipped, {days} days of history"}
    cur.execute("""SELECT "LocationId", "ProductId", "MovementDate", -sum("Qty")
                   FROM inventory."StockMovement"
                   WHERE "MovementType" = 'SALE' AND "LocationType" = 'SHOP'
                     AND "MovementTime" >= %s AND "MovementTime" < %s
                   GROUP BY 1, 2, 3""", (start_of(window_from), start_of(t)))
    total, square = defaultdict(float), defaultdict(float)
    for shop, pid, _, qty in cur.fetchall():
        qty = float(qty)
        total[(shop, pid)] += qty
        square[(shop, pid)] += qty * qty
    shops = current_policies(cur, t, "SHOP")
    dcs = current_policies(cur, t, "DC")
    cur.execute('SELECT "Code", "ServingWarehouseId" FROM shop."Shop"')
    serving = dict(cur.fetchall())
    cur.execute("""SELECT pr."Id", s."LeadTimeMeanDays", s."LeadTimeStdDays" FROM product."Product" pr
                   JOIN procurement."Supplier" s ON s."Id" = pr."SupplierId" """)
    lead = {pid: (float(m), float(sd)) for pid, m, sd in cur.fetchall()}
    rows, closed = [], []
    dc_mu, dc_var = defaultdict(float), defaultdict(float)
    for (shop, pid), pol in shops.items():
        mu = total[(shop, pid)] / days
        sigma = math.sqrt(max(square[(shop, pid)] / days - mu * mu, 0.0))
        dc_mu[(serving[shop], pid)] += mu
        dc_var[(serving[shop], pid)] += sigma ** 2
        if pol["source"] != "BASELINE":
            continue
        lv = policy.shop_levels(mu, sigma, pol["review"], p.shop_lead_days, p.z)
        rows.append((f"RP-{shop}-{pid}-{t:%Y%m%d}", "SHOP", shop, pid, "RsS", pol["review"], lv.reorder_point,
                     lv.order_up_to, lv.safety_stock))
        closed.append(("SHOP", shop, pid))
    for (dc, pid), pol in dcs.items():
        if pol["source"] != "BASELINE" or pid not in lead:
            continue
        lv = policy.dc_levels(dc_mu[(dc, pid)], math.sqrt(dc_var[(dc, pid)]), lead[pid][0], lead[pid][1],
                              p.dc_cover_days, p.z)
        rows.append((f"RP-{dc}-{pid}-{t:%Y%m%d}", "DC", dc, pid, "ROP", None, lv.reorder_point, lv.order_up_to,
                     lv.safety_stock))
        closed.append(("DC", dc, pid))
    cur.executemany("""UPDATE inventory."ReplenishmentPolicy" SET "EffectiveTo" = %s, "ModifiedDate" = now()
                       WHERE "LocationType" = %s AND "LocationId" = %s AND "ProductId" = %s
                         AND "EffectiveTo" IS NULL AND "EffectiveFrom" < %s AND "Source" = 'BASELINE'""",
                    [(t - timedelta(days=1),) + c + (t,) for c in closed])
    n = db.upsert(cur, 'inventory."ReplenishmentPolicy"', ("Id",),
                  ("LocationType", "LocationId", "ProductId", "PolicyType", "ReviewPeriodDays", "ReorderPoint",
                   "OrderUpTo", "SafetyStock", "Source", "SolverRunId", "EffectiveFrom", "EffectiveTo"),
                  [r + ("BASELINE", None, t, None) for r in rows])
    return {"policy_refresh_rows": n, "history_days": days}


def _network(cur, t: date) -> dict:
    """Master data needed to plan T. / Master cần để lập kế hoạch T."""
    cur.execute("""SELECT "Id", "UnitVolumeCm3", "UnitWeightG", "IsColdChain", "CasePack", "StockPrice", "SupplierId"
                   FROM product."Product" """)
    products = {r[0]: {"m3": r[1] / 1e6, "kg": r[2] / 1e3, "cold": r[3], "case": r[4], "cost": int(r[5]),
                       "supplier": r[6]} for r in cur.fetchall()}
    cur.execute("""SELECT s."Code", s."Tier", s."ServingWarehouseId", g."SubRegion", c."StorageM3"
                   FROM shop."Shop" s
                   JOIN public."Geography" g ON g."Id" = s."WardId"
                   LEFT JOIN shop."ShopCapacity" c ON c."ShopCode" = s."Code" AND c."EffectiveFrom" <= %(t)s
                        AND (c."EffectiveTo" IS NULL OR c."EffectiveTo" >= %(t)s)
                   WHERE (s."OpenDate" IS NULL OR s."OpenDate" <= %(t)s) AND (s."CloseDate" IS NULL OR s."CloseDate" > %(t)s)""",
                {"t": t})
    shops = {r[0]: {"tier": r[1], "dc": r[2], "core": r[3] == "SG_CORE", "storage": float(r[4] or 0)}
             for r in cur.fetchall()}
    cur.execute("""SELECT "FromId", "ToId", "RoadKm", "OffPeakMinutes", "PeakMinutes" FROM logistics."DistanceMatrix" """)
    dist = {(a, b): (float(km), off, peak) for a, b, km, off, peak in cur.fetchall()}
    cur.execute("""SELECT "ShopCode", "DeliveryWindowFrom", "DeliveryWindowTo" FROM logistics."Lane" """)
    windows = {s: (a, b) for s, a, b in cur.fetchall()}
    cur.execute("""SELECT "Id", "CarrierId", "VehicleType", "CapacityKg", "CapacityM3", "GrossWeightT", "IsCold",
                          "HomeWarehouseId"
                   FROM logistics."Vehicle" WHERE "Status" = 'ACTIVE' ORDER BY "Id" """)
    vehicles = [(routing.Vehicle(r[0], r[1], r[2], r[3], float(r[4]), float(r[5]), r[6]), r[7]) for r in cur.fetchall()]
    cur.execute("""SELECT "Id", "FixedCostPerTrip", "CostPerKm", "CostPerKg", "CostPerStop" FROM logistics."Carrier" """)
    carriers = {r[0]: {"fixed": r[1], "per_km": r[2], "per_kg": r[3], "per_stop": r[4]} for r in cur.fetchall()}
    return {"products": products, "shops": shops, "dist": dist, "windows": windows, "vehicles": vehicles,
            "carriers": carriers}


def _positions(cur, t: date) -> tuple:
    """On hand, in transit and on order per (location, product) right now.
    Tồn hiện có, đang vận chuyển và đã đặt theo (vị trí, sản phẩm) tại thời điểm này.
    """
    cur.execute("""SELECT "LocationId", "ProductId", sum("OnHandQty") FROM inventory."InventoryBalance" GROUP BY 1, 2""")
    on_hand = {(loc, pid): int(q) for loc, pid, q in cur.fetchall()}
    # Shipped to a shop and not delivered yet / Đã xuất cho shop nhưng chưa giao
    cur.execute("""SELECT st."ShopCode", si."ProductId", sum(si."Qty")
                   FROM logistics."ShipmentItem" si
                   JOIN logistics."ShipmentStop" st ON st."Id" = si."ShipmentStopId"
                   JOIN logistics."Shipment" sh ON sh."Id" = st."ShipmentId"
                   WHERE sh."Status" IN ('PICKED', 'IN_TRANSIT') AND st."Status" IN ('PENDING', 'DELAYED')
                   GROUP BY 1, 2""")
    transit = {(s, pid): int(q) for s, pid, q in cur.fetchall()}
    # Ordered by a shop and not picked yet / Shop đã đặt nhưng chưa soạn
    cur.execute("""SELECT t."ToShopCode", i."ProductId", sum(i."RequestedQty")
                   FROM inventory."TransferOrder" t JOIN inventory."TransferOrderItem" i ON i."TransferOrderId" = t."Id"
                   WHERE t."Status" = 'CREATED' GROUP BY 1, 2""")
    shop_order = {(s, pid): int(q) for s, pid, q in cur.fetchall()}
    return on_hand, transit, shop_order


def _dc_lots(cur, t: date, p: SupplyParams) -> dict:
    """Shippable DC lots in FEFO order; lots too close to expiry stay behind.
    Lô DC xuất được theo FEFO; lô sắp hết hạn bị giữ lại.
    """
    cur.execute("""SELECT b."LocationId", b."ProductId", b."LotId", b."OnHandQty"
                   FROM inventory."InventoryBalance" b
                   JOIN inventory."InventoryLot" l ON l."Id" = b."LotId"
                   WHERE b."LocationType" = 'DC' AND b."OnHandQty" > 0
                     AND (l."ExpiryDate" IS NULL OR l."ExpiryDate" > %s)
                   ORDER BY b."LocationId", b."ProductId", l."ExpiryDate" NULLS LAST, l."ReceivedDate", b."LotId" """,
                (t + timedelta(days=p.min_shelf_life_at_receipt_days),))
    lots = defaultdict(list)
    for dc, pid, lot, qty in cur.fetchall():
        lots[(dc, pid)].append([lot, int(qty)])
    return lots


def shop_orders(t: date, net: dict, policies: dict, on_hand: dict, transit: dict, shop_order: dict,
                due: list) -> dict:
    """Order lines per due shop: (R, s, S) on the inventory position, shrunk to fit the shop's dry storage.
    Dòng đặt cho mỗi shop đến kỳ: (R, s, S) trên vị thế tồn, co lại cho vừa kho khô của shop.
    """
    products = net["products"]
    out = {}
    for shop in due:
        lines, used = {}, 0.0
        for (loc, pid), pol in policies.items():
            if loc != shop or pid not in products:
                continue
            physical = on_hand.get((shop, pid), 0) + transit.get((shop, pid), 0)
            if not products[pid]["cold"]:
                used += physical * products[pid]["m3"]
            qty = policy.order_qty(physical + shop_order.get((shop, pid), 0), pol["levels"])
            if qty > 0:
                lines[pid] = qty
        storage = net["shops"][shop]["storage"]
        if storage > 0:
            dry = {pid: q for pid, q in lines.items() if not products[pid]["cold"]}
            scaled = policy.scale_to_capacity(dry, {pid: products[pid]["m3"] * 1e6 for pid in dry}, used, storage)
            lines = {**{pid: q for pid, q in lines.items() if products[pid]["cold"]}, **scaled}
        if lines:
            out[shop] = lines
    return out


def _schedule(origin: str, trip: routing.Trip, t: date, net: dict, earliest: datetime, p: SupplyParams) -> tuple:
    """Departure and planned arrivals; trips into core wards leave so the first core stop is reached at 09:00.
    Giờ xuất phát và giờ tới dự kiến; chuyến vào nội đô xuất phát để tới điểm nội đô đầu tiên lúc 09:00.
    """
    def arrivals(depart: datetime) -> list:
        """(arrival before waiting, planned arrival) per stop / (giờ tới trước khi chờ, giờ tới dự kiến)."""
        at, here, out = depart, origin, []
        for stop in trip.stops:
            _, off, peak = net["dist"][(here, stop.shop)]
            at = geo.add_minutes(at, geo.drive_minutes(off, peak, at))
            window = net["windows"].get(stop.shop, WINDOW_CORE if stop.core else WINDOW_OTHER)
            raw = at
            at = max(at, datetime.combine(t, window[0], ICT))
            out.append((raw, at))
            at = geo.add_minutes(at, STOP_SERVICE_MIN)
            here = stop.shop
        return out

    depart = max(datetime.combine(t, time(p.depart_hour), ICT), earliest)
    plan = arrivals(depart)
    core_idx = next((i for i, s in enumerate(trip.stops) if s.core), None)
    if core_idx is not None:
        # Leave later instead of idling outside a core shop / Xuất phát muộn hơn thay vì chờ trước shop nội đô
        wait = datetime.combine(t, WINDOW_CORE[0], ICT) - plan[core_idx][0]
        if wait > timedelta(0):
            depart += wait
            plan = arrivals(depart)
    return depart, [at for _, at in plan]


def plan_transfers(cur, t: date, now: datetime, p: SupplyParams) -> dict:
    """Transfer orders for shops due on T, DC allocation by FEFO lot and the trips that carry them.
    Lệnh điều chuyển cho shop đến kỳ giao ngày T, phân bổ tồn DC theo lô FEFO và các chuyến chở hàng.
    """
    cur.execute('SELECT 1 FROM inventory."TransferOrder" WHERE "RequestedDate" = %s LIMIT 1', (t,))
    if cur.fetchone():
        return {"transfers": "already planned"}
    net = _network(cur, t)
    cur.execute("""SELECT DISTINCT "ShopCode" FROM logistics."DeliverySchedule"
                   WHERE "Weekday" = %s AND "EffectiveFrom" <= %s AND ("EffectiveTo" IS NULL OR "EffectiveTo" >= %s)""",
                (t.isoweekday(), t, t))
    due = sorted(s for (s,) in cur.fetchall() if s in net["shops"])
    on_hand, transit, shop_order = _positions(cur, t)
    orders = shop_orders(t, net, current_policies(cur, t, "SHOP"), on_hand, transit, shop_order, due)

    # Allocate DC stock product by product, then lot by lot / Phân bổ tồn DC theo sản phẩm rồi theo lô
    lots = _dc_lots(cur, t, p)
    requests = defaultdict(dict)
    for shop, lines in orders.items():
        for pid, qty in lines.items():
            requests[(net["shops"][shop]["dc"], pid)][shop] = qty
    picked = defaultdict(list)
    for (dc, pid), req in sorted(requests.items()):
        stock = lots.get((dc, pid), [])
        grant = policy.allocate(req, sum(q for _, q in stock))
        for shop in sorted(grant):
            need = grant[shop]
            for lot in stock:
                if need <= 0:
                    break
                take = min(need, lot[1])
                if take > 0:
                    picked[shop].append((pid, lot[0], take))
                    lot[1] -= take
                    need -= take

    transfer_rows, item_rows, loads = [], [], defaultdict(list)
    for shop, lines in sorted(orders.items()):
        dc = net["shops"][shop]["dc"]
        to_id = f"TO-{t:%Y%m%d}-{shop}"
        status = "CREATED" if picked.get(shop) else "CANCELLED"
        transfer_rows.append((to_id, dc, shop, status, t, t, now, now))
        for pid, qty in sorted(lines.items()):
            item_rows.append((f"{to_id}-{pid}", to_id, pid, qty, 0, 0, now, now))
        by_cold = defaultdict(list)
        for pid, lot, qty in picked.get(shop, []):
            pr = net["products"][pid]
            by_cold[pr["cold"]].append(routing.Line(f"{to_id}-{pid}", pid, lot, qty, pr["kg"], pr["m3"]))
        for cold, cargo in sorted(by_cold.items()):
            core = net["shops"][shop]["core"]
            load = routing.Load(f"{to_id}-{'C' if cold else 'D'}", shop, to_id, core, cold, cargo)
            cap = VEHICLE_TYPES["COLD_VAN" if cold else ("VAN_1T" if core else "TRUCK_2_5T")]
            loads[dc].extend(routing.split_load(load, cap.capacity_kg, cap.capacity_m3))
    cur.executemany("""INSERT INTO inventory."TransferOrder" ("Id", "FromWarehouseId", "ToShopCode", "Status",
                           "RequestedDate", "PlannedDeliveryDate", "CreatedDate", "ModifiedDate")
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", transfer_rows)
    cur.executemany("""INSERT INTO inventory."TransferOrderItem" ("Id", "TransferOrderId", "ProductId", "RequestedQty",
                           "ShippedQty", "ReceivedQty", "CreatedDate", "ModifiedDate")
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""", item_rows)

    # Trips: own fleet first, then the shared pool, then on-demand hire / Chuyến: xe nhà, xe dùng chung, rồi thuê ngoài
    pool = [v for v, home in net["vehicles"] if home is None]
    earliest = now + timedelta(minutes=p.pick_lead_min + 30)

    def km(a: str, b: str) -> float:
        return net["dist"][(a, b)][0]

    def on_demand(core: bool, cold: bool) -> routing.Vehicle:
        spec = VEHICLE_TYPES["COLD_VAN" if cold else ("VAN_1T" if core else "TRUCK_2_5T")]
        return routing.Vehicle(None, "CAR04" if cold else ON_DEMAND_CARRIER, spec.code, spec.capacity_kg,
                               spec.capacity_m3, spec.gross_t, spec.cold)

    stats = {"due_shops": len(due), "transfer_orders": len(transfer_rows), "transfer_lines": len(item_rows),
             "cancelled_orders": sum(r[3] == "CANCELLED" for r in transfer_rows), "shipments": 0, "stops": 0,
             "on_demand_trips": 0, "transport_cost": 0}
    for dc in sorted(loads):
        own = [v for v, home in net["vehicles"] if home == dc]
        trips = routing.plan_trips(dc, loads[dc], km, [own, pool], on_demand, p.max_stops)
        for n, trip in enumerate(trips, start=1):
            _write_trip(cur, f"SHP-{t:%Y%m%d}-{dc}-{n:02d}", dc, trip, t, now, earliest, net, p, stats)
    return stats


def _write_trip(cur, sid: str, dc: str, trip: routing.Trip, t: date, now: datetime, earliest: datetime, net: dict,
                p: SupplyParams, stats: dict) -> None:
    depart, arrivals = _schedule(dc, trip, t, net, earliest, p)
    distance = routing.route_km(dc, trip.stops, lambda a, b: net["dist"][(a, b)][0])
    kg = sum(s.kg for s in trip.stops)
    m3 = sum(s.m3 for s in trip.stops)
    v = trip.vehicle
    cost = routing.trip_cost(net["carriers"][v.carrier_id], distance, kg, len(trip.stops))
    cur.execute("""INSERT INTO logistics."Shipment" ("Id", "WarehouseId", "CarrierId", "VehicleId", "Status",
                       "PlannedDepartureTime", "ActualDepartureTime", "DistanceKm", "TotalWeightKg", "TotalVolumeM3",
                       "StopCount", "TransportCost", "ShipmentDate", "CreatedDate", "ModifiedDate")
                   VALUES (%s, %s, %s, %s, 'PLANNED', %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (sid, dc, v.carrier_id, v.id, depart, round(distance, 2), round(kg, 2), round(m3, 3),
                 len(trip.stops), cost, depart.astimezone(ICT).date(), now, now))
    for seq, (stop, at) in enumerate(zip(trip.stops, arrivals), start=1):
        stop_id = f"{sid}-S{seq}"
        cur.execute("""INSERT INTO logistics."ShipmentStop" ("Id", "ShipmentId", "StopSeq", "ShopCode", "TransferOrderId",
                           "PlannedArrivalTime", "ActualArrivalTime", "Status", "CreatedDate", "ModifiedDate")
                       VALUES (%s, %s, %s, %s, %s, %s, NULL, 'PENDING', %s, %s)""",
                    (stop_id, sid, seq, stop.shop, stop.transfer_order_id, at, now, now))
        cur.executemany("""INSERT INTO logistics."ShipmentItem" ("Id", "ShipmentStopId", "TransferOrderItemId",
                               "ProductId", "LotId", "Qty", "ReceivedQty", "CreatedDate", "ModifiedDate")
                           VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s)""",
                        [(f"{stop_id}-{l.product_id}-{l.lot_id}", stop_id, l.item_id, l.product_id, l.lot_id, l.qty,
                          now, now) for l in stop.lines])
    stats["shipments"] += 1
    stats["stops"] += len(trip.stops)
    stats["on_demand_trips"] += v.id is None
    stats["transport_cost"] += cost


def plan_purchases(cur, t: date, now: datetime, p: SupplyParams) -> dict:
    """DC reorder point check; lines rounded to cases and grouped per supplier and DC. An order below the
    supplier's minimum value waits unless a line is under safety stock.
    Kiểm tra điểm đặt hàng của DC; dòng làm tròn theo thùng và gom theo NCC và DC. Đơn dưới giá trị tối thiểu
    của NCC được hoãn trừ khi có dòng dưới tồn an toàn.
    """
    cur.execute("SELECT 1 FROM procurement.\"PurchaseOrder\" WHERE \"Id\" LIKE %s LIMIT 1", (f"PO-{t:%Y%m%d}-%",))
    if cur.fetchone():
        return {"purchases": "already planned"}
    cur.execute("""SELECT "LocationId", "ProductId", sum("OnHandQty") FROM inventory."InventoryBalance"
                   WHERE "LocationType" = 'DC' GROUP BY 1, 2""")
    on_hand = {(dc, pid): int(q) for dc, pid, q in cur.fetchall()}
    # Allocated to transfers but not picked yet / Đã phân cho lệnh điều chuyển nhưng chưa soạn
    cur.execute("""SELECT sh."WarehouseId", si."ProductId", sum(si."Qty") FROM logistics."ShipmentItem" si
                   JOIN logistics."ShipmentStop" st ON st."Id" = si."ShipmentStopId"
                   JOIN logistics."Shipment" sh ON sh."Id" = st."ShipmentId"
                   WHERE sh."Status" = 'PLANNED' GROUP BY 1, 2""")
    committed = {(dc, pid): int(q) for dc, pid, q in cur.fetchall()}
    cur.execute("""SELECT po."WarehouseId", i."ProductId", sum(i."OrderedQty") FROM procurement."PurchaseOrder" po
                   JOIN procurement."PurchaseOrderItem" i ON i."PurchaseOrderId" = po."Id"
                   WHERE po."Status" IN ('CREATED', 'CONFIRMED') GROUP BY 1, 2""")
    on_order = {(dc, pid): int(q) for dc, pid, q in cur.fetchall()}
    cur.execute("""SELECT sp."ProductId", sp."SupplierId", sp."UnitCost", sp."CasePack", sp."MinOrderQty",
                          s."LeadTimeMeanDays", s."MinOrderValue"
                   FROM procurement."SupplierProduct" sp JOIN procurement."Supplier" s ON s."Id" = sp."SupplierId"
                   WHERE sp."IsPrimary" AND sp."EffectiveFrom" <= %s AND (sp."EffectiveTo" IS NULL OR sp."EffectiveTo" >= %s)""",
                (t, t))
    sources = {r[0]: {"supplier": r[1], "cost": int(r[2]), "case": r[3], "moq": r[4], "lead": float(r[5]),
                      "mov": int(r[6])} for r in cur.fetchall()}
    groups = defaultdict(list)
    for (dc, pid), pol in sorted(current_policies(cur, t, "DC").items()):
        src = sources.get(pid)
        if src is None:
            continue
        position = on_hand.get((dc, pid), 0) - committed.get((dc, pid), 0) + on_order.get((dc, pid), 0)
        qty = policy.round_to_case(policy.order_qty(position, pol["levels"]), src["case"], src["moq"])
        if qty > 0:
            urgent = position < pol["levels"].safety_stock
            groups[(src["supplier"], dc)].append((pid, qty, src["cost"], urgent))
    stats = {"purchase_orders": 0, "purchase_lines": 0, "purchase_value": 0, "deferred_below_mov": 0}
    for (supplier, dc), lines in sorted(groups.items()):
        value = sum(q * c for _, q, c, _ in lines)
        src = sources[lines[0][0]]
        if value < src["mov"] and not any(u for *_, u in lines):
            stats["deferred_below_mov"] += 1
            continue
        po_id = f"PO-{t:%Y%m%d}-{supplier}-{dc}"
        cur.execute("""INSERT INTO procurement."PurchaseOrder" ("Id", "SupplierId", "WarehouseId", "Status", "OrderDate",
                           "ExpectedDate", "ReceivedDate", "TotalAmount", "CreatedDate", "ModifiedDate")
                       VALUES (%s, %s, %s, 'CREATED', %s, %s, NULL, %s, %s, %s)""",
                    (po_id, supplier, dc, now, t + timedelta(days=math.ceil(src["lead"])), value, now, now))
        cur.executemany("""INSERT INTO procurement."PurchaseOrderItem" ("Id", "PurchaseOrderId", "ProductId",
                               "OrderedQty", "ReceivedQty", "UnitCost", "CreatedDate", "ModifiedDate")
                           VALUES (%s, %s, %s, %s, 0, %s, %s, %s)""",
                        [(f"{po_id}-{pid}", po_id, pid, q, c, now, now) for pid, q, c, _ in lines])
        stats["purchase_orders"] += 1
        stats["purchase_lines"] += len(lines)
        stats["purchase_value"] += value
    return stats


# ---------------------------------------------------------------------------------------------------------
# Orchestration / Điều phối
# ---------------------------------------------------------------------------------------------------------

def close_day(conn, d: date, now: datetime, p: SupplyParams = PARAMS) -> dict:
    run_id = f"DAILY_CLOSE-{d:%Y-%m-%d}"
    if not db.run_started(conn, run_id, "DAILY_CLOSE", d):
        return {"day": str(d), "skipped": "already closed"}
    t = d + timedelta(days=1)
    stats = {"day": str(d), "plan_day": str(t)}
    try:
        stats.update(db.retry(conn, lambda cur: expire(cur, d)))
        stats.update(db.retry(conn, lambda cur: snapshot(cur, d)))
        if t.isoweekday() == 1:
            stats.update(db.retry(conn, lambda cur: refresh_policies(cur, t, p)))
        stats.update(db.retry(conn, lambda cur: plan_transfers(cur, t, now, p)))
        stats.update(db.retry(conn, lambda cur: plan_purchases(cur, t, now, p)))
        stats.update(db.retry(conn, lambda cur: promotions.refresh_vouchers(cur, now)))
    except Exception as exc:
        db.run_finished(conn, run_id, "FAILED", {**stats, "error": str(exc)[:500]})
        raise
    db.run_finished(conn, run_id, "SUCCESS", stats)
    return stats


def days_to_close(conn, today: date) -> list:
    """Every day after the last successful close up to yesterday; yesterday alone on the first run.
    Mọi ngày sau lần chốt thành công gần nhất tới hôm qua; lần đầu chỉ chốt hôm qua.
    """
    with conn.cursor() as cur:
        cur.execute("""SELECT max("BusinessDate") FROM datagen."GeneratorRun"
                       WHERE "Mode" = 'DAILY_CLOSE' AND "Status" = 'SUCCESS'""")
        last = cur.fetchone()[0]
    conn.rollback()
    yesterday = today - timedelta(days=1)
    start = yesterday if last is None else last + timedelta(days=1)
    return [start + timedelta(days=i) for i in range((yesterday - start).days + 1)]


def run(now: datetime | None = None) -> list:
    """Airflow entry point. / Điểm vào cho Airflow."""
    now = now or datetime.now(ICT)
    with db.connection() as conn:
        results = [close_day(conn, d, now) for d in days_to_close(conn, now.astimezone(ICT).date())]
    for r in results:
        log.info("daily close %s", json.dumps(r, default=str))
    return results


def dry_run(d: date, now: datetime) -> dict:
    """Close D inside one transaction and roll it back. / Chốt D trong một transaction rồi rollback."""
    t = d + timedelta(days=1)
    with db.connection() as conn:
        with conn.cursor() as cur:
            stats = {"day": str(d)}
            stats.update(expire(cur, d))
            stats.update(snapshot(cur, d))
            if t.isoweekday() == 1:
                stats.update(refresh_policies(cur, t, PARAMS))
            stats.update(plan_transfers(cur, t, now, PARAMS))
            stats.update(plan_purchases(cur, t, now, PARAMS))
            stats.update(promotions.refresh_vouchers(cur, now))
            cur.execute("""SELECT s."Id", s."CarrierId", s."VehicleId", s."PlannedDepartureTime", s."StopCount",
                                  s."DistanceKm", s."TotalWeightKg", s."TotalVolumeM3", s."TransportCost"
                           FROM logistics."Shipment" s WHERE s."ShipmentDate" = %s ORDER BY 1""", (t,))
            stats["trips"] = [list(r) for r in cur.fetchall()]
        conn.rollback()
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run-day", type=date.fromisoformat, help="close this day and roll back")
    args = parser.parse_args()
    now = datetime.now(ICT)
    if args.dry_run_day:
        print(json.dumps(dry_run(args.dry_run_day, now), ensure_ascii=False, indent=1, default=str))
    else:
        print(json.dumps(run(now), ensure_ascii=False, indent=1, default=str))


if __name__ == "__main__":
    main()
