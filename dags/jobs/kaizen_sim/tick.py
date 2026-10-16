"""Tick: writes every trip event that is due (picking, departure, delays, arrivals, deliveries) with its
stock movements, ShipmentEvent row and Outbox row in one transaction per event, then confirms and receives
purchase orders. Late ticks catch up because events carry their scheduled time, not the tick time.
Runs as a long-lived worker (k8s Deployment kaizen-supply-tick, --every 60).
Tick: ghi mọi sự kiện chuyến đã đến giờ (soạn, xuất bến, trễ, tới nơi, giao hàng) cùng biến động kho,
dòng ShipmentEvent và dòng Outbox trong một transaction mỗi sự kiện, sau đó xác nhận và nhận hàng đơn mua.
Tick chạy trễ vẫn bù đúng vì sự kiện mang giờ dự kiến, không phải giờ tick.
Chạy dạng worker thường trực (k8s Deployment kaizen-supply-tick, --every 60).
"""
import argparse
import json
import logging
import math
import signal
import threading
from collections import Counter, defaultdict
from datetime import datetime, time, timedelta
from pathlib import Path
from time import monotonic

from . import db, rng
from .events import ShipmentPlan, StopPlan, payload, timeline
from .settings import ICT, PARAMS, VEHICLE_TYPES, SupplyParams

log = logging.getLogger(__name__)

# Session lock so two ticks never interleave / Khoá phiên để hai tick không chạy xen nhau
TICK_LOCK = 72_001


def load_active(cur) -> tuple:
    """Open trips as plans, plus item details and the last written event seq per trip.
    Các chuyến đang mở dưới dạng kế hoạch, kèm chi tiết dòng hàng và seq sự kiện đã ghi cuối cùng.
    """
    cur.execute("""SELECT s."Id", s."WarehouseId", s."CarrierId", s."VehicleId", s."PlannedDepartureTime",
                          s."CreatedDate", s."TotalWeightKg", s."TotalVolumeM3", s."TransportCost", c."BaseOnTimeRate",
                          v."CapacityKg", v."CapacityM3"
                   FROM logistics."Shipment" s
                   JOIN logistics."Carrier" c ON c."Id" = s."CarrierId"
                   LEFT JOIN logistics."Vehicle" v ON v."Id" = s."VehicleId"
                   WHERE s."Status" IN ('PLANNED', 'PICKED', 'IN_TRANSIT')
                   ORDER BY s."PlannedDepartureTime", s."Id" """)
    heads = cur.fetchall()
    if not heads:
        return [], {}, {}
    ids = [h[0] for h in heads]
    cur.execute("""SELECT st."Id", st."ShipmentId", st."StopSeq", st."ShopCode", st."PlannedArrivalTime",
                          si."Id", si."Qty", si."ProductId", si."LotId", si."TransferOrderItemId", st."TransferOrderId"
                   FROM logistics."ShipmentStop" st
                   JOIN logistics."ShipmentItem" si ON si."ShipmentStopId" = st."Id"
                   WHERE st."ShipmentId" = ANY(%s)""", (ids,))
    stops, items = {}, {}
    for stop_id, sid, seq, shop, planned, item_id, qty, pid, lot, to_item, to_id in cur.fetchall():
        stop = stops.setdefault(stop_id, StopPlan(stop_id, seq, shop, planned.astimezone(ICT), {}, sid))
        stop.items[item_id] = qty
        items[item_id] = {"product": pid, "lot": lot, "to_item": to_item, "to": to_id, "qty": qty}
    cur.execute("""SELECT "ShipmentId", max("EventSeq") FROM logistics."ShipmentEvent"
                   WHERE "ShipmentId" = ANY(%s) GROUP BY 1""", (ids,))
    last = dict(cur.fetchall())
    van = VEHICLE_TYPES["VAN_1T"]
    plans = []
    for sid, dc, carrier, vehicle, depart, created, kg, m3, cost, on_time, cap_kg, cap_m3 in heads:
        cap_kg = cap_kg or van.capacity_kg
        cap_m3 = float(cap_m3 or van.capacity_m3)
        kg, m3 = float(kg or 0), float(m3 or 0)
        plans.append(ShipmentPlan(sid, dc, carrier, vehicle, depart.astimezone(ICT), created.astimezone(ICT), kg, m3,
                                  int(cost), float(on_time), max(kg / cap_kg, m3 / cap_m3),
                                  sorted((s for s in stops.values() if s.shipment_id == sid), key=lambda s: s.seq)))
    return plans, items, last


def _lock(cur, location_type: str, location: str, keys: list) -> None:
    """Lock balance rows in post_sales_stock order. / Khoá dòng tồn theo thứ tự của post_sales_stock."""
    cur.execute("""SELECT 1 FROM inventory."InventoryBalance"
                   WHERE "LocationType" = %s AND "LocationId" = %s AND ("ProductId", "LotId") IN %s
                   ORDER BY "LocationId", "ProductId", "LotId" FOR UPDATE""",
                (location_type, location, tuple(sorted(keys))))


def _costs(cur, products: set) -> dict:
    cur.execute('SELECT "Id", "StockPrice" FROM product."Product" WHERE "Id" = ANY(%s)', (list(products),))
    return {pid: int(c) for pid, c in cur.fetchall()}


def apply_event(cur, sh: ShipmentPlan, ev, items: dict) -> None:
    """Effects of one event, then its ShipmentEvent and Outbox rows. / Tác động của một sự kiện, rồi dòng ShipmentEvent và Outbox."""
    cur.execute('SELECT 1 FROM logistics."ShipmentEvent" WHERE "ShipmentId" = %s AND "EventSeq" = %s',
                (sh.id, ev.seq))
    if cur.fetchone():
        return
    if ev.type == "PICKED":
        lines = [(iid, items[iid]) for s in sh.stops for iid in s.items]
        _lock(cur, "DC", sh.origin, [(it["product"], it["lot"]) for _, it in lines])
        cost = _costs(cur, {it["product"] for _, it in lines})
        shipped = Counter()
        for iid, it in sorted(lines, key=lambda x: (x[1]["product"], x[1]["lot"], x[0])):
            db.apply_movement(cur, f"SM-SHIP-{iid}-OUT", "TRANSFER_OUT", "DC", sh.origin, it["product"], it["lot"],
                              -it["qty"], cost[it["product"]], "SHIPMENT", sh.id, iid, None, ev.at)
            shipped[it["to_item"]] += it["qty"]
        cur.executemany("""UPDATE inventory."TransferOrderItem" SET "ShippedQty" = "ShippedQty" + %s, "ModifiedDate" = now()
                           WHERE "Id" = %s""", [(q, k) for k, q in sorted(shipped.items())])
        cur.execute("""UPDATE inventory."TransferOrder" SET "Status" = 'PICKED', "ModifiedDate" = now()
                       WHERE "Id" = ANY(%s) AND "Status" = 'CREATED'""",
                    (sorted({it["to"] for _, it in lines}),))
        cur.execute("""UPDATE logistics."Shipment" SET "Status" = 'PICKED', "ModifiedDate" = now() WHERE "Id" = %s""",
                    (sh.id,))
    elif ev.type == "DEPARTED":
        cur.execute("""UPDATE logistics."Shipment" SET "Status" = 'IN_TRANSIT', "ActualDepartureTime" = %s,
                           "ModifiedDate" = now() WHERE "Id" = %s""", (ev.at, sh.id))
        cur.execute("""UPDATE inventory."TransferOrder" SET "Status" = 'SHIPPED', "ModifiedDate" = now()
                       WHERE "Id" IN (SELECT "TransferOrderId" FROM logistics."ShipmentStop" WHERE "ShipmentId" = %s)
                         AND "Status" = 'PICKED'""", (sh.id,))
    elif ev.type == "DELAYED":
        cur.execute("""UPDATE logistics."ShipmentStop" SET "Status" = 'DELAYED', "ModifiedDate" = now()
                       WHERE "Id" = %s AND "Status" = 'PENDING'""", (ev.stop.id,))
    elif ev.type == "ARRIVED_STOP":
        cur.execute("""UPDATE logistics."ShipmentStop" SET "ActualArrivalTime" = %s, "ModifiedDate" = now()
                       WHERE "Id" = %s""", (ev.actual_arrival, ev.stop.id))
    elif ev.type in ("DELIVERED", "PARTIAL"):
        _deliver(cur, sh, ev, items)
    if ev.status in ("COMPLETED", "PARTIAL") and ev.type in ("DELIVERED", "PARTIAL"):
        cur.execute("""UPDATE logistics."Shipment" SET "Status" = %s, "ModifiedDate" = now() WHERE "Id" = %s""",
                    (ev.status, sh.id))

    cur.execute("""INSERT INTO logistics."ShipmentEvent" ("Id", "ShipmentId", "EventSeq", "ShipmentStopId", "EventType",
                       "EventTime", "ReasonCode", "CreatedDate", "ModifiedDate")
                   VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now())""",
                (f"{sh.id}-E{ev.seq:03d}", sh.id, ev.seq, ev.stop.id if ev.stop else None, ev.type, ev.at, ev.reason))
    db.outbox_logistics(cur, sh.id, ev.type, ev.seq, payload(sh, ev), ev.at)


def _deliver(cur, sh: ShipmentPlan, ev, items: dict) -> None:
    stop = ev.stop
    received = {iid: q for iid, q in ev.received.items() if q > 0}
    if received:
        _lock(cur, "SHOP", stop.shop, [(items[i]["product"], items[i]["lot"]) for i in received])
        cost = _costs(cur, {items[i]["product"] for i in received})
        for iid in sorted(received, key=lambda i: (items[i]["product"], items[i]["lot"], i)):
            it = items[iid]
            db.apply_movement(cur, f"SM-SHIP-{iid}-IN", "TRANSFER_IN", "SHOP", stop.shop, it["product"], it["lot"],
                              received[iid], cost[it["product"]], "SHIPMENT", sh.id, iid, None, ev.at)
    # Units short-picked never left the DC: post them back so the ledger stays closed
    # Phần lấy thiếu chưa rời DC: ghi trả lại để sổ kho khép kín
    short = {iid: items[iid]["qty"] - q for iid, q in ev.received.items() if q < items[iid]["qty"]}
    if short:
        _lock(cur, "DC", sh.origin, [(items[i]["product"], items[i]["lot"]) for i in short])
        cost = _costs(cur, {items[i]["product"] for i in short})
        for iid in sorted(short, key=lambda i: (items[i]["product"], items[i]["lot"], i)):
            it = items[iid]
            db.apply_movement(cur, f"SM-SHIP-{iid}-SHORT", "ADJUST", "DC", sh.origin, it["product"], it["lot"],
                              short[iid], cost[it["product"]], "SHIPMENT", sh.id, iid, "SHORT_PICK", ev.at)
    cur.executemany("""UPDATE logistics."ShipmentItem" SET "ReceivedQty" = %s, "ModifiedDate" = now() WHERE "Id" = %s""",
                    [(q, iid) for iid, q in sorted(ev.received.items())])
    to_received = Counter()
    for iid, q in ev.received.items():
        to_received[items[iid]["to_item"]] += q
    cur.executemany("""UPDATE inventory."TransferOrderItem" SET "ReceivedQty" = "ReceivedQty" + %s, "ModifiedDate" = now()
                       WHERE "Id" = %s""", [(q, k) for k, q in sorted(to_received.items())])
    cur.execute("""UPDATE logistics."ShipmentStop" SET "Status" = %s, "ActualArrivalTime" = coalesce("ActualArrivalTime", %s),
                       "ModifiedDate" = now() WHERE "Id" = %s""", (ev.type, ev.actual_arrival, stop.id))
    # Close the transfer order once none of its stops is still open / Đóng lệnh điều chuyển khi không còn điểm dừng mở
    to_id = items[next(iter(ev.received))]["to"] if ev.received else None
    if to_id:
        cur.execute("""UPDATE inventory."TransferOrder" t
                       SET "Status" = CASE WHEN NOT EXISTS (SELECT 1 FROM inventory."TransferOrderItem" i
                                                            WHERE i."TransferOrderId" = t."Id"
                                                              AND i."ReceivedQty" < i."RequestedQty")
                                           THEN 'RECEIVED' ELSE 'PARTIAL' END,
                           "ModifiedDate" = now()
                       WHERE t."Id" = %s AND t."Status" IN ('PICKED', 'SHIPPED')
                         AND NOT EXISTS (SELECT 1 FROM logistics."ShipmentStop" st
                                         WHERE st."TransferOrderId" = t."Id" AND st."Status" IN ('PENDING', 'DELAYED'))""",
                    (to_id,))


def advance_shipments(conn, now: datetime, p: SupplyParams = PARAMS) -> Counter:
    with conn.cursor() as cur:
        plans, items, last = load_active(cur)
    conn.rollback()
    done = Counter()
    for sh in plans:
        written = last.get(sh.id, 0)
        for ev in timeline(sh, p):
            if ev.seq <= written:
                continue
            if ev.at > now:
                break
            db.retry(conn, lambda cur, sh=sh, ev=ev: apply_event(cur, sh, ev, items))
            done[ev.type] += 1
    return done


def po_times(po_id: str, order_date: datetime, lead_mean: float, lead_std: float) -> tuple:
    """(confirmed at, arrives at) for a purchase order. / (giờ xác nhận, giờ hàng tới) của đơn mua."""
    order_ict = order_date.astimezone(ICT)
    confirmed = order_ict + timedelta(minutes=rng.integer(60, 240, po_id, "confirm"))
    days = max(1, int(round(rng.normal(lead_mean, lead_std, po_id, "lead"))))
    lo, hi = PARAMS.po_receipt_hours
    minute = rng.integer(lo * 60, hi * 60, po_id, "arrive")
    arrives = datetime.combine(order_ict.date() + timedelta(days=days), time(minute // 60, minute % 60), ICT)
    return confirmed, max(arrives, confirmed + timedelta(minutes=1))


def receive_po(cur, po_id: str, dc: str, supplier: str, fill_rate: float, arrives: datetime, p: SupplyParams) -> str:
    cur.execute('SELECT "Status" FROM procurement."PurchaseOrder" WHERE "Id" = %s FOR UPDATE', (po_id,))
    if cur.fetchone()[0] not in ("CREATED", "CONFIRMED"):
        return "skipped"
    cur.execute("""SELECT i."Id", i."ProductId", i."OrderedQty", i."UnitCost", pr."ShelfLifeDays"
                   FROM procurement."PurchaseOrderItem" i JOIN product."Product" pr ON pr."Id" = i."ProductId"
                   WHERE i."PurchaseOrderId" = %s ORDER BY i."ProductId" """, (po_id,))
    full = True
    for item_id, pid, ordered, unit_cost, life in cur.fetchall():
        qty = ordered
        if rng.chance((1.0 - fill_rate) * p.po_short_extra, item_id, "short"):
            qty = max(1, int(ordered * rng.uniform(0.5, 0.95, item_id, "fill")))
            full = False
        used = rng.uniform(p.lot_life_used[0], p.lot_life_used[1], item_id, "life")
        expiry = arrives.date() + timedelta(days=int(math.floor((life or 365) * used)))
        lot = f"LOT-{item_id}"
        cur.execute("""INSERT INTO inventory."InventoryLot" ("Id", "ProductId", "LotNo", "ExpiryDate", "ReceivedDate",
                           "SupplierId", "PurchaseOrderId", "CreatedDate", "ModifiedDate")
                       VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now()) ON CONFLICT ("Id") DO NOTHING""",
                    (lot, pid, f"L{arrives:%y%m%d}{supplier[-2:]}{pid[-4:]}", expiry, arrives.date(), supplier, po_id))
        db.apply_movement(cur, f"SM-PO-{item_id}-RECEIPT", "RECEIPT", "DC", dc, pid, lot, qty, int(unit_cost), "PO",
                          po_id, item_id, None, arrives)
        cur.execute("""UPDATE procurement."PurchaseOrderItem" SET "ReceivedQty" = %s, "ModifiedDate" = now()
                       WHERE "Id" = %s""", (qty, item_id))
    status = "RECEIVED" if full else "PARTIAL"
    cur.execute("""UPDATE procurement."PurchaseOrder" SET "Status" = %s, "ReceivedDate" = %s, "ModifiedDate" = now()
                   WHERE "Id" = %s""", (status, arrives, po_id))
    return status


def advance_purchases(conn, now: datetime, p: SupplyParams = PARAMS) -> Counter:
    with conn.cursor() as cur:
        cur.execute("""SELECT po."Id", po."WarehouseId", po."SupplierId", po."Status", po."OrderDate",
                              s."LeadTimeMeanDays", s."LeadTimeStdDays", s."FillRate"
                       FROM procurement."PurchaseOrder" po JOIN procurement."Supplier" s ON s."Id" = po."SupplierId"
                       WHERE po."Status" IN ('CREATED', 'CONFIRMED') ORDER BY po."Id" """)
        open_pos = cur.fetchall()
    conn.rollback()
    done = Counter()
    for po_id, dc, supplier, status, order_date, mean, std, fill in open_pos:
        confirmed, arrives = po_times(po_id, order_date, float(mean), float(std))
        if status == "CREATED" and now >= confirmed and now < arrives:
            db.retry(conn, lambda cur, i=po_id: cur.execute(
                """UPDATE procurement."PurchaseOrder" SET "Status" = 'CONFIRMED', "ModifiedDate" = now()
                   WHERE "Id" = %s AND "Status" = 'CREATED'""", (i,)))
            done["po_confirmed"] += 1
        if now >= arrives:
            result = db.retry(conn, lambda cur, i=po_id, d=dc, s=supplier, f=float(fill), a=arrives:
                              receive_po(cur, i, d, s, f, a, p))
            done[f"po_{result.lower()}"] += 1
    return done


def run(now: datetime | None = None) -> dict:
    """Airflow entry point. / Điểm vào cho Airflow."""
    now = (now or datetime.now(ICT)).astimezone(ICT)
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (TICK_LOCK,))
            locked = cur.fetchone()[0]
        conn.commit()
        if not locked:
            return {"skipped": "another tick is running"}
        stats = dict(advance_shipments(conn, now))
        stats.update(advance_purchases(conn, now))
        if stats:
            run_id = f"TICK-{now:%Y%m%dT%H%M}"
            if db.run_started(conn, run_id, "TICK", now.date()):
                db.run_finished(conn, run_id, "SUCCESS", stats)
    log.log(logging.INFO if stats else logging.DEBUG, "tick %s %s", now.isoformat(), json.dumps(stats))
    return stats


def loop(every: float, heartbeat: str | None = None) -> None:
    """Run a tick every `every` seconds until SIGTERM; a failed tick is logged and retried next round.
    The heartbeat file is touched after each successful tick for the liveness probe.
    Chạy tick mỗi `every` giây tới khi nhận SIGTERM; tick lỗi được ghi log và thử lại ở vòng sau.
    File heartbeat được chạm sau mỗi tick thành công để liveness probe kiểm tra.
    """
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        started = monotonic()
        try:
            run()
            if heartbeat:
                Path(heartbeat).touch()
        except Exception:
            log.exception("tick failed")
        stop.wait(max(1.0, every - (monotonic() - started)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--now", type=datetime.fromisoformat, help="pretend time (ICT), for catch-up tests")
    parser.add_argument("--every", type=float, help="run forever, one tick every N seconds")
    parser.add_argument("--heartbeat", help="file touched after each successful tick")
    args = parser.parse_args()
    if args.every:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
        loop(args.every, args.heartbeat)
        return
    now = args.now.replace(tzinfo=args.now.tzinfo or ICT) if args.now else None
    print(json.dumps(run(now), indent=1))


if __name__ == "__main__":
    main()
