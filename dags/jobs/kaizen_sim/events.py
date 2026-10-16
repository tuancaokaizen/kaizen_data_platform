"""Deterministic timeline of a shipment and its logistics-events payloads. The timeline is a pure function of
the planned rows, so every tick recomputes it and writes only the events that are due and not yet written.
Dòng thời gian tất định của một chuyến và payload logistics-events. Dòng thời gian là hàm thuần của các dòng
kế hoạch, nên mỗi tick tính lại và chỉ ghi các sự kiện đã đến giờ mà chưa ghi.
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from . import rng
from .settings import PARAMS, STOP_SERVICE_MIN, SupplyParams

RAINY_MONTHS = range(5, 12)
TYPE_ORDER = {"SHIPMENT_PLANNED": 0, "PICKED": 1, "DEPARTED": 2, "DELAYED": 3, "ARRIVED_STOP": 4,
              "DELIVERED": 5, "PARTIAL": 5}


@dataclass
class StopPlan:
    id: str
    seq: int
    shop: str
    planned_arrival: datetime
    # ShipmentItem id -> shipped qty / Id ShipmentItem -> số lượng xuất
    items: dict
    shipment_id: str = ""


@dataclass
class ShipmentPlan:
    id: str
    origin: str
    carrier_id: str
    vehicle_id: str | None
    planned_departure: datetime
    planned_at: datetime
    load_kg: float
    load_m3: float
    cost: int
    on_time_rate: float
    load_factor: float
    stops: list


@dataclass
class Event:
    seq: int
    type: str
    at: datetime
    status: str
    stop: StopPlan | None = None
    reason: str | None = None
    actual_arrival: datetime | None = None
    # ShipmentItem id -> received qty, only on DELIVERED / PARTIAL / Chỉ có ở DELIVERED / PARTIAL
    received: dict = field(default_factory=dict)


def _stop_delay(sh: ShipmentPlan, stop: StopPlan, p: SupplyParams) -> tuple:
    """Own delay of a stop in minutes and its reason. / Độ trễ riêng của điểm dừng (phút) và lý do."""
    rainy = sh.planned_departure.month in RAINY_MONTHS
    prob = (1.0 - sh.on_time_rate) * (1.0 + 0.5 * sh.load_factor) * (1.3 if rainy else 1.0)
    if not rng.chance(prob, sh.id, stop.seq, "delay"):
        return 0, None
    if rng.chance(p.vehicle_breakdown_share, sh.id, stop.seq, "breakdown"):
        # Breakdown: goods arrive the next day / Hỏng xe: hàng tới ngày hôm sau
        return 24 * 60, "VEHICLE"
    if rainy and rng.chance(0.4, sh.id, stop.seq, "weather"):
        return rng.integer(45, 240, sh.id, stop.seq, "minutes"), "WEATHER"
    return rng.integer(p.delay_threshold_min + 1, 150, sh.id, stop.seq, "minutes"), "TRAFFIC"


def _received(sh: ShipmentPlan, stop: StopPlan, p: SupplyParams) -> dict:
    """Received qty per item; a few stops lose part of one or two lines in transit.
    Số nhận theo dòng; một số điểm dừng mất một phần của một hai dòng trên đường.
    """
    received = dict(stop.items)
    if not received or not rng.chance(p.partial_stop_prob, sh.id, stop.seq, "partial"):
        return received
    keys = sorted(received)
    hits = {rng.choice(keys, sh.id, stop.seq, "short", i) for i in range(rng.integer(1, 2, sh.id, stop.seq, "n"))}
    for k in hits:
        qty = received[k]
        lost = max(1, int(qty * rng.uniform(0.05, 0.3, sh.id, stop.seq, k, "lost")))
        received[k] = max(0, qty - lost)
    return received


def timeline(sh: ShipmentPlan, p: SupplyParams = PARAMS) -> list:
    """All events of the trip, numbered 1..n in time order. / Mọi sự kiện của chuyến, đánh số 1..n theo thời gian."""
    jitter = timedelta(minutes=rng.integer(0, 20, sh.id, "depart"))
    # Picking never precedes planning, even for a late close / Soạn hàng không bao giờ trước lập kế hoạch
    picked_at = max(sh.planned_departure - timedelta(minutes=p.pick_lead_min), sh.planned_at + timedelta(minutes=1))
    raw = [
        ("SHIPMENT_PLANNED", sh.planned_at, None, None, None, {}),
        ("PICKED", picked_at, None, None, None, {}),
        ("DEPARTED", sh.planned_departure + jitter, None, None, None, {}),
    ]
    stops = sorted(sh.stops, key=lambda s: s.seq)
    extra, carried = 0, None
    outcomes = []
    for stop in stops:
        own, reason = _stop_delay(sh, stop, p)
        extra += own
        carried = reason or carried
        actual = stop.planned_arrival + jitter + timedelta(minutes=extra)
        late = (actual - stop.planned_arrival).total_seconds() / 60
        if late > p.delay_threshold_min:
            raw.append(("DELAYED", stop.planned_arrival + timedelta(minutes=p.delay_threshold_min), stop,
                        carried or "TRAFFIC", None, {}))
        raw.append(("ARRIVED_STOP", actual, stop, None, actual, {}))
        received = _received(sh, stop, p)
        kind = "PARTIAL" if any(received[k] < stop.items[k] for k in received) else "DELIVERED"
        outcomes.append(kind)
        raw.append((kind, actual + timedelta(minutes=STOP_SERVICE_MIN), stop,
                    "SHORT_PICK" if kind == "PARTIAL" else None, actual, received))

    final_status = "PARTIAL" if "PARTIAL" in outcomes else "COMPLETED"
    raw.sort(key=lambda e: (e[1], e[2].seq if e[2] else 0, TYPE_ORDER[e[0]]))
    last_stop = stops[-1].seq if stops else None
    events = []
    for i, (kind, at, stop, reason, actual, received) in enumerate(raw, start=1):
        if kind == "SHIPMENT_PLANNED":
            status = "PLANNED"
        elif kind == "PICKED":
            status = "PICKED"
        elif kind in ("DELIVERED", "PARTIAL") and stop.seq == last_stop:
            status = final_status
        else:
            status = "IN_TRANSIT"
        events.append(Event(i, kind, at, status, stop, reason, actual, received))
    return events


def _ms(dt: datetime | None) -> int | None:
    return None if dt is None else int(dt.timestamp() * 1000)


def payload(sh: ShipmentPlan, ev: Event) -> dict:
    """logistics-events body, see ShipmentEvent.java. / Thân logistics-events, xem ShipmentEvent.java."""
    stop = None
    if ev.stop is not None:
        stop = {"stopSeq": ev.stop.seq, "shopCode": ev.stop.shop, "plannedArrival": _ms(ev.stop.planned_arrival),
                "actualArrival": _ms(ev.actual_arrival)}
    return {
        "shipmentId": sh.id,
        "eventSeq": ev.seq,
        "eventType": ev.type,
        "status": ev.status,
        "carrierCode": sh.carrier_id,
        "vehicleId": sh.vehicle_id,
        "originCode": sh.origin,
        "plannedDeparture": _ms(sh.planned_departure),
        "stopCount": len(sh.stops),
        "stop": stop,
        "reasonCode": ev.reason,
        "loadKg": round(sh.load_kg, 2),
        "loadM3": round(sh.load_m3, 3),
        "costVnd": sh.cost,
        "occurredAt": _ms(ev.at),
    }
