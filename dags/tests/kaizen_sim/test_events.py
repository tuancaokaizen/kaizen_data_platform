"""Timeline and payload invariants that LogisticsRules / ShipmentRealtimeFunction rely on.
Bất biến của dòng thời gian và payload mà LogisticsRules / ShipmentRealtimeFunction dựa vào.
"""
from datetime import datetime, timedelta

import pytest

from jobs.kaizen_sim.events import ShipmentPlan, StopPlan, payload, timeline
from jobs.kaizen_sim.settings import ICT

STATUSES = {"PLANNED", "PICKED", "IN_TRANSIT", "COMPLETED", "PARTIAL", "FAILED"}
REASONS = {"WEATHER", "TRAFFIC", "VEHICLE", "HOLIDAY", "SHORT_PICK", "TRUCK_BAN"}
STOP_TYPES = {"ARRIVED_STOP", "DELIVERED", "PARTIAL"}


def _plan(i: int, stops: int = 3, on_time: float = 0.85) -> ShipmentPlan:
    depart = datetime(2026, 7, 14, 6, 0, tzinfo=ICT)
    plans = [StopPlan(f"SHP-{i}-S{k}", k, f"SHOP{k:04d}", depart + timedelta(minutes=50 * k),
                      {f"SHP-{i}-S{k}-P{j}": 10 + j for j in range(3)}) for k in range(1, stops + 1)]
    return ShipmentPlan(f"SHP-{i}", "DC01", "CAR02", None, depart, depart.replace(hour=0, minute=10), 500.0, 3.0,
                        900_000, on_time, 0.8, plans)


@pytest.mark.parametrize("i", range(400))
def test_timeline_invariants(i):
    sh = _plan(i, stops=1 + i % 4, on_time=0.5 if i % 2 else 0.95)
    events = timeline(sh)
    assert [e.seq for e in events] == list(range(1, len(events) + 1))
    assert all(a.at <= b.at for a, b in zip(events, events[1:]))
    assert [e.type for e in events[:3]] == ["SHIPMENT_PLANNED", "PICKED", "DEPARTED"]
    last = events[-1]
    assert last.type in ("DELIVERED", "PARTIAL") and last.stop.seq == len(sh.stops)
    assert last.status in ("COMPLETED", "PARTIAL")
    outcomes = [e for e in events if e.type in ("DELIVERED", "PARTIAL")]
    assert sorted(e.stop.seq for e in outcomes) == list(range(1, len(sh.stops) + 1))
    assert (last.status == "PARTIAL") == any(e.type == "PARTIAL" for e in outcomes)
    for e in outcomes:
        assert set(e.received) == set(e.stop.items)
        assert all(0 <= e.received[k] <= e.stop.items[k] for k in e.received)
    for e in events:
        body = payload(sh, e)
        for key in ("shipmentId", "eventSeq", "eventType", "status", "originCode", "plannedDeparture", "stopCount",
                    "occurredAt"):
            assert body[key] is not None
        assert body["status"] in STATUSES
        assert body["reasonCode"] is None or body["reasonCode"] in REASONS
        if e.type in STOP_TYPES:
            assert body["stop"] is not None and 1 <= body["stop"]["stopSeq"] <= body["stopCount"]
            assert body["stop"]["actualArrival"] is not None


def test_timeline_is_stable():
    assert timeline(_plan(1)) == timeline(_plan(1))


def test_delays_happen_but_not_everywhere():
    events = [e for i in range(500) for e in timeline(_plan(i, on_time=0.85))]
    delayed = sum(e.type == "DELAYED" for e in events)
    partial = sum(e.type == "PARTIAL" for e in events)
    stops = 500 * 3
    assert 0.05 * stops < delayed < 0.5 * stops
    assert 0 < partial < 0.15 * stops
