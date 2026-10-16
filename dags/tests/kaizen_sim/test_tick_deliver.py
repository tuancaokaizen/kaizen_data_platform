from datetime import datetime, timezone

from jobs.kaizen_sim import tick
from jobs.kaizen_sim.events import Event, ShipmentPlan, StopPlan

AT = datetime(2026, 10, 15, 3, 0, tzinfo=timezone.utc)


class FakeCursor:
    """Records SQL; answers the product cost lookup. / Ghi lại SQL; trả lời truy vấn giá vốn."""

    def __init__(self):
        self.sql = []
        self._rows = []

    def execute(self, sql, params=None):
        self.sql.append(sql)
        self._rows = [("P1", 1000), ("P2", 2000)] if 'FROM product."Product"' in sql else []

    def executemany(self, sql, rows):
        self.sql.append(sql)

    def fetchall(self):
        return self._rows


def _deliver(received, monkeypatch):
    moves = []
    monkeypatch.setattr(tick.db, "apply_movement", lambda cur, *a: moves.append(a))
    stop = StopPlan("ST1", 1, "SHOP0001", AT, {"I1": 10, "I2": 5}, "SH1")
    sh = ShipmentPlan("SH1", "DC01", "CAR01", None, AT, AT, 0, 0, 0, 0.9, 0.5, [stop])
    items = {"I1": {"product": "P1", "lot": "L1", "qty": 10, "to": "TO1", "to_item": "TI1"},
             "I2": {"product": "P2", "lot": "L2", "qty": 5, "to": "TO1", "to_item": "TI2"}}
    ev = Event(9, "PARTIAL" if received != {"I1": 10, "I2": 5} else "DELIVERED", AT, "PARTIAL", stop,
               received=received)
    tick._deliver(FakeCursor(), sh, ev, items)
    return moves


def test_short_pick_returns_units_to_dc(monkeypatch):
    moves = _deliver({"I1": 7, "I2": 0}, monkeypatch)
    shop_in = [m for m in moves if m[1] == "TRANSFER_IN"]
    back = [m for m in moves if m[1] == "ADJUST"]
    assert [(m[0], m[3], m[5], m[6]) for m in shop_in] == [("SM-SHIP-I1-IN", "SHOP0001", "L1", 7)]
    assert [(m[0], m[2], m[3], m[6], m[11]) for m in back] == [
        ("SM-SHIP-I1-SHORT", "DC", "DC01", 3, "SHORT_PICK"),
        ("SM-SHIP-I2-SHORT", "DC", "DC01", 5, "SHORT_PICK"),
    ]
    # Shipped = received at the shop + returned to the DC / Đã xuất = shop nhận + trả về DC
    assert sum(m[6] for m in shop_in + back) == 15


def test_full_delivery_posts_no_adjustment(monkeypatch):
    moves = _deliver({"I1": 10, "I2": 5}, monkeypatch)
    assert {m[1] for m in moves} == {"TRANSFER_IN"}
