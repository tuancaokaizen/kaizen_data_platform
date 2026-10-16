"""Trip building for one DC and one day: farthest-seed greedy packing by weight and volume, nearest-neighbour
stop order, at most `max_stops` stops; heavy trucks never enter core wards.
Lập chuyến cho một DC trong một ngày: gom hàng tham lam bắt đầu từ điểm xa nhất theo khối lượng và thể tích,
thứ tự điểm dừng láng giềng gần nhất, tối đa `max_stops` điểm; xe tải nặng không vào phường nội đô.
"""
from dataclasses import dataclass, field
from typing import Callable

from .settings import CORE_MAX_GROSS_T


@dataclass
class Line:
    """Goods of one transfer-order line from one DC lot. / Hàng của một dòng lệnh điều chuyển từ một lô DC."""
    item_id: str
    product_id: str
    lot_id: str
    qty: int
    unit_kg: float
    unit_m3: float


@dataclass
class Load:
    """What one stop receives from one trip. / Hàng một điểm dừng nhận trong một chuyến."""
    key: str
    shop: str
    transfer_order_id: str
    core: bool
    cold: bool
    lines: list = field(default_factory=list)

    @property
    def kg(self) -> float:
        return sum(l.qty * l.unit_kg for l in self.lines)

    @property
    def m3(self) -> float:
        return sum(l.qty * l.unit_m3 for l in self.lines)


@dataclass
class Vehicle:
    id: str | None
    carrier_id: str
    type: str
    capacity_kg: float
    capacity_m3: float
    gross_t: float
    cold: bool

    def allows(self, load: Load) -> bool:
        return self.cold == load.cold and (not load.core or self.gross_t <= CORE_MAX_GROSS_T)


@dataclass
class Trip:
    vehicle: Vehicle
    stops: list


def split_load(load: Load, cap_kg: float, cap_m3: float) -> list:
    """Cut a load bigger than one vehicle into parts that each fit, splitting line quantities if needed.
    Cắt hàng lớn hơn một xe thành nhiều phần vừa xe, tách cả số lượng của dòng nếu cần.
    """
    if load.kg <= cap_kg and load.m3 <= cap_m3:
        return [load]
    parts, current, kg, m3 = [], [], 0.0, 0.0
    for line in sorted(load.lines, key=lambda l: (l.product_id, l.lot_id, l.item_id)):
        left = line.qty
        while left > 0:
            room_kg = (cap_kg - kg) / line.unit_kg if line.unit_kg > 0 else float("inf")
            room_m3 = (cap_m3 - m3) / line.unit_m3 if line.unit_m3 > 0 else float("inf")
            take = int(min(left, room_kg, room_m3))
            if take <= 0:
                # Current part is full / Phần hiện tại đã đầy
                parts.append(current)
                current, kg, m3 = [], 0.0, 0.0
                continue
            current.append(Line(line.item_id, line.product_id, line.lot_id, take, line.unit_kg, line.unit_m3))
            kg += take * line.unit_kg
            m3 += take * line.unit_m3
            left -= take
    if current:
        parts.append(current)
    return [Load(f"{load.key}-P{i + 1}", load.shop, load.transfer_order_id, load.core, load.cold, lines)
            for i, lines in enumerate(parts)]


def _fits(vehicle: Vehicle, kg: float, m3: float) -> bool:
    return kg <= vehicle.capacity_kg + 1e-9 and m3 <= vehicle.capacity_m3 + 1e-9


def plan_trips(origin: str, loads: list, km: Callable[[str, str], float], tiers: list,
               on_demand: Callable[[bool, bool], Vehicle], max_stops: int) -> list:
    """Build trips from `origin`; `tiers` are vehicle lists in preference order (own fleet, shared pool) and
    are consumed in place, `on_demand(core, cold)` hires a vehicle when the tiers run out.
    Lập chuyến từ `origin`; `tiers` là các danh sách xe theo thứ tự ưu tiên (xe nhà, xe dùng chung) và bị
    dùng dần tại chỗ, `on_demand(core, cold)` thuê xe khi các tầng đã hết.
    """
    remaining = sorted(loads, key=lambda l: l.key)
    trips = []
    while remaining:
        seed = max(remaining, key=lambda l: (km(origin, l.shop), l.key))

        def usable(v: Vehicle) -> bool:
            return v.allows(seed) and _fits(v, seed.kg, seed.m3)

        tier = next((t for t in tiers if any(usable(v) for v in t)), None)
        if tier is None:
            vehicle = on_demand(seed.core, seed.cold)
        else:
            vehicle = max((v for v in tier if usable(v)), key=lambda v: (v.capacity_kg, v.capacity_m3, v.id))
        stops = [seed]
        remaining.remove(seed)
        kg, m3 = seed.kg, seed.m3
        while len(stops) < max_stops:
            options = [l for l in remaining if vehicle.allows(l) and l.shop not in {s.shop for s in stops}
                       and _fits(vehicle, kg + l.kg, m3 + l.m3)]
            if not options:
                break
            nxt = min(options, key=lambda l: (km(stops[-1].shop, l.shop), l.key))
            stops.append(nxt)
            remaining.remove(nxt)
            kg += nxt.kg
            m3 += nxt.m3
        if tier is not None:
            # Hand back a bigger vehicle when a smaller one of the same tier carries the trip
            # Trả lại xe lớn khi một xe nhỏ hơn cùng tầng chở được chuyến
            fitting = [v for v in tier if _fits(v, kg, m3) and all(v.allows(s) for s in stops)]
            vehicle = min(fitting, key=lambda v: (v.capacity_kg, v.capacity_m3, v.id))
            tier.remove(vehicle)
        trips.append(Trip(vehicle, order_stops(origin, stops, km)))
    return trips


def order_stops(origin: str, stops: list, km: Callable[[str, str], float]) -> list:
    """Nearest-neighbour visiting order from the DC. / Thứ tự ghé láng giềng gần nhất từ DC."""
    left = list(stops)
    ordered, here = [], origin
    while left:
        nxt = min(left, key=lambda s: (km(here, s.shop), s.key))
        ordered.append(nxt)
        left.remove(nxt)
        here = nxt.shop
    return ordered


def route_km(origin: str, stops: list, km: Callable[[str, str], float]) -> float:
    """DC -> first stop -> ... -> last stop, no return leg. / DC -> điểm đầu -> ... -> điểm cuối, không tính chiều về."""
    total, here = 0.0, origin
    for s in stops:
        total += km(here, s.shop)
        here = s.shop
    return total


def trip_cost(carrier: dict, distance_km: float, kg: float, stops: int) -> int:
    return int(round(carrier["fixed"] + distance_km * carrier["per_km"] + kg * carrier["per_kg"]
                     + stops * carrier["per_stop"]))
