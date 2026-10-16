"""Pure-function tests for rng, geo, policy and routing. / Test hàm thuần cho rng, geo, policy và routing."""
import random

import pytest

from jobs.kaizen_sim import geo, policy, rng, routing


def test_rng_is_deterministic_and_bounded():
    assert rng.unit("a", 1) == rng.unit("a", 1)
    assert rng.unit("a", 1) != rng.unit("a", 2)
    draws = [rng.integer(3, 5, "k", i) for i in range(2000)]
    assert set(draws) == {3, 4, 5}
    assert abs(sum(rng.normal(10, 2, "n", i) for i in range(4000)) / 4000 - 10) < 0.2


def test_haversine_known_pair():
    # DC01 Tân Tạo -> DC02 Bến Cát, about 51 km straight / DC01 -> DC02 khoảng 51 km chim bay
    km = geo.haversine_km(10.744320, 106.584224, 11.208668, 106.628184)
    assert 51 < km < 52


@pytest.mark.parametrize("a,b,factor", [
    ("SG_CORE", "SG_CORE", 1.35), ("SG_OUTER", "SG_OUTER", 1.25), ("BA_RIA_VUNG_TAU", "BA_RIA_VUNG_TAU", 1.30),
    ("SG_CORE", "BINH_DUONG", 1.30), ("BINH_DUONG", "BA_RIA_VUNG_TAU", 1.30), ("SG_OUTER", "BINH_DUONG", 1.25),
])
def test_circuity_rules(a, b, factor):
    assert geo.circuity(a, b) == factor


def test_leg_peak_not_faster_than_off_peak():
    leg = geo.leg(10.7, 106.6, "SG_CORE", 10.8, 106.7, "SG_CORE")
    assert leg["road_km"] >= leg["straight_km"]
    assert leg["peak_min"] >= leg["off_peak_min"] > 0


def test_place_in_ward_stays_inside_polygon():
    square = {"type": "Polygon", "coordinates": [[[106.0, 10.0], [106.1, 10.0], [106.1, 10.1], [106.0, 10.1],
                                                  [106.0, 10.0]]]}
    for key in ("SHOP0001", "SHOP0002", "DC01"):
        lat, lon = geo.place_in_ward(key, 10.05, 106.05, 120.0, square)
        assert geo.in_geometry(lon, lat, square)
        assert geo.haversine_km(10.05, 106.05, lat, lon) <= 1.5 + 1e-6


def test_shop_levels_formula():
    lv = policy.shop_levels(mu=10, sigma=2, review_days=3, lead_days=1, z=1.28)
    # s = 10*4 + 1.28*2*2 = 45.12 -> 46, S = s + 10*3 / s làm tròn lên 46, S = s + mu*R
    assert lv.reorder_point == 46
    assert lv.order_up_to == 76
    assert lv.order_up_to >= lv.reorder_point >= lv.safety_stock


def test_order_qty_and_cases():
    lv = policy.Levels(10, 30, 3)
    assert policy.order_qty(11, lv) == 0
    assert policy.order_qty(10, lv) == 20
    assert policy.round_to_case(21, 20) == 40
    assert policy.round_to_case(5, 20, 60) == 60
    assert policy.round_to_case(0, 20) == 0


def test_allocate_never_exceeds_stock_or_request():
    rnd = random.Random(7)
    for _ in range(300):
        req = {f"S{i}": rnd.randint(1, 50) for i in range(rnd.randint(1, 8))}
        avail = rnd.randint(0, 200)
        grant = policy.allocate(req, avail)
        assert sum(grant.values()) == min(avail, sum(req.values()))
        assert all(0 <= grant[k] <= req[k] for k in req)


def test_scale_to_capacity_fits_free_space():
    lines = {"P1": 100, "P2": 50}
    vol = {"P1": 1000, "P2": 2000}
    scaled = policy.scale_to_capacity(lines, vol, used_m3=0.1, capacity_m3=0.15)
    assert sum(q * vol[p] for p, q in scaled.items()) / 1e6 <= 0.05 + 1e-9
    assert policy.scale_to_capacity(lines, vol, 0.0, 10.0) == lines


def _load(key, shop, kg, core=False, cold=False):
    return routing.Load(key, shop, f"TO-{shop}", core, cold, [routing.Line(f"{key}-i", "P", "L", 1, kg, kg / 200)])


def _fleet():
    van = lambda i: routing.Vehicle(f"VAN{i}", "CAR01", "VAN_1T", 1000, 6.0, 1.9, False)
    truck = lambda i: routing.Vehicle(f"TRK{i}", "CAR01", "TRUCK_2_5T", 2500, 14.0, 4.9, False)
    return [van(1), van(2), truck(1)], [routing.Vehicle("POOL1", "CAR03", "VAN_1T", 1000, 6.0, 1.9, False)]


def test_plan_trips_respects_capacity_stops_and_core_rule():
    rnd = random.Random(3)
    coords = {f"SH{i}": (rnd.random(), rnd.random()) for i in range(20)}
    coords["DC"] = (0.5, 0.5)
    km = lambda a, b: ((coords[a][0] - coords[b][0]) ** 2 + (coords[a][1] - coords[b][1]) ** 2) ** 0.5
    loads = [_load(f"L{i}", f"SH{i}", rnd.uniform(50, 600), core=i % 3 == 0) for i in range(20)]
    own, pool = _fleet()
    hired = []

    def on_demand(core, cold):
        v = routing.Vehicle(None, "CAR02", "VAN_1T" if core else "TRUCK_2_5T", 1000 if core else 2500,
                            6.0 if core else 14.0, 1.9 if core else 4.9, cold)
        hired.append(v)
        return v

    trips = routing.plan_trips("DC", loads, km, [own, pool], on_demand, 4)
    seen = [s.key for t in trips for s in t.stops]
    assert sorted(seen) == sorted(l.key for l in loads)
    for t in trips:
        assert 1 <= len(t.stops) <= 4
        assert sum(s.kg for s in t.stops) <= t.vehicle.capacity_kg + 1e-6
        assert all(t.vehicle.gross_t <= 2.5 for s in t.stops if s.core)
    used = [t.vehicle.id for t in trips if t.vehicle.id]
    assert len(used) == len(set(used))


def test_split_load_conserves_quantity():
    line = routing.Line("i1", "P1", "L1", 5000, 0.4, 0.001)
    load = routing.Load("TO-1-D", "SH1", "TO-1", False, False, [line])
    parts = routing.split_load(load, 1000, 6.0)
    assert len(parts) == 2
    assert sum(l.qty for p in parts for l in p.lines) == 5000
    assert all(p.kg <= 1000 + 1e-6 for p in parts)
