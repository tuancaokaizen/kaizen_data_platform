"""Master build and rebaseline plan on the real reference geography with synthetic shops.
Dựng master và kế hoạch rebaseline trên địa lý tham chiếu thật với shop giả lập.
"""
import os
from datetime import date

import pytest

from jobs.kaizen_sim import master_supply, rebaseline
from jobs.kaizen_sim.settings import MAX_SERVING_KM

REF = os.path.join(os.path.dirname(__file__), "..", "..", "..", "schema_manager", "reference")


@pytest.fixture(scope="module")
def built():
    wards, rows = master_supply.load_reference(REF)
    mainland = sorted(w.id for w in wards.values() if w.road and w.sub_region != "CON_DAO")
    shops = {f"SHOP{i:04d}": (mainland[(i * 7) % len(mainland)], "addr") for i in range(1, 51)}
    shops["SHOP0011"] = ("W162", "island")
    shops["SHOP0035"] = ("W162", "island")
    products = {f"PROD{i:04d}": {"type": "medicine" if i % 2 else "supplement", "unit": ["Hộp", "Chai", "Vỉ", "Viên"][i % 4],
                                 "stock_price": 10_000 + i, "indication": "Hỗ trợ điều trị bệnh về tim mạch",
                                 "brand": "Blackmores (Úc)"} for i in range(1, 41)}
    opening = {(s, p): 60 * (1 + (int(s[-2:]) * 3 + int(p[-2:])) % 9) for s in shops for p in products}
    inp = master_supply.MasterInputs(wards, rows, shops, {s: 20 for s in shops}, opening, products, date(2026, 10, 12))
    return inp, master_supply.build(inp)


def test_tiers_and_islands(built):
    inp, data = built
    tiers = [s["tier"] for s in data.shops.values()]
    assert (tiers.count("A"), tiers.count("B"), tiers.count("C")) == (10, 20, 20)
    assert data.shops["SHOP0011"]["ward_id"] == "W126"
    assert data.shops["SHOP0035"]["ward_id"] == "W150"
    assert not [w for w in data.warnings if "road access" in w]


def test_distance_matrix_complete(built):
    _, data = built
    nodes = 3 + 50
    assert len(data.distances) == nodes * (nodes - 1)
    assert all(r[5] >= r[4] and r[8] >= r[7] for r in data.distances)
    assert len(data.lanes) == 50 and len({l[2] for l in data.lanes}) == 50


def test_serving_distance_limit_reported(built):
    _, data = built
    far = [l for l in data.lanes if l[4] > MAX_SERVING_KM]
    assert len(far) == len([w for w in data.warnings if " km from " in w])


def test_policies_and_capacity(built):
    inp, data = built
    shop_rows = [r for r in data.policies if r[1] == "SHOP"]
    assert len(shop_rows) == len(inp.opening)
    assert all(r[7] >= r[6] >= 0 and r[5] > 0 for r in shop_rows)
    assert all(c[2] > 0 and c[5] in (1, 2) for c in data.capacities)
    assert master_supply.review_days("A") == 3 and master_supply.review_days("B") == 4
    assert master_supply.review_days("C") == 7


def test_rebaseline_plan_conserves_network_stock(built):
    inp, data = built
    levels = {(r[2], r[3]): (r[6], r[7]) for r in data.policies if r[1] == "SHOP"}
    serving = {c: s["dc"] for c, s in data.shops.items()}
    moves = rebaseline.plan(inp.opening, levels, serving, {}, {})
    assert moves
    for m in moves:
        s, big_s = levels[(m.shop, m.product_id)]
        kept = inp.opening[(m.shop, m.product_id)] - m.shop_out
        assert s <= kept <= big_s
        assert m.dc_in == m.shop_out
