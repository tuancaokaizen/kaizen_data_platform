from jobs.kaizen_sim.rebaseline import target_level
from jobs.kaizen_sim.resize import Level, dc_target, plan, scale

SERVING = {"SHOP0001": "DC01", "SHOP0002": "DC01"}


def test_scale_keeps_level_order():
    assert scale(Level(33, 55, 8), 10 / 7) == Level(47, 79, 11)
    assert scale(Level(1, 1, 1), 0.1) == Level(1, 1, 0)
    lv = scale(Level(10, 12, 9), 3.0)
    assert 0 <= lv.safety_stock <= lv.reorder_point <= lv.order_up_to


def test_shops_are_topped_up_to_target_and_dc_cut_to_band():
    shop_levels = {("SHOP0001", "P1"): Level(40, 80, 10), ("SHOP0002", "P1"): Level(40, 80, 10)}
    dc_level = Level(500, 1500, 200)
    topups, cuts = plan({("SHOP0001", "P1"): 5, ("SHOP0002", "P1"): 500}, {("DC01", "P1"): 5000}, shop_levels,
                        {("DC01", "P1"): dc_level}, SERVING, 0.1, 0.8)
    assert topups == {("SHOP0001", "P1"): target_level("SHOP0001", "P1", 40, 80) - 5}
    keep = 5000 - topups[("SHOP0001", "P1")] - cuts[("DC01", "P1")]
    assert keep == dc_target("DC01", "P1", dc_level, 0.1, 0.8)
    assert 500 + 100 <= keep <= 500 + 800


def test_short_dc_splits_stock_and_cuts_nothing():
    shop_levels = {("SHOP0001", "P1"): Level(40, 80, 10), ("SHOP0002", "P1"): Level(40, 80, 10)}
    topups, cuts = plan({}, {("DC01", "P1"): 30}, shop_levels, {("DC01", "P1"): Level(500, 1500, 200)}, SERVING,
                        0.1, 0.8)
    assert sum(topups.values()) <= 30
    assert set(topups) == {("SHOP0001", "P1"), ("SHOP0002", "P1")}
    assert cuts == {}
