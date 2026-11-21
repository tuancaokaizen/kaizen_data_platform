"""FP-Growth unit tests (no ClickHouse).
Kiểm thử đơn vị FP-Growth (không cần ClickHouse).
"""
from jobs.kaizen_analytics.mining.fpgrowth import mine_fpgrowth


def test_fpgrowth_finds_milk_bread_rule():
    baskets = [
        ["milk", "bread"],
        ["milk", "bread", "butter"],
        ["milk", "bread"],
        ["milk", "diaper"],
        ["bread", "butter"],
        ["milk", "bread", "butter"],
        ["beer", "diaper"],
        ["milk", "bread"],
    ]
    rules = mine_fpgrowth(
        baskets,
        min_support=0.3,
        min_confidence=0.6,
        min_lift=1.0,
        max_itemset_size=3,
        max_rules=50,
    )
    assert rules
    milk_to_bread = [
        rule
        for rule in rules
        if rule.antecedents == frozenset(["milk"]) and rule.consequents == frozenset(["bread"])
    ]
    assert milk_to_bread
    assert milk_to_bread[0].confidence >= 0.6


def test_fpgrowth_empty_when_no_multi_item_baskets():
    assert mine_fpgrowth([["a"], ["b"], []], min_support=0.01) == []
