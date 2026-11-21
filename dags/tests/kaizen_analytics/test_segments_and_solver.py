"""RFM labels and greedy solver unit tests.
Kiểm thử nhãn RFM và solver greedy.
"""
from jobs.kaizen_analytics.solvers.base import DemandSignal, ShopCapacity, SolverInput, StockPosition
from jobs.kaizen_analytics.solvers.greedy import GreedyReplenishmentSolver
from jobs.kaizen_analytics.steps.customer_segments import label_segment


def test_label_segment_champions_and_at_risk():
    code, name = label_segment(5, 5, 5)
    assert code == "champions"
    assert name == "Khách tinh hoa"
    assert label_segment(1, 5, 3)[0] == "at_risk"
    assert label_segment(5, 1, 2) == ("new_high_intent", "Khách mới / tiềm năng cao")


def test_greedy_orders_when_below_reorder_and_scales_to_capacity():
    solver = GreedyReplenishmentSolver()
    result = solver.solve(
        SolverInput(
            demands=[
                DemandSignal("S1", "P1", daily_mean=10, daily_std=2, unit_volume_m3=1.0, unit_cost=100),
                DemandSignal("S1", "P2", daily_mean=10, daily_std=2, unit_volume_m3=1.0, unit_cost=50),
            ],
            stocks=[
                StockPosition("S1", "P1", on_hand=0),
                StockPosition("S1", "P2", on_hand=0),
            ],
            capacities=[ShopCapacity("S1", storage_m3=10, used_m3=0)],
            cover_days=7,
            service_z=1.0,
        )
    )
    assert result.backend == "greedy"
    suggested = {line.product_id: line.suggested_qty for line in result.lines}
    assert suggested["P1"] > 0
    assert suggested["P2"] > 0
    # Capacity 10 m3 must bind when each unit is 1 m3
    # Sức chứa 10 m3 phải siết khi mỗi đơn vị 1 m3
    assert sum(line.suggested_qty for line in result.lines) <= 10
