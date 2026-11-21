"""Greedy (s, S) replenishment with capacity scaling — no extra dependency.
Bổ sung (s, S) kiểu tham lam có scale capacity — không phụ thuộc thêm.
"""
from __future__ import annotations

import math
from collections import defaultdict

from jobs.kaizen_analytics.solvers.base import (
    ReplenishmentLine,
    SolverInput,
    SolverResult,
)


class GreedyReplenishmentSolver:
    """Excel-like target-stock heuristic: order up to S when on-hand <= s, then shrink to capacity.
    Heuristic kiểu Excel: đặt lên S khi tồn <= s, rồi co theo sức chứa.
    """

    name = "greedy"

    def solve(self, payload: SolverInput) -> SolverResult:
        stock_map = {(row.shop_code, row.product_id): row.on_hand for row in payload.stocks}
        capacity_map = {row.shop_code: row for row in payload.capacities}
        draft: dict[str, list[ReplenishmentLine]] = defaultdict(list)

        for demand in payload.demands:
            on_hand = float(stock_map.get((demand.shop_code, demand.product_id), 0.0))
            horizon = payload.cover_days
            safety = payload.service_z * demand.daily_std * math.sqrt(max(horizon, 1))
            reorder = math.ceil(demand.daily_mean * horizon + safety)
            target = max(reorder, math.ceil(reorder + demand.daily_mean * max(horizon / 2, 1)))
            qty = 0
            reason = "ok"
            if on_hand <= reorder:
                qty = max(0, int(target - on_hand))
                reason = "below_reorder_point"
            else:
                reason = "above_reorder_point"
            volume = qty * max(demand.unit_volume_m3, 0.0)
            cost = qty * max(demand.unit_cost, 0.0)
            draft[demand.shop_code].append(
                ReplenishmentLine(
                    shop_code=demand.shop_code,
                    product_id=demand.product_id,
                    on_hand=on_hand,
                    reorder_point=float(reorder),
                    target_level=float(target),
                    suggested_qty=qty,
                    estimated_volume_m3=volume,
                    estimated_cost=cost,
                    reason=reason,
                )
            )

        lines: list[ReplenishmentLine] = []
        for shop_code, shop_lines in draft.items():
            capacity = capacity_map.get(shop_code)
            free = None if capacity is None else max(capacity.storage_m3 - capacity.used_m3, 0.0)
            ordered_volume = sum(line.estimated_volume_m3 for line in shop_lines)
            scale = 1.0
            if free is not None and ordered_volume > free > 0:
                scale = free / ordered_volume
            elif free is not None and free <= 0:
                scale = 0.0
            for line in shop_lines:
                qty = int(line.suggested_qty * scale) if scale < 1 else line.suggested_qty
                reason = line.reason
                if scale < 1 and line.suggested_qty > 0:
                    reason = f"{line.reason}|scaled_to_capacity:{scale:.3f}"
                lines.append(
                    ReplenishmentLine(
                        shop_code=line.shop_code,
                        product_id=line.product_id,
                        on_hand=line.on_hand,
                        reorder_point=line.reorder_point,
                        target_level=line.target_level,
                        suggested_qty=qty,
                        estimated_volume_m3=qty * (line.estimated_volume_m3 / line.suggested_qty if line.suggested_qty else 0.0),
                        estimated_cost=qty * (line.estimated_cost / line.suggested_qty if line.suggested_qty else 0.0),
                        reason=reason,
                    )
                )

        objective = sum(line.estimated_cost for line in lines if line.suggested_qty > 0)
        return SolverResult(backend=self.name, lines=lines, objective_cost=objective, notes="greedy_(s,S)+capacity")
