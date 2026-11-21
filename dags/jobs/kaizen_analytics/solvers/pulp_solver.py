"""PuLP / CBC backend — optional; falls back is handled by the factory.
Backend PuLP / CBC — tuỳ chọn; factory sẽ fallback nếu thiếu thư viện.
"""
from __future__ import annotations

import math
from collections import defaultdict

from jobs.kaizen_analytics.solvers.base import (
    ReplenishmentLine,
    SolverInput,
    SolverResult,
)
from jobs.kaizen_analytics.solvers.greedy import GreedyReplenishmentSolver


class PulpReplenishmentSolver:
    """Minimize purchase cost subject to (s,S) targets and shop volume capacity.
    Tối thiểu chi phí mua với mục tiêu (s,S) và ràng buộc thể tích cửa hàng.
    """

    name = "pulp"

    def solve(self, payload: SolverInput) -> SolverResult:
        try:
            import pulp  # type: ignore
        except ImportError as exc:
            raise RuntimeError("PuLP is not installed") from exc

        stock_map = {(row.shop_code, row.product_id): row.on_hand for row in payload.stocks}
        capacity_map = {row.shop_code: row for row in payload.capacities}

        # Pre-compute targets with the same (s,S) formula as greedy
        # Tính trước mục tiêu bằng cùng công thức (s,S) như greedy
        candidates: list[tuple] = []
        for demand in payload.demands:
            on_hand = float(stock_map.get((demand.shop_code, demand.product_id), 0.0))
            horizon = payload.cover_days
            safety = payload.service_z * demand.daily_std * math.sqrt(max(horizon, 1))
            reorder = math.ceil(demand.daily_mean * horizon + safety)
            target = max(reorder, math.ceil(reorder + demand.daily_mean * max(horizon / 2, 1)))
            need = max(0, int(target - on_hand)) if on_hand <= reorder else 0
            candidates.append((demand, on_hand, float(reorder), float(target), need))

        problem = pulp.LpProblem("kaizen_replenishment", pulp.LpMinimize)
        qty_vars = {}
        for demand, on_hand, reorder, target, need in candidates:
            key = (demand.shop_code, demand.product_id)
            # Soft upper bound: never order more than the (s,S) need
            # Cận trên mềm: không đặt vượt nhu cầu (s,S)
            qty_vars[key] = pulp.LpVariable(
                f"q_{demand.shop_code}_{demand.product_id}".replace("-", "_"),
                lowBound=0,
                upBound=max(need, 0),
                cat="Integer",
            )

        # Purchase cost + penalty on unmet (s,S) need
        # Chi phí mua + phạt phần nhu cầu (s,S) chưa đáp ứng
        unmet = []
        for demand, on_hand, reorder, target, need in candidates:
            key = (demand.shop_code, demand.product_id)
            gap = pulp.LpVariable(f"gap_{demand.shop_code}_{demand.product_id}".replace("-", "_"), lowBound=0)
            problem += qty_vars[key] + gap == need
            unmet.append(gap * (max(demand.unit_cost, 1.0) * 5))
        problem += pulp.lpSum(
            qty_vars[(demand.shop_code, demand.product_id)] * max(demand.unit_cost, 0.0)
            for demand, *_ in candidates
        ) + pulp.lpSum(unmet)

        by_shop: dict[str, list] = defaultdict(list)
        for demand, on_hand, reorder, target, need in candidates:
            by_shop[demand.shop_code].append(demand)
        for shop_code, demands in by_shop.items():
            capacity = capacity_map.get(shop_code)
            if capacity is None:
                continue
            free = max(capacity.storage_m3 - capacity.used_m3, 0.0)
            problem += (
                pulp.lpSum(
                    qty_vars[(demand.shop_code, demand.product_id)] * max(demand.unit_volume_m3, 0.0)
                    for demand in demands
                )
                <= free
            )

        status = problem.solve(pulp.PULP_CBC_CMD(msg=False, timeLimit=60))
        if pulp.LpStatus[status] not in {"Optimal", "Feasible"}:
            # Safe fallback keeps the pipeline green / Fallback an toàn để pipeline không đỏ
            fallback = GreedyReplenishmentSolver().solve(payload)
            return SolverResult(
                backend="pulp->greedy",
                lines=fallback.lines,
                objective_cost=fallback.objective_cost,
                notes=f"pulp_status={pulp.LpStatus[status]}",
            )

        lines: list[ReplenishmentLine] = []
        for demand, on_hand, reorder, target, need in candidates:
            key = (demand.shop_code, demand.product_id)
            qty = int(pulp.value(qty_vars[key]) or 0)
            reason = "below_reorder_point" if need > 0 else "above_reorder_point"
            if need > 0 and qty < need:
                reason = f"{reason}|capacity_or_cost_limited"
            lines.append(
                ReplenishmentLine(
                    shop_code=demand.shop_code,
                    product_id=demand.product_id,
                    on_hand=on_hand,
                    reorder_point=reorder,
                    target_level=target,
                    suggested_qty=qty,
                    estimated_volume_m3=qty * max(demand.unit_volume_m3, 0.0),
                    estimated_cost=qty * max(demand.unit_cost, 0.0),
                    reason=reason,
                )
            )
        objective = sum(line.estimated_cost for line in lines)
        return SolverResult(backend=self.name, lines=lines, objective_cost=objective, notes="pulp_cbc")
