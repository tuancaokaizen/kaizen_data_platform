"""Pick a solver backend from settings.
Chọn backend solver từ cấu hình.
"""
from __future__ import annotations

import logging

from jobs.kaizen_analytics.solvers.base import ReplenishmentSolver
from jobs.kaizen_analytics.solvers.greedy import GreedyReplenishmentSolver
from jobs.kaizen_analytics.solvers.pulp_solver import PulpReplenishmentSolver

log = logging.getLogger(__name__)


def build_solver(backend: str = "auto") -> ReplenishmentSolver:
    choice = (backend or "auto").strip().lower()
    if choice == "greedy":
        return GreedyReplenishmentSolver()
    if choice == "pulp":
        return PulpReplenishmentSolver()
    if choice == "auto":
        try:
            import pulp  # noqa: F401
            log.info("solver backend=pulp")
            return PulpReplenishmentSolver()
        except ImportError:
            log.info("PuLP missing — solver backend=greedy")
            return GreedyReplenishmentSolver()
    raise ValueError(f"Unknown solver backend: {backend}")
