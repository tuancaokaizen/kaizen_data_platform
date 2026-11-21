"""Solver contracts and shared data shapes.
Hợp đồng solver và hình dạng dữ liệu dùng chung.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class DemandSignal:
    shop_code: str
    product_id: str
    daily_mean: float
    daily_std: float
    unit_volume_m3: float
    unit_cost: float


@dataclass(frozen=True)
class StockPosition:
    shop_code: str
    product_id: str
    on_hand: float


@dataclass(frozen=True)
class ShopCapacity:
    shop_code: str
    storage_m3: float
    used_m3: float


@dataclass
class SolverInput:
    demands: list[DemandSignal] = field(default_factory=list)
    stocks: list[StockPosition] = field(default_factory=list)
    capacities: list[ShopCapacity] = field(default_factory=list)
    cover_days: int = 7
    service_z: float = 1.65


@dataclass(frozen=True)
class ReplenishmentLine:
    shop_code: str
    product_id: str
    on_hand: float
    reorder_point: float
    target_level: float
    suggested_qty: int
    estimated_volume_m3: float
    estimated_cost: float
    reason: str


@dataclass(frozen=True)
class SolverResult:
    backend: str
    lines: list[ReplenishmentLine]
    objective_cost: float
    notes: str = ""


class ReplenishmentSolver(Protocol):
    name: str

    def solve(self, payload: SolverInput) -> SolverResult:
        ...
