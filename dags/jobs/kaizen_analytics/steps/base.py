"""Shared contracts for analytics steps.
Hợp đồng dùng chung cho các bước analytics.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

from jobs.kaizen_analytics.clickhouse_client import ClickHouseClient
from jobs.kaizen_analytics.settings import AnalyticsSettings


@dataclass(frozen=True)
class StepContext:
    """Runtime context passed to every step.
    Ngữ cảnh runtime truyền vào mọi bước.
    """

    client: ClickHouseClient
    settings: AnalyticsSettings
    as_of: date
    run_id: str


@dataclass(frozen=True)
class StepResult:
    """What a step wrote, for Airflow logs and downstream checks.
    Bước đã ghi gì, phục vụ log Airflow và kiểm tra phía sau.
    """

    name: str
    rows_written: int
    detail: str = ""


class AnalyticsStep(Protocol):
    """One replaceable analytics unit.
    Một đơn vị analytics có thể thay thế.
    """

    name: str

    def run(self, ctx: StepContext) -> StepResult:
        ...
