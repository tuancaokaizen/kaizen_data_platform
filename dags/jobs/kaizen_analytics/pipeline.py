"""Orchestrate analytics steps; safe to call from Airflow or CLI.
Điều phối các bước analytics; gọi an toàn từ Airflow hoặc CLI.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone
from typing import Iterable, Sequence

from jobs.kaizen_analytics.clickhouse_client import ClickHouseClient
from jobs.kaizen_analytics.settings import AnalyticsSettings
from jobs.kaizen_analytics.steps import DEFAULT_STEPS
from jobs.kaizen_analytics.steps.base import AnalyticsStep, StepContext, StepResult

log = logging.getLogger(__name__)


def run_analytics_pipeline(
    *,
    as_of: date | None = None,
    settings: AnalyticsSettings | None = None,
    steps: Sequence[AnalyticsStep] | None = None,
    run_id: str | None = None,
) -> list[StepResult]:
    """Run selected steps sequentially and return per-step results.
    Chạy các bước đã chọn tuần tự và trả kết quả từng bước.
    """
    resolved_settings = settings or AnalyticsSettings.from_env()
    resolved_as_of = as_of or datetime.now(timezone.utc).date()
    resolved_run_id = run_id or f"analytics-{uuid.uuid4().hex[:12]}"
    client = ClickHouseClient()
    ctx = StepContext(
        client=client,
        settings=resolved_settings,
        as_of=resolved_as_of,
        run_id=resolved_run_id,
    )
    # Ensure report database exists once / Đảm bảo database report tồn tại một lần
    client.execute("CREATE DATABASE IF NOT EXISTS report")

    selected: Iterable[AnalyticsStep] = steps if steps is not None else DEFAULT_STEPS
    results: list[StepResult] = []
    for step in selected:
        log.info("analytics step start name=%s as_of=%s run_id=%s", step.name, resolved_as_of, resolved_run_id)
        result = step.run(ctx)
        log.info(
            "analytics step done name=%s rows=%s detail=%s",
            result.name,
            result.rows_written,
            result.detail,
        )
        results.append(result)
    return results
