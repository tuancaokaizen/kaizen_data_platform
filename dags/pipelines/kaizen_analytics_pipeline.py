"""Daily Kaizen analytics: RFM+cluster+RF, per-segment FP-Growth, replenishment plan.
Analytics Kaizen hằng ngày: RFM+cụm+RF, FP-Growth theo phân khúc, kế hoạch bổ sung.
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta

import pendulum
from airflow import DAG  # type: ignore
from airflow.models.param import Param  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from jobs.kaizen_analytics.pipeline import run_analytics_pipeline  # noqa: E402
from jobs.kaizen_analytics.settings import AnalyticsSettings  # noqa: E402
from jobs.kaizen_analytics.steps.association_rules import AssociationRulesStep  # noqa: E402
from jobs.kaizen_analytics.steps.customer_segments import CustomerSegmentsStep  # noqa: E402
from jobs.kaizen_analytics.steps.replenishment_plan import ReplenishmentPlanStep  # noqa: E402
from jobs.clickhouse_utils import run_clickhouse_views_setup  # noqa: E402

ICT = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "kaizen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _parse_as_of(raw: str | None) -> date:
    if raw:
        return date.fromisoformat(raw)
    return datetime.now(tz=ICT).date() - timedelta(days=1)


def run_customer_segments(**context):
    conf = context.get("dag_run").conf or {}
    as_of = _parse_as_of(conf.get("asOf") or context["params"].get("asOf"))
    return [
        result.__dict__
        for result in run_analytics_pipeline(
            as_of=as_of,
            settings=AnalyticsSettings.from_env(),
            steps=(CustomerSegmentsStep(),),
            run_id=context["run_id"],
        )
    ]


def run_association_rules(**context):
    conf = context.get("dag_run").conf or {}
    as_of = _parse_as_of(conf.get("asOf") or context["params"].get("asOf"))
    return [
        result.__dict__
        for result in run_analytics_pipeline(
            as_of=as_of,
            settings=AnalyticsSettings.from_env(),
            steps=(AssociationRulesStep(),),
            run_id=context["run_id"],
        )
    ]


def run_replenishment_plan(**context):
    conf = context.get("dag_run").conf or {}
    as_of = _parse_as_of(conf.get("asOf") or context["params"].get("asOf"))
    return [
        result.__dict__
        for result in run_analytics_pipeline(
            as_of=as_of,
            settings=AnalyticsSettings.from_env(),
            steps=(ReplenishmentPlanStep(),),
            run_id=context["run_id"],
        )
    ]


with DAG(
    "kaizen_analytics_pipeline",
    default_args=default_args,
    description="RFM+KMeans+RF segments → per-segment FP-Growth + replenishment solver",
    schedule_interval="0 3 * * *",  # after lakehouse (~01:55 UTC)
    start_date=datetime(2026, 11, 1, tzinfo=ICT),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["kaizen", "analytics", "ml"],
    params={
        "asOf": Param(
            None,
            type=["null", "string"],
            description="As-of date yyyy-MM-dd (default yesterday ICT)",
        ),
    },
) as dag:
    refresh_views = PythonOperator(
        task_id="refresh_report_views",
        python_callable=run_clickhouse_views_setup,
    )
    segments = PythonOperator(
        task_id="customer_segments",
        python_callable=run_customer_segments,
    )
    rules = PythonOperator(
        task_id="association_rules",
        python_callable=run_association_rules,
    )
    plan = PythonOperator(
        task_id="replenishment_plan",
        python_callable=run_replenishment_plan,
    )

    # Cycle: segments (labels + checkpoint) → association rules per segment
    # Cycle: phân khúc (nhãn + checkpoint) → luật kết hợp theo từng phân khúc
    # Views refresh last so VI aliases see the enriched columns
    # Refresh view cuối để alias VI thấy đủ cột mới
    segments >> rules
    segments >> plan
    [rules, plan] >> refresh_views
