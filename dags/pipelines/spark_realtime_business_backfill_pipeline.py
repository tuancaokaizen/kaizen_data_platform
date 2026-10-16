"""Manual one-shot: rebuild realtime_business_by_order cold from fnd (+ optional hot seed).
Chạy tay một lần: dựng lại realtime_business_by_order cold từ fnd (+ tuỳ chọn seed hot).

Conf / params: fromDate, toDate (excluded), hotDays, seedHot.
"""
from datetime import datetime, timedelta
import os
import sys

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.models.param import Param
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from jobs.clickhouse_utils import refresh_realtime_business_serving_table, run_clickhouse_views_setup
from jobs.spark_task import spark_job_task

# One-shot defaults differ from lakehouse D-1 window / Mặc định one-shot khác cửa sổ D-1 của lakehouse
FROM_DATE = "{{ (dag_run.conf.get('fromDate') if dag_run.conf else None) or params.fromDate }}"
TO_DATE = "{{ (dag_run.conf.get('toDate') if dag_run.conf else None) or params.toDate }}"
HOT_DAYS = "{{ (dag_run.conf.get('hotDays') if dag_run.conf else None) or params.hotDays }}"
SEED_HOT = (
    "{% if dag_run.conf and dag_run.conf.get('seedHot') is not none %}"
    "{{ 'true' if dag_run.conf.get('seedHot') else 'false' }}"
    "{% else %}{{ 'true' if params.seedHot else 'false' }}{% endif %}"
)

default_args = {
    "owner": "kaizen",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 11),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        "spark_realtime_business_backfill_pipeline",
        default_args=default_args,
        description=(
            "ONE-TIME: backfill realtime_business_by_order history from lakehouse (fnd) into Iceberg cold "
            "and seed ClickHouse hot for the last hotDays window"
        ),
        schedule_interval=None,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["kaizen", "maintenance", "clickhouse", "realtime", "realtime_business", "backfill", "manual"],
        params={
            "fromDate": Param("2026-01-01", type="string", description="Inclusive start yyyy-MM-dd"),
            "toDate": Param("2026-10-16", type="string", description="Exclusive end yyyy-MM-dd"),
            "hotDays": Param("14", type="string"),
            "seedHot": Param(True, type="boolean"),
        },
) as dag:
    backfill = KubernetesPodOperator(**spark_job_task(
        "backfill_realtime_business_cold",
        "publish.clickhouse.BackfillRealtimeBusinessCold",
        job_arguments=[HOT_DAYS, SEED_HOT],
        from_date=FROM_DATE,
        to_date=TO_DATE,
    ))
    refresh_serving = PythonOperator(
        task_id="refresh_realtime_business_serving",
        python_callable=refresh_realtime_business_serving_table,
        retries=1,
    )
    refresh_views = PythonOperator(
        task_id="refresh_realtime_business_views",
        python_callable=run_clickhouse_views_setup,
        retries=1,
    )

    backfill >> refresh_serving >> refresh_views
