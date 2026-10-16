"""Daily: migrate hot ClickHouse realtime_business_by_order older than hotDays into Iceberg cold.
Hàng ngày: chuyển hot ClickHouse realtime_business_by_order cũ hơn hotDays sang Iceberg cold.

Conf: {"hotDays": "14"} (optional). fromDate/toDate are unused by the job but kept positional.
Conf: {"hotDays": "14"} (tuỳ chọn). fromDate/toDate job không dùng nhưng vẫn truyền theo vị trí.
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

# hotDays from conf or DAG param, default 14 / hotDays từ conf hoặc param DAG, mặc định 14
HOT_DAYS = "{{ (dag_run.conf.get('hotDays') if dag_run.conf else None) or params.hotDays }}"

default_args = {
    "owner": "kaizen",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 11),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        "spark_realtime_business_cold_migrate_pipeline",
        default_args=default_args,
        description=(
            "Migrate realtime_business_by_order from ClickHouse hot to Iceberg cold, "
            "then refresh serving.* Iceberg pointer and report views"
        ),
        schedule_interval="30 1 * * *",  # 01:30 UTC after lakehouse daily / sau lakehouse daily
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["kaizen", "maintenance", "clickhouse", "realtime", "realtime_business"],
        params={
            "hotDays": Param("14", type="string", description="Hot window days kept in ClickHouse"),
        },
) as dag:
    migrate = KubernetesPodOperator(**spark_job_task(
        "migrate_realtime_business_to_cold",
        "publish.clickhouse.MigrateRealtimeBusinessToCold",
        job_arguments=[HOT_DAYS],
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

    migrate >> refresh_serving >> refresh_views
