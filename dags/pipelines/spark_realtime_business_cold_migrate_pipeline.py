from datetime import datetime
import sys
import os

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.spark_generic_ingest import (
    get_migrate_biz_rt_to_cold_task,
    get_refresh_biz_rt_serving_task,
)
from jobs.clickhouse_utils import run_clickhouse_views_setup

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'retries': 0,
}

with DAG(
        'spark_biz_rt_cold_migrate_pipeline',
        default_args=default_args,
        description=(
            'Migrate realtime biz_rt_order from ClickHouse hot to Iceberg cold, '
            'then refresh serving.* Iceberg pointer and report views.'
        ),
        # Daily after midnight — keep cold history, hot TTL handles purge
        # Hàng ngày sau nửa đêm — giữ lịch sử cold, TTL hot tự xóa
        schedule_interval='30 1 * * *',
        catchup=False,
        max_active_runs=1,
        tags=['maintenance', 'clickhouse', 'realtime', 'biz_rt'],
) as dag:
    migrate = KubernetesPodOperator(**get_migrate_biz_rt_to_cold_task())
    refresh_serving = PythonOperator(**get_refresh_biz_rt_serving_task())
    refresh_views = PythonOperator(
        task_id='refresh_biz_rt_views',
        python_callable=run_clickhouse_views_setup,
        retries=1,
    )

    migrate >> refresh_serving >> refresh_views
