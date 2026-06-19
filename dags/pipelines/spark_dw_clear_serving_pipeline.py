from datetime import datetime, timedelta
import sys
import os

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator

# Allow importing configs from jobs folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.spark_generic_ingest import get_transformer_task, get_clickhouse_clear_serving_task

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'spark_dw_clear_serving_pipeline',
        default_args=default_args,
        description=(
            'Manually triggered DAG to reset report serving layer only: '
            'Nessie stg/serving report tables + ClickHouse views/hot/serving pointers. '
            'DW/FND layers are kept intact.'
        ),
        schedule_interval=None,  # Manual trigger only — không schedule tự động
        catchup=False,
        max_active_runs=1,  # Prevent concurrent serving resets / Tránh clear serving song song
        tags=['maintenance', 'serving', 'reports', 'manual'],
) as dag:
    clear_nessie_serving = KubernetesPodOperator(
        **get_transformer_task(
            task_id="clear_nessie_serving",
            transformer_class="com.kaizen.spark.utility.ClearNessieServingJob",
        )
    )

    clear_clickhouse_serving = PythonOperator(**get_clickhouse_clear_serving_task())

    clear_nessie_serving >> clear_clickhouse_serving
