from datetime import datetime, timedelta
import sys
import os

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Allow importing configs from jobs folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.spark_generic_ingest import get_transformer_task

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
        'spark_dw_maintenance_pipeline',
        default_args=default_args,
        description='Daily Scheduled DAG to perform Iceberg maintenance (compaction, expire snapshots, orphan files cleanup)',
        schedule_interval='0 2 * * *',  # Run daily at 2:00 AM
        catchup=False
) as dag:
    run_maintenance = KubernetesPodOperator(
        **get_transformer_task(
            task_id="run_maintenance",
            transformer_class="com.kaizen.spark.utility.IcebergMaintenanceJob"
        )
    )
