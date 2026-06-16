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
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_dw_clear_nessie_pipeline',
    default_args=default_args,
    description='Manually triggered DAG to drop all Nessie tables (STG, FND, DW) using ClearNessieTablesJob',
    schedule_interval=None,  # Triggered manually only
    catchup=False
) as dag:

    clear_nessie_tables = KubernetesPodOperator(
        **get_transformer_task(
            task_id="clear_nessie_tables",
            transformer_class="com.kaizen.spark.utility.ClearNessieTablesJob"
        )
    )
