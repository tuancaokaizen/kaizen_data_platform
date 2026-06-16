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
    'retries': 0,
}

with DAG(
    'spark_dw_partition_realignment_pipeline',
    default_args=default_args,
    description='One-shot DAG to realign partitions and rewrite historical data in Nessie fact tables',
    schedule_interval=None,
    catchup=False
) as dag:

    realign_partitions = KubernetesPodOperator(
        **get_transformer_task(
            task_id="realign_partitions",
            transformer_class="com.kaizen.spark.utility.PartitionRealignmentJob"
        )
    )
