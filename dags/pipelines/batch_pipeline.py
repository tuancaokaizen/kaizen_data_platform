from datetime import datetime, timedelta
import sys
import os

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Allow importing configs from jobs folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.spark_cleansing import get_pod_operator_config as cleansing_config
from jobs.spark_analytics import get_pod_operator_config as analytics_config

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'batch_spark_pipeline',
    default_args=default_args,
    description='Spark Batch Processing Cleansing and Aggregator Pipeline',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    cleansing_task = KubernetesPodOperator(**cleansing_config())
    analytics_task = KubernetesPodOperator(**analytics_config())

    cleansing_task >> analytics_task
