from datetime import datetime
import sys
import os

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# pyrefly: ignore [missing-import]
from airflow.models.param import Param

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
}

with DAG(
    'sil_daily_pipeline',
    default_args=default_args,
    description='Daily Pipeline running sequentially at 12:30 PM (Ingest -> Transform)',
    schedule_interval='30 12 * * *',  # Runs daily at 12:30 PM
    catchup=False,
    params={
        "fromDate": Param(None, type=["null", "string"], description="Start date (YYYY-MM-DD) for ingestion & transform. Defaults to D-1 if blank."),
        "toDate": Param(None, type=["null", "string"], description="End date (YYYY-MM-DD) for ingestion & transform. Defaults to D if blank."),
        "useWatermark": Param(False, type="boolean", description="Whether to ignore date range and use target table watermark.")
    }
) as dag:

    # 1. Trigger Master Data Ingestion DAG and wait for it to complete successfully
    trigger_master_ingest = TriggerDagRunOperator(
        task_id='trigger_master_ingest',
        trigger_dag_id='spark_master_data_ingest_pipeline',
        conf="{{ dag_run.conf }}",
        wait_for_completion=True,
        poke_interval=30
    )

    # 2. Trigger Incremental Transaction Ingestion DAG and wait for it to complete successfully
    trigger_transaction_ingest = TriggerDagRunOperator(
        task_id='trigger_transaction_ingest',
        trigger_dag_id='spark_transaction_data_ingest_pipeline',
        conf="{{ dag_run.conf }}",
        wait_for_completion=True,
        poke_interval=30
    )

    # 3. Trigger DW Transformation DAG and wait for it to complete successfully
    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='spark_dw_transform_pipeline',
        conf="{{ dag_run.conf }}",
        wait_for_completion=True,
        poke_interval=30
    )

    # Sequence: Master Ingest >> Transaction Ingest >> DW Transform
    trigger_master_ingest >> trigger_transaction_ingest >> trigger_transform
