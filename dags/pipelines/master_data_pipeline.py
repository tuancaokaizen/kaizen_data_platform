from datetime import datetime
import sys
import os

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

# Allow importing from jobs folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.generate_master_data import generate_all_master_data  # type: ignore

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
}

with DAG(
        'master_data_generation_pipeline',
        default_args=default_args,
        description='Pipeline to generate master data once and sync to Postgres/ES',
        schedule_interval=None,  # Manual trigger
        catchup=False,
) as dag:
    generate_master_data_task = PythonOperator(
        task_id='generate_master_data',
        python_callable=generate_all_master_data
    )
