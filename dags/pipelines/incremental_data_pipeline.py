from datetime import datetime
import sys
import os

from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

# Allow importing from jobs folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.generate_incremental_data import generate_incremental_data  # type: ignore

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
}

with DAG(
    'incremental_data_generation_pipeline',
    default_args=default_args,
    description='Pipeline to periodically generate incremental Employee Salary and Promotion data',
    schedule_interval='@monthly',  # Runs monthly on the 1st of the month
    catchup=False,
) as dag:

    generate_incremental_data_task = PythonOperator(
        task_id='generate_incremental_data',
        python_callable=generate_incremental_data
    )
