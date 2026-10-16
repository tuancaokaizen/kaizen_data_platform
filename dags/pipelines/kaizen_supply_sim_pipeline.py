"""Supply-chain simulation on the live stream (dags/jobs/kaizen_sim): the daily close plans transfers, trips and
purchase orders for the day. Trip events and receipts are written by the tick worker
(k8s Deployment compute/kaizen-supply-tick), not by Airflow.
Mô phỏng chuỗi cung ứng trên luồng live (dags/jobs/kaizen_sim): chốt sổ hằng ngày lập lệnh điều chuyển, chuyến
xe và đơn mua cho ngày. Sự kiện chuyến và nhận hàng do worker tick (k8s Deployment compute/kaizen-supply-tick)
ghi, không phải Airflow.
"""
import os
import sys
from datetime import datetime, timedelta

import pendulum
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.kaizen_sim import daily_close  # type: ignore  # noqa: E402

ICT = pendulum.timezone('Asia/Ho_Chi_Minh')

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
        'kaizen_supply_daily_close',
        default_args=default_args,
        description='Close yesterday (expiry, snapshot) and plan today (transfers, trips, purchase orders)',
        schedule_interval='10 0 * * *',
        start_date=datetime(2026, 10, 12, tzinfo=ICT),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(minutes=30),
        tags=['kaizen', 'supply'],
) as close_dag:
    PythonOperator(task_id='daily_close', python_callable=lambda: daily_close.run())
