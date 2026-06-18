from datetime import datetime, timedelta
import sys
import os

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from airflow.models.param import Param

# Allow importing configs from jobs folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.spark_generic_ingest import get_ingestion_task, get_clickhouse_task

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'spark_transaction_data_ingest_pipeline',
        default_args=default_args,
        description='Spark Batch PostgreSQL Transaction & Periodical Data Ingestion to Iceberg (Incremental Load)',
        schedule_interval=None,  # Triggered manually or by the generation pipeline
        catchup=False,
        params={
            "fromDate": Param(None, type=["null", "string"],
                              description="Start date (YYYY-MM-DD) for range ingestion. Defaults to D-1 if left blank."),
            "toDate": Param(None, type=["null", "string"],
                            description="End date (YYYY-MM-DD) for range ingestion. Defaults to today (D) if left blank."),
            "useWatermark": Param(False, type="boolean",
                                  description="Whether to ignore date range and use target table watermark.")
        }
) as dag:
    # 1. Order Ingestion
    ingest_order = KubernetesPodOperator(**get_ingestion_task("order"))
    ch_order = PythonOperator(**get_clickhouse_task("order"))

    # 2. OrderItem Ingestion
    ingest_order_item = KubernetesPodOperator(**get_ingestion_task("order_item"))
    ch_order_item = PythonOperator(**get_clickhouse_task("order_item"))

    # 3. Return Ingestion
    ingest_return = KubernetesPodOperator(**get_ingestion_task("return"))
    ch_return = PythonOperator(**get_clickhouse_task("return"))

    # 4. ReturnItem Ingestion
    ingest_return_item = KubernetesPodOperator(**get_ingestion_task("return_item"))
    ch_return_item = PythonOperator(**get_clickhouse_task("return_item"))

    # 5. Payment Ingestion
    ingest_payment = KubernetesPodOperator(**get_ingestion_task("payment"))
    ch_payment = PythonOperator(**get_clickhouse_task("payment"))

    # 6. Transaction Ingestion
    ingest_transaction = KubernetesPodOperator(**get_ingestion_task("transaction"))
    ch_transaction = PythonOperator(**get_clickhouse_task("transaction"))

    # 7. OrderPayment Ingestion
    ingest_order_payment = KubernetesPodOperator(**get_ingestion_task("order_payment"))
    ch_order_payment = PythonOperator(**get_clickhouse_task("order_payment"))

    # 8. ReturnPayment Ingestion
    ingest_return_payment = KubernetesPodOperator(**get_ingestion_task("return_payment"))
    ch_return_payment = PythonOperator(**get_clickhouse_task("return_payment"))

    # 9. EmployeeSalary Ingestion
    ingest_employee_salary = KubernetesPodOperator(**get_ingestion_task("employee_salary"))
    ch_employee_salary = PythonOperator(**get_clickhouse_task("employee_salary"))

    # 10. Promotion Ingestion
    ingest_promotion = KubernetesPodOperator(**get_ingestion_task("promotion"))
    ch_promotion = PythonOperator(**get_clickhouse_task("promotion"))

    # 11. Voucher Ingestion
    ingest_voucher = KubernetesPodOperator(**get_ingestion_task("voucher"))
    ch_voucher = PythonOperator(**get_clickhouse_task("voucher"))

    # 12. VoucherSeries Ingestion
    ingest_voucher_series = KubernetesPodOperator(**get_ingestion_task("voucher_series"))
    ch_voucher_series = PythonOperator(**get_clickhouse_task("voucher_series"))

    # 13. Gift Ingestion
    ingest_gift = KubernetesPodOperator(**get_ingestion_task("gift"))
    ch_gift = PythonOperator(**get_clickhouse_task("gift"))

    # Run tasks sequentially to fit within cluster resources
    (
            ingest_order >> ch_order >>
            ingest_order_item >> ch_order_item >>
            ingest_return >> ch_return >>
            ingest_return_item >> ch_return_item >>
            ingest_payment >> ch_payment >>
            ingest_transaction >> ch_transaction >>
            ingest_order_payment >> ch_order_payment >>
            ingest_return_payment >> ch_return_payment >>
            ingest_employee_salary >> ch_employee_salary >>
            ingest_promotion >> ch_promotion >>
            ingest_voucher >> ch_voucher >>
            ingest_voucher_series >> ch_voucher_series >>
            ingest_gift >> ch_gift
    )
