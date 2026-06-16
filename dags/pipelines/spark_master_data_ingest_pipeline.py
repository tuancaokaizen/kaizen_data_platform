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
from jobs.spark_generic_ingest import get_ingestion_task, get_clickhouse_task

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_master_data_ingest_pipeline',
    default_args=default_args,
    description='Spark Batch PostgreSQL Master Data Ingestion to Iceberg Nessie Catalog (Full Load)',
    schedule_interval=None,  # Manually triggered or triggered from generation pipeline
    catchup=False,
) as dag:

    # 1. Geography Ingestion and mapping
    ingest_geography = KubernetesPodOperator(**get_ingestion_task("geography"))
    ch_geography = PythonOperator(**get_clickhouse_task("geography"))

    # 2. Product Ingestion and mapping
    ingest_product = KubernetesPodOperator(**get_ingestion_task("product"))
    ch_product = PythonOperator(**get_clickhouse_task("product"))

    # 3. ProductMedicine Ingestion and mapping
    ingest_product_medicine = KubernetesPodOperator(**get_ingestion_task("product_medicine"))
    ch_product_medicine = PythonOperator(**get_clickhouse_task("product_medicine"))

    # 4. ProductSupplement Ingestion and mapping
    ingest_product_supplement = KubernetesPodOperator(**get_ingestion_task("product_supplement"))
    ch_product_supplement = PythonOperator(**get_clickhouse_task("product_supplement"))

    # 5. Shop Ingestion and mapping
    ingest_shop = KubernetesPodOperator(**get_ingestion_task("shop"))
    ch_shop = PythonOperator(**get_clickhouse_task("shop"))

    # 6. Employee Ingestion and mapping
    ingest_employee = KubernetesPodOperator(**get_ingestion_task("employee"))
    ch_employee = PythonOperator(**get_clickhouse_task("employee"))

    # 7. Customer Ingestion and mapping
    ingest_customer = KubernetesPodOperator(**get_ingestion_task("customer"))
    ch_customer = PythonOperator(**get_clickhouse_task("customer"))

    # 8. PaymentType Ingestion and mapping
    ingest_payment_type = KubernetesPodOperator(**get_ingestion_task("payment_type"))
    ch_payment_type = PythonOperator(**get_clickhouse_task("payment_type"))

    # 9. PaymentMethod Ingestion and mapping
    ingest_payment_method = KubernetesPodOperator(**get_ingestion_task("payment_method"))
    ch_payment_method = PythonOperator(**get_clickhouse_task("payment_method"))

    # Run tasks sequentially to fit within cluster resources
    (
        ingest_geography >> ch_geography >> 
        ingest_product >> ch_product >> 
        ingest_product_medicine >> ch_product_medicine >> 
        ingest_product_supplement >> ch_product_supplement >> 
        ingest_shop >> ch_shop >> 
        ingest_employee >> ch_employee >> 
        ingest_customer >> ch_customer >> 
        ingest_payment_type >> ch_payment_type >> 
        ingest_payment_method >> ch_payment_method
    )
