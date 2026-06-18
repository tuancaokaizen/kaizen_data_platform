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
from jobs.spark_generic_ingest import (
    get_transformer_task,
    get_clickhouse_dw_task,
    get_clickhouse_serving_task,
    get_clickhouse_report_task,
    get_push_serving_task
)

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'spark_dw_transform_pipeline',
        default_args=default_args,
        description='Spark Batch DW Dimensional Transformations (Sales & Returns Facts)',
        schedule_interval=None,  # Triggered manually or by upstream ingestion pipeline
        catchup=False,
        params={
            "fromDate": Param(None, type=["null", "string"],
                              description="Start date (YYYY-MM-DD) for fact partitioning. Defaults to D-1 if left blank."),
            "toDate": Param(None, type=["null", "string"],
                            description="End date (YYYY-MM-DD) for fact partitioning. Defaults to today (D) if left blank."),
            "useWatermark": Param(False, type="boolean",
                                  description="Whether to ignore date range and use target table watermark.")
        }
) as dag:
    # 1. Transform Product Master Data into DW Product Dimension
    transform_dim_product = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_dim_product",
            transformer_class="com.kaizen.spark.processing.ProcessDimProduct"
        )
    )
    ch_dim_product = PythonOperator(**get_clickhouse_dw_task("dim_product"))

    # 2. Transform Customer Master Data into DW Customer Dimension
    transform_dim_customer = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_dim_customer",
            transformer_class="com.kaizen.spark.processing.ProcessDimCustomer"
        )
    )
    ch_dim_customer = PythonOperator(**get_clickhouse_dw_task("dim_customer"))

    # 3. Transform Shop Master Data into DW Shop Dimension
    transform_dim_shop = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_dim_shop",
            transformer_class="com.kaizen.spark.processing.ProcessDimShop"
        )
    )
    ch_dim_shop = PythonOperator(**get_clickhouse_dw_task("dim_shop"))

    # 4. Transform Employee Master Data into DW Employee Dimension
    transform_dim_employee = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_dim_employee",
            transformer_class="com.kaizen.spark.processing.ProcessDimEmployee"
        )
    )
    ch_dim_employee = PythonOperator(**get_clickhouse_dw_task("dim_employee"))

    # 4a. Transform Promotion Master Data into DW Promotion Dimension
    transform_dim_promotion = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_dim_promotion",
            transformer_class="com.kaizen.spark.processing.ProcessDimPromotion"
        )
    )
    ch_dim_promotion = PythonOperator(**get_clickhouse_dw_task("dim_promotion"))

    # 4b. Transform Voucher Master Data into DW Voucher Dimension
    transform_dim_voucher = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_dim_voucher",
            transformer_class="com.kaizen.spark.processing.ProcessDimVoucher"
        )
    )
    ch_dim_voucher = PythonOperator(**get_clickhouse_dw_task("dim_voucher"))

    # 5. Transform Sales Transactions into DW Fact Table
    transform_sales_transactions = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_sales_transactions",
            transformer_class="com.kaizen.spark.processing.ProcessFactSalesTransaction"
        )
    )
    ch_fact_sales_transaction = PythonOperator(**get_clickhouse_dw_task("fact_sales_transaction"))

    # 6. Transform Return Transactions into DW Fact Table
    transform_return_transactions = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_return_transactions",
            transformer_class="com.kaizen.spark.processing.ProcessFactReturnTransaction"
        )
    )
    ch_fact_return_transaction = PythonOperator(**get_clickhouse_dw_task("fact_return_transaction"))

    # 7. Transform Payment Transactions into DW Fact Table
    transform_payment_transactions = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_payment_transactions",
            transformer_class="com.kaizen.spark.processing.ProcessFactPaymentTransaction"
        )
    )
    ch_fact_payment_transaction = PythonOperator(**get_clickhouse_dw_task("fact_payment_transaction"))

    # 8. Transform Promotion Costs into DW Fact Table
    transform_promotion_cost = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_promotion_cost",
            transformer_class="com.kaizen.spark.processing.ProcessFactPromotionCost"
        )
    )
    ch_fact_promotion_cost = PythonOperator(**get_clickhouse_dw_task("fact_promotion_cost"))

    # 9. Transform COGS Transactions into DW Fact Table
    transform_cogs = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_cogs",
            transformer_class="com.kaizen.spark.processing.ProcessFactCogs"
        )
    )
    ch_fact_cogs_transaction = PythonOperator(**get_clickhouse_dw_task("fact_cogs_transaction"))

    # 10. Transform Financial Report in Serving Layer
    transform_serving_financial_report = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_serving_financial_report",
            transformer_class="com.kaizen.spark.processing.ProcessSrvFinancialReport"
        )
    )
    ch_financial_serving = PythonOperator(**get_clickhouse_serving_task("financial_report"))
    ch_financial_report = PythonOperator(**get_clickhouse_report_task("financial_report"))

    # 11. Transform Fund Report in Serving Layer
    transform_serving_fund_report = KubernetesPodOperator(
        **get_transformer_task(
            task_id="transform_serving_fund_report",
            transformer_class="com.kaizen.spark.processing.ProcessSrvFundReport"
        )
    )
    ch_fund_serving = PythonOperator(**get_clickhouse_serving_task("fund_report"))
    ch_fund_report = PythonOperator(**get_clickhouse_report_task("fund_report"))

    # 12. Push Serving Reports (1-month) to ClickHouse Hot Layer
    push_serving_to_clickhouse = KubernetesPodOperator(**get_push_serving_task())

    # Execute Dimensions sequentially, then Facts sequentially, and finally Serving Reports sequentially
    (
            transform_dim_product >> ch_dim_product >>
            transform_dim_customer >> ch_dim_customer >>
            transform_dim_shop >> ch_dim_shop >>
            transform_dim_employee >> ch_dim_employee >>
            transform_dim_promotion >> ch_dim_promotion >>
            transform_dim_voucher >> ch_dim_voucher >>
            transform_sales_transactions >> ch_fact_sales_transaction >>
            transform_return_transactions >> ch_fact_return_transaction >>
            transform_payment_transactions >> ch_fact_payment_transaction >>
            transform_promotion_cost >> ch_fact_promotion_cost >>
            transform_cogs >> ch_fact_cogs_transaction >>
            transform_serving_financial_report >> ch_financial_serving >> ch_financial_report >>
            transform_serving_fund_report >> ch_fund_serving >> ch_fund_report >>
            push_serving_to_clickhouse
    )
