"""Kaizen lakehouse daily run: Postgres -> stg -> fnd -> dw -> serving -> ClickHouse report.
Chạy lakehouse Kaizen hằng ngày: Postgres -> stg -> fnd -> dw -> serving -> ClickHouse report.

Backfill: trigger with conf {"fromDate": "2026-10-01", "toDate": "2026-10-15"} (toDate excluded).
Chạy bù: trigger với conf {"fromDate": "2026-10-01", "toDate": "2026-10-15"} (không gồm toDate).

Resource-limited lab: one table (or one dim/fact/report) per Spark pod, all chained sequentially
so only one pod hits the standalone cluster at a time (max_active_tasks=1).
Lab tài nguyên hạn chế: mỗi bảng (hoặc dim/fact/report) một pod Spark, nối tuần tự
để cùng lúc chỉ một pod chiếm cluster standalone (max_active_tasks=1).
"""
from datetime import datetime, timedelta
import os
import sys

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.models.param import Param
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
# pyrefly: ignore [missing-import]
from airflow.utils.task_group import TaskGroup

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from jobs.clickhouse_utils import run_clickhouse_setup, run_clickhouse_views_setup
from jobs.spark_task import spark_job_task

# (group, table) from tables.yaml — one PostgresToBronze / BronzeToSilver pod per row
# (nhóm, bảng) từ tables.yaml — mỗi dòng một pod PostgresToBronze / BronzeToSilver
SOURCE_TABLES = [
    # master
    ("master", "geography"),
    ("master", "customer"),
    ("master", "shop"),
    ("master", "shop_capacity"),
    ("master", "employee"),
    ("master", "product"),
    ("master", "product_medicine"),
    ("master", "product_supplement"),
    ("master", "inventory_lot"),
    ("master", "warehouse"),
    ("master", "supplier"),
    ("master", "carrier"),
    ("master", "vehicle"),
    ("master", "payment_method"),
    ("master", "payment_type"),
    ("master", "promotion"),
    ("master", "gift"),
    ("master", "voucher"),
    ("master", "voucher_series"),
    # sales
    ("sales", "order"),
    ("sales", "order_item"),
    ("sales", "return"),
    ("sales", "return_item"),
    ("sales", "lost_sale"),
    # payment
    ("payment", "payment"),
    ("payment", "transaction"),
    # inventory
    ("inventory", "stock_movement"),
    ("inventory", "inventory_snapshot"),
    # supply
    ("supply", "transfer_order"),
    ("supply", "transfer_order_item"),
    ("supply", "purchase_order"),
    ("supply", "purchase_order_item"),
    ("supply", "shipment"),
    ("supply", "shipment_stop"),
    ("supply", "shipment_item"),
    ("supply", "shipment_event"),
    # hr
    ("hr", "employee_salary"),
]

DIMENSIONS = [
    "DimDate",
    "DimGeography",
    "DimShop",
    "DimLocation",
    "DimWarehouse",
    "DimSupplier",
    "DimCarrier",
    "DimVehicle",
    "DimProduct",
    "DimCustomer",
    "DimEmployee",
    "DimPromotion",
    "DimVoucher",
    "DimPaymentMethod",
]

# Facts read from fnd only / Fact chỉ đọc từ fnd
FACTS = [
    "FactSaleTransaction",
    "FactReturnTransaction",
    "FactPaymentTransaction",
    "FactOrderTransaction",
    "FactPromotionTransaction",
    "FactVoucherSeries",
    "FactLostSale",
    "FactStockMovement",
    "FactInventorySnapshot",
    "FactPurchaseOrder",
    "FactTransferOrder",
    "FactShipment",
    "FactShipmentStop",
]

# After base facts (sequential order above already covers their inputs)
# Sau fact gốc (thứ tự tuần tự phía trên đã đủ input)
DERIVED_FACTS = [
    "FactCustomerMonthly",
    "FactEmployeeSalary",
]

# Serving reports in dependency-safe order / Báo cáo serving theo thứ tự phụ thuộc an toàn
REPORTS = [
    ("financial_report_detail", "processing.report.financial.FinancialReportDetail"),
    ("financial_report", "processing.report.financial.FinancialReport"),
    ("financial_report_product", "processing.report.financial.FinancialReportProduct"),
    ("financial_report_order", "processing.report.financial.FinancialReportOrder"),
    ("fund_report_order", "processing.report.fund.FundReportOrder"),
    ("fund_report", "processing.report.fund.FundReport"),
    ("fund_report_shop", "processing.report.fund.FundReportShop"),
    ("inventory_health", "processing.report.supply.InventoryHealth"),
    ("shop_capacity_utilization", "processing.report.supply.ShopCapacityUtilization"),
    ("carrier_scorecard", "processing.report.supply.CarrierScorecard"),
    ("supplier_performance", "processing.report.supply.SupplierPerformance"),
]

SERVING_REPORTS = [name for name, _ in REPORTS]


def snake_case(class_name):
    """DimShop -> dim_shop / Đổi tên class sang snake_case cho task_id."""
    return "".join(f"_{c.lower()}" if c.isupper() else c for c in class_name).lstrip("_")


def chain_sequential(tasks):
    """Wire tasks[0] >> tasks[1] >> … / Nối tasks[0] >> tasks[1] >> …"""
    for upstream, downstream in zip(tasks, tasks[1:]):
        upstream >> downstream
    return tasks[-1] if tasks else None


def register_serving_tables():
    """Point ClickHouse serving.* at the current Iceberg folder of each report.
    Trỏ serving.* của ClickHouse tới thư mục Iceberg hiện tại của từng báo cáo."""
    for report in SERVING_REPORTS:
        run_clickhouse_setup(report, layer="serving")


def spark_task(task_id, job_class, job_arguments=()):
    """One pod running one batch job / Một pod chạy một job batch."""
    return KubernetesPodOperator(**spark_job_task(task_id, job_class, job_arguments))


default_args = {
    "owner": "kaizen",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        "kaizen_lakehouse_daily",
        default_args=default_args,
        description="Kaizen lakehouse: Postgres -> Iceberg stg/fnd/dw/serving -> ClickHouse report (sequential)",
        schedule_interval="30 0 * * *",  # 00:30 UTC = 07:30 VN
        catchup=False,
        max_active_runs=1,
        # One Spark pod at a time on the small standalone cluster / Mỗi lúc một pod Spark trên cluster nhỏ
        max_active_tasks=1,
        is_paused_upon_creation=True,
        tags=["kaizen", "lakehouse"],
        params={
            "fromDate": Param(None, type=["null", "string"],
                              description="yyyy-MM-dd, default D-1 / mặc định D-1"),
            "toDate": Param(None, type=["null", "string"],
                            description="yyyy-MM-dd, excluded, default D / không bao gồm, mặc định D"),
        },
) as dag:
    # Ingest: bronze then silver per table, tables one after another
    # Ingest: bronze rồi silver từng bảng, các bảng nối tiếp nhau
    with TaskGroup("source") as source:
        source_tasks = []
        for group, table in SOURCE_TABLES:
            bronze = spark_task(
                f"postgres_to_bronze_{table}",
                "ingest.postgres.PostgresToBronze",
                [group, table],
            )
            silver = spark_task(
                f"bronze_to_silver_{table}",
                "processing.silver.BronzeToSilver",
                [group, table],
            )
            bronze >> silver
            source_tasks.extend([bronze, silver])
        chain_sequential(source_tasks)

    with TaskGroup("dimension") as dimension:
        chain_sequential([
            spark_task(snake_case(name), f"processing.dim.{name}") for name in DIMENSIONS
        ])

    with TaskGroup("fact") as fact:
        chain_sequential([
            spark_task(snake_case(name), f"processing.fact.{name}")
            for name in FACTS + DERIVED_FACTS
        ])

    with TaskGroup("report") as report:
        chain_sequential([
            spark_task(task_id, job_class) for task_id, job_class in REPORTS
        ])

    with TaskGroup("clickhouse") as clickhouse:
        register = PythonOperator(task_id="register_serving_tables", python_callable=register_serving_tables)
        views = PythonOperator(task_id="report_views", python_callable=run_clickhouse_views_setup)
        publish = spark_task("publish_report", "publish.clickhouse.PublishReportToClickHouse")
        register >> views >> publish

    source >> dimension >> fact >> report >> clickhouse
