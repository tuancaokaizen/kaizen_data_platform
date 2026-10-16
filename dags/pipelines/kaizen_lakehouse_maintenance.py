"""Kaizen lakehouse housekeeping: Iceberg snapshots, small files, orphan files; Postgres vacuum.
Dọn dẹp lakehouse Kaizen: snapshot Iceberg, file nhỏ, file mồ côi; vacuum Postgres.
"""
from datetime import datetime, timedelta
import os
import sys

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from jobs.spark_task import spark_job_task

default_args = {
    "owner": "kaizen",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
        "kaizen_lakehouse_maintenance",
        default_args=default_args,
        description="Kaizen Iceberg: expire snapshots -> compact -> remove orphan files",
        schedule_interval="0 2 * * *",  # 02:00 UTC = 09:00 VN, after kaizen_lakehouse_daily
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["kaizen", "lakehouse", "maintenance"],
) as lakehouse_dag:
    # Expire first so compaction and orphan removal see fewer files / Expire trước để compact và xoá mồ côi ít file hơn
    expire = KubernetesPodOperator(**spark_job_task(
        "iceberg_expire_snapshots", "maintenance.iceberg.IcebergExpireSnapshots"))
    compact = KubernetesPodOperator(**spark_job_task(
        "iceberg_compact_tables", "maintenance.iceberg.IcebergCompactTables"))
    orphans = KubernetesPodOperator(**spark_job_task(
        "iceberg_remove_orphan_files", "maintenance.iceberg.IcebergRemoveOrphanFiles"))
    expire >> compact >> orphans

with DAG(
        "kaizen_postgres_vacuum",
        default_args=default_args,
        description="Kaizen Postgres information_system: VACUUM (ANALYZE) of the streaming tables",
        schedule_interval="0 4 * * 0",  # Sunday 04:00 UTC = 11:00 VN
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["kaizen", "postgres", "maintenance"],
) as vacuum_dag:
    # Only a JDBC connection is needed, so no Spark cluster cores / Chỉ cần kết nối JDBC, không chiếm core Spark cluster
    KubernetesPodOperator(**spark_job_task(
        "postgres_vacuum", "maintenance.postgres.PostgresVacuum",
        spark_conf={"spark.driver.memory": "512m"}, master="local[1]"))
