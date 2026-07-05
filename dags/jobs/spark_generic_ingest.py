from jobs.clickhouse_utils import (
    run_clickhouse_setup,
    run_clickhouse_views_setup,
    run_clickhouse_clear_serving,
    ensure_all_report_base_tables,
    refresh_biz_rt_serving_table,
)

# Shared spark_conf for Standalone Master resource settings
spark_conf = {
    "spark.master": "spark://kaizen-spark-master.compute.svc.cluster.local:7077",
    "spark.hadoop.fs.s3a.endpoint": "http://minio.storage.svc.cluster.local:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "kaizen123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
    "spark.sql.catalog.nessie.uri": "http://nessie-catalog-svc.compute.svc.cluster.local:19120/api/v1",
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
    "spark.sql.catalog.nessie.warehouse": "s3a://iceberg-data",
    "spark.sql.catalog.nessie.gc-enabled": "true",
    "spark.jars.ivy": "/tmp/.ivy2",
    "spark.executor.memory": "1g",
    "spark.executor.cores": "1",
    "spark.cores.max": "2",
    "spark.driver.memory": "1g",
    "spark.sql.session.timeZone": "UTC",
    "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
    "spark.executor.extraJavaOptions": "-Duser.timezone=UTC"
}

import yaml
import os

# Load TABLE_CONFIGS from table_configs.yaml
config_dir = os.path.dirname(os.path.abspath(__file__))
yaml_path = os.path.join(config_dir, "table_configs.yaml")

with open(yaml_path, "r", encoding="utf-8") as f:
    TABLE_CONFIGS = yaml.safe_load(f)


def get_ingestion_task(table_key):
    """
    Generates KubernetesPodOperator configuration dictionary dynamically
    for any configured table.
    """
    if table_key not in TABLE_CONFIGS:
        raise ValueError(f"Table configuration not found for key: '{table_key}'")

    config = TABLE_CONFIGS[table_key]
    task_id = f"ingest_{table_key}"
    java_class = 'com.kaizen.spark.ingest.GenericIngestJob'
    default_source = config["source"]
    default_target = table_key

    # Build the spark-submit command manually
    spark_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master 'spark://kaizen-spark-master.compute.svc.cluster.local:7077' "
        f"--class '{java_class}' "
    )

    # Add spark configurations as --conf flags
    for key, val in spark_conf.items():
        spark_cmd += f"--conf '{key}={val}' "

    # Dynamically set the driver host to the pod IP address so executors can connect back
    spark_cmd += "--conf spark.driver.host=$(hostname -i) "

    escaped_source = default_source.replace('"', '\\"')
    escaped_target = default_target.replace('"', '\\"')

    source_template = f"{{{{ (dag_run.conf.get(\"{task_id}_sourceTable\") if dag_run.conf else None) or \"{escaped_source}\" }}}}"
    target_template = f"{{{{ (dag_run.conf.get(\"{task_id}_targetTable\") if dag_run.conf else None) or \"{escaped_target}\" }}}}"
    load_type_template = f"{{{{ (dag_run.conf.get(\"{task_id}_loadType\") if dag_run.conf else None) or \"{config['load_type']}\" }}}}"
    watermark_template = f"{{{{ (dag_run.conf.get(\"{task_id}_watermarkColumn\") if dag_run.conf else None) or \"{config['watermark_col']}\" }}}}"
    join_key_template = f"{{{{ (dag_run.conf.get(\"{task_id}_joinKey\") if dag_run.conf else None) or \"{config['join_key']}\" }}}}"

    # Jinja templates to support manual date range backfills defaulting to D-1 [yesterday, today) or start of month
    range_type = config.get("range_type", "daily")
    if range_type == "start_of_month":
        from_date_tmpl = (
            "{% if dag_run.conf and dag_run.conf.get('useWatermark') %}{% else %}"
            "{{ (dag_run.conf.get('fromDate') if dag_run.conf else None) or (ds[0:7] ~ '-01') }}"
            "{% endif %}"
        )
    else:
        from_date_tmpl = (
            "{% if dag_run.conf and dag_run.conf.get('useWatermark') %}{% else %}"
            "{{ (dag_run.conf.get('fromDate') if dag_run.conf else None) or (macros.ds_add(ds, -1) if dag_run.external_trigger else (data_interval_start | ds)) }}"
            "{% endif %}"
        )
    to_date_tmpl = (
        "{% if dag_run.conf and dag_run.conf.get('useWatermark') %}{% else %}"
        "{{ (dag_run.conf.get('toDate') if dag_run.conf else None) or (ds if dag_run.external_trigger else (data_interval_end | ds)) }}"
        "{% endif %}"
    )

    spark_cmd += (
        f"'/tmp/kaizen-batch-processing.jar' "
        f"--sourceTable '{source_template}' "
        f"--targetTable '{target_template}' "
        f"--loadType '{load_type_template}' "
        f"--watermarkColumn '{watermark_template}' "
        f"--joinKey '{join_key_template}' "
        f"--fromDate '{from_date_tmpl}' "
        f"--toDate '{to_date_tmpl}'"
    )

    download_cmd = "curl -s -f -o /tmp/kaizen-batch-processing.jar http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    full_command = f"{download_cmd} && {spark_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "apache/spark:3.4.1",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
        "env_vars": {
            "SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"
        }
    }


def get_clickhouse_task(table_key):
    """
    Generates ClickHouse integration PythonOperator configuration dynamically.
    """
    if table_key not in TABLE_CONFIGS:
        raise ValueError(f"Table configuration not found for key: '{table_key}'")

    return {
        "task_id": f"create_clickhouse_{table_key}",
        "python_callable": run_clickhouse_setup,
        "op_kwargs": {"table_name": table_key},
        "retries": 0
    }


def get_clickhouse_dw_task(table_name):
    """
    Generates ClickHouse integration PythonOperator configuration dynamically for DW layer.
    """
    return {
        "task_id": f"create_clickhouse_dw_{table_name}",
        "python_callable": run_clickhouse_setup,
        "op_kwargs": {"table_name": table_name, "layer": "dw"},
        "retries": 0
    }


def get_clickhouse_serving_task(table_name):
    """
    Generates ClickHouse integration PythonOperator configuration dynamically for serving layer.
    """
    return {
        "task_id": f"create_clickhouse_serving_{table_name}",
        "python_callable": run_clickhouse_setup,
        "op_kwargs": {"table_name": table_name, "layer": "serving"},
        "retries": 0
    }


def get_clickhouse_report_task(table_name):
    """
    Generates ClickHouse integration PythonOperator configuration dynamically for report layer.
    """
    return {
        "task_id": f"create_clickhouse_report_{table_name}",
        "python_callable": run_clickhouse_setup,
        "op_kwargs": {"table_name": table_name, "layer": "report"},
        "retries": 0
    }


def get_clickhouse_views_task():
    """
    Generates ClickHouse PythonOperator to deploy hot/cold report views with Vietnamese columns.
    """
    return {
        "task_id": "create_clickhouse_report_views",
        "python_callable": run_clickhouse_views_setup,
        "retries": 0
    }


def get_clickhouse_ensure_report_task():
    """
    Ensures report.* ReplacingMergeTree tables exist before Spark push / hot view deploy.
    Tạo bảng report.* trước push Spark — tránh UNKNOWN_TABLE sau clear serving.
    """
    return {
        "task_id": "ensure_clickhouse_report_tables",
        "python_callable": ensure_all_report_base_tables,
        "retries": 0
    }


def get_clickhouse_clear_serving_task():
    """
    Generates ClickHouse PythonOperator to drop report views and hot/serving report tables.
    Tạo PythonOperator xóa view + bảng report/serving trên ClickHouse.
    """
    return {
        "task_id": "clear_clickhouse_serving",
        "python_callable": run_clickhouse_clear_serving,
        "retries": 0
    }


def get_push_serving_task():
    """
    Generates KubernetesPodOperator configuration dictionary dynamically
    for PushServingToClickHouseJob.
    """
    task_id = "push_serving_to_clickhouse"
    java_class = 'com.kaizen.spark.push.PushServingToClickHouseJob'

    # Build the spark-submit command manually
    spark_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master 'spark://kaizen-spark-master.compute.svc.cluster.local:7077' "
        f"--class '{java_class}' "
    )

    # Add spark configurations as --conf flags
    for key, val in spark_conf.items():
        spark_cmd += f"--conf '{key}={val}' "

    # Dynamically set the driver host to the pod IP address so executors can connect back
    spark_cmd += "--conf spark.driver.host=$(hostname -i) "

    spark_cmd += "'/tmp/kaizen-batch-processing.jar'"

    download_cmd = "curl -s -f -o /tmp/kaizen-batch-processing.jar http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    full_command = f"{download_cmd} && {spark_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "apache/spark:3.4.1",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
        "env_vars": {
            "SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"
        }
    }


def get_transformer_task(task_id, transformer_class):
    """
    Generates KubernetesPodOperator configuration dictionary dynamically
    for any configured Spark Transformation job.
    """
    # Build the spark-submit command manually
    spark_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master 'spark://kaizen-spark-master.compute.svc.cluster.local:7077' "
        f"--class '{transformer_class}' "
    )

    # Add spark configurations as --conf flags
    for key, val in spark_conf.items():
        spark_cmd += f"--conf '{key}={val}' "

    # Dynamically set the driver host to the pod IP address so executors can connect back
    spark_cmd += "--conf spark.driver.host=$(hostname -i) "

    # Jinja templates to support manual date range backfills defaulting to D-1 [yesterday, today)
    from_date_tmpl = (
        "{% if dag_run.conf and dag_run.conf.get('useWatermark') %}{% else %}"
        "{{ (dag_run.conf.get('fromDate') if dag_run.conf else None) or (macros.ds_add(ds, -1) if dag_run.external_trigger else (data_interval_start | ds)) }}"
        "{% endif %}"
    )
    to_date_tmpl = (
        "{% if dag_run.conf and dag_run.conf.get('useWatermark') %}{% else %}"
        "{{ (dag_run.conf.get('toDate') if dag_run.conf else None) or (ds if dag_run.external_trigger else (data_interval_end | ds)) }}"
        "{% endif %}"
    )

    spark_cmd += (
        f"'/tmp/kaizen-batch-processing.jar' "
        f"--fromDate '{from_date_tmpl}' "
        f"--toDate '{to_date_tmpl}'"
    )

    download_cmd = "curl -s -f -o /tmp/kaizen-batch-processing.jar http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    full_command = f"{download_cmd} && {spark_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "apache/spark:3.4.1",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
        "env_vars": {
            "SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"
        }
    }


def get_kafka_retention_task():
    """
    KubernetesPodOperator for KafkaRetentionJob — lightweight JRE pod (no Spark cluster).
    Pod JRE nhẹ chạy KafkaRetentionJob — không cần Spark cluster.
    """
    task_id = "kafka_retention_purge"
    java_class = "com.kaizen.spark.utility.KafkaRetentionJob"

    retention_days_tmpl = (
        "{{ (dag_run.conf.get('retentionDays') if dag_run.conf else None) or 2 }}"
    )
    topics_tmpl = (
        "{{ (dag_run.conf.get('topics') if dag_run.conf else None) or '' }}"
    )
    strategy_tmpl = (
        "{{ (dag_run.conf.get('strategy') if dag_run.conf else None) or 'delete_records' }}"
    )
    dry_run_tmpl = (
        "{{ (dag_run.conf.get('dryRun') if dag_run.conf else None) or 'false' }}"
    )
    check_lag_tmpl = (
        "{{ (dag_run.conf.get('checkConsumerLag') if dag_run.conf else None) or 'true' }}"
    )
    bootstrap_servers_tmpl = (
        "{{ (dag_run.conf.get('bootstrapServers') if dag_run.conf else None) or '' }}"
    )

    java_cmd = (
        f"java -cp /tmp/kaizen-batch-processing.jar {java_class} "
        f"--retentionDays '{retention_days_tmpl}' "
        f"--strategy '{strategy_tmpl}' "
        f"--dryRun '{dry_run_tmpl}' "
        f"--checkConsumerLag '{check_lag_tmpl}' "
    )

    java_cmd += (
        "{% if dag_run.conf and dag_run.conf.get('bootstrapServers') %}"
        f"--bootstrapServers '{bootstrap_servers_tmpl}' "
        "{% endif %}"
    )

    # Only pass --topics when Airflow conf provides it / Chỉ truyền --topics khi conf có giá trị
    java_cmd += (
        "{% if dag_run.conf and dag_run.conf.get('topics') %}"
        f"--topics '{topics_tmpl}' "
        "{% endif %}"
    )

    download_cmd = (
        "curl -s -f -o /tmp/kaizen-batch-processing.jar "
        "http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    )
    full_command = f"{download_cmd} && {java_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "eclipse-temurin:17-jre",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
    }


def get_postgres_vacuum_task():
    """
    KubernetesPodOperator for PostgresVacuumJob — VACUUM (ANALYZE) on hot PG tables.
    Pod JRE nhẹ chạy PostgresVacuumJob — không cần Spark cluster.
    """
    task_id = "postgres_vacuum"
    java_class = "com.kaizen.spark.utility.PostgresVacuumJob"

    tables_tmpl = (
        "{{ (dag_run.conf.get('tables') if dag_run.conf else None) or '' }}"
    )
    dry_run_tmpl = (
        "{{ (dag_run.conf.get('dryRun') if dag_run.conf else None) or 'false' }}"
    )

    java_cmd = (
        f"java -cp /tmp/kaizen-batch-processing.jar {java_class} "
        f"--dryRun '{dry_run_tmpl}' "
    )

    java_cmd += (
        "{% if dag_run.conf and dag_run.conf.get('tables') %}"
        f"--tables '{tables_tmpl}' "
        "{% endif %}"
    )

    download_cmd = (
        "curl -s -f -o /tmp/kaizen-batch-processing.jar "
        "http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    )
    full_command = f"{download_cmd} && {java_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "eclipse-temurin:17-jre",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
    }


def get_iceberg_partition_cleanup_task():
    """
    KubernetesPodOperator for IcebergPartitionCleanupJob — fix orphan root parquet on MinIO.
    Pod Spark chạy INSERT OVERWRITE + expire + remove_orphan sau partition migration.
    """
    task_id = "iceberg_partition_cleanup"
    java_class = "com.kaizen.spark.utility.IcebergPartitionCleanupJob"

    tables_tmpl = (
        "{{ (dag_run.conf.get('tables') if dag_run.conf else None) or '' }}"
    )
    skip_overwrite_tmpl = (
        "{{ (dag_run.conf.get('skipOverwrite') if dag_run.conf else None) or 'false' }}"
    )

    spark_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master 'spark://kaizen-spark-master.compute.svc.cluster.local:7077' "
        f"--class '{java_class}' "
    )

    for key, val in spark_conf.items():
        spark_cmd += f"--conf '{key}={val}' "

    spark_cmd += "--conf spark.driver.host=$(hostname -i) "
    spark_cmd += "'/tmp/kaizen-batch-processing.jar' "

    # Optional args — omit tables to use job DefaultTables (20 tables)
    # Tham số tuỳ chọn — bỏ --tables để job dùng danh sách mặc định 20 bảng
    spark_cmd += (
        "{% if dag_run.conf and dag_run.conf.get('tables') %}"
        f"--tables '{tables_tmpl}' "
        "{% endif %}"
        f"--skipOverwrite '{skip_overwrite_tmpl}'"
    )

    download_cmd = (
        "curl -s -f -o /tmp/kaizen-batch-processing.jar "
        "http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    )
    full_command = f"{download_cmd} && {spark_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "apache/spark:3.4.1",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
        "env_vars": {
            "SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"
        },
    }


def get_iceberg_ghost_cleanup_task():
    """
    KubernetesPodOperator for IcebergGhostFolderCleanupJob — delete ghost UUID folders on MinIO.
    Pod Spark xóa folder warehouse không còn trong Nessie catalog (sau DROP/recreate).
    """
    task_id = "iceberg_ghost_cleanup"
    java_class = "com.kaizen.spark.utility.IcebergGhostFolderCleanupJob"

    layers_tmpl = (
        "{{ (dag_run.conf.get('layers') if dag_run.conf else None) or 'stg,serving' }}"
    )
    dry_run_tmpl = (
        "{{ (dag_run.conf.get('dryRun') if dag_run.conf else None) or 'false' }}"
    )

    spark_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master 'spark://kaizen-spark-master.compute.svc.cluster.local:7077' "
        f"--class '{java_class}' "
    )

    for key, val in spark_conf.items():
        spark_cmd += f"--conf '{key}={val}' "

    spark_cmd += "--conf spark.driver.host=$(hostname -i) "
    spark_cmd += "'/tmp/kaizen-batch-processing.jar' "
    spark_cmd += f"--layers '{layers_tmpl}' --dryRun '{dry_run_tmpl}'"

    download_cmd = (
        "curl -s -f -o /tmp/kaizen-batch-processing.jar "
        "http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    )
    full_command = f"{download_cmd} && {spark_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "apache/spark:3.4.1",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
        "env_vars": {
            "SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"
        },
    }


def get_migrate_biz_rt_to_cold_task():
    """
    KubernetesPodOperator for MigrateBizRtToColdJob — hot CH → Iceberg cold.
    Pod Spark migrate report.biz_rt_order (cũ hơn hot window) sang nessie.serving.biz_rt_order.
    """
    task_id = "migrate_biz_rt_to_cold"
    java_class = "com.kaizen.spark.utility.MigrateBizRtToColdJob"

    hot_days_tmpl = (
        "{{ (dag_run.conf.get('hotDays') if dag_run.conf else None) or '14' }}"
    )

    spark_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master 'spark://kaizen-spark-master.compute.svc.cluster.local:7077' "
        f"--class '{java_class}' "
    )

    for key, val in spark_conf.items():
        spark_cmd += f"--conf '{key}={val}' "

    spark_cmd += "--conf spark.driver.host=$(hostname -i) "
    spark_cmd += "'/tmp/kaizen-batch-processing.jar' "
    spark_cmd += f"--hotDays '{hot_days_tmpl}'"

    download_cmd = (
        "curl -s -f -o /tmp/kaizen-batch-processing.jar "
        "http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    )
    full_command = f"{download_cmd} && {spark_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "apache/spark:3.4.1",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
        "env_vars": {
            "SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"
        },
    }


def get_refresh_biz_rt_serving_task():
    """Refresh ClickHouse serving.biz_rt_order Iceberg pointer after migrate."""
    return {
        "task_id": "refresh_biz_rt_serving",
        "python_callable": refresh_biz_rt_serving_table,
        "retries": 1,
    }


def get_backfill_biz_rt_cold_task():
    """
    One-time BackfillBizRtColdJob — lakehouse fnd → cold (+ optional hot seed).
    Job một lần: lịch sử từ 2026-01-01 (hoặc fromDate) vào cold / seed hot.
    """
    task_id = "backfill_biz_rt_cold"
    java_class = "com.kaizen.spark.utility.BackfillBizRtColdJob"

    from_date_tmpl = (
        "{{ (dag_run.conf.get('fromDate') if dag_run.conf else None) or '2026-01-01' }}"
    )
    to_date_tmpl = (
        "{{ (dag_run.conf.get('toDate') if dag_run.conf else None) or '' }}"
    )
    hot_days_tmpl = (
        "{{ (dag_run.conf.get('hotDays') if dag_run.conf else None) or '14' }}"
    )
    # Boolean false must not fall through to default 'true' / false boolean không được fallback 'true'
    seed_hot_tmpl = (
        "{% if dag_run.conf and dag_run.conf.get('seedHot') is not none %}"
        "{{ 'true' if dag_run.conf.get('seedHot') else 'false' }}"
        "{% else %}true{% endif %}"
    )

    spark_cmd = (
        f"/opt/spark/bin/spark-submit "
        f"--master 'spark://kaizen-spark-master.compute.svc.cluster.local:7077' "
        f"--class '{java_class}' "
    )

    for key, val in spark_conf.items():
        spark_cmd += f"--conf '{key}={val}' "

    spark_cmd += "--conf spark.driver.host=$(hostname -i) "
    spark_cmd += "'/tmp/kaizen-batch-processing.jar' "
    spark_cmd += f"--fromDate '{from_date_tmpl}' --hotDays '{hot_days_tmpl}' --seedHot '{seed_hot_tmpl}' "
    spark_cmd += (
        "{% if dag_run.conf and dag_run.conf.get('toDate') %}"
        f"--toDate '{to_date_tmpl}' "
        "{% endif %}"
    )

    download_cmd = (
        "curl -s -f -o /tmp/kaizen-batch-processing.jar "
        "http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    )
    full_command = f"{download_cmd} && {spark_cmd}"

    return {
        "task_id": task_id,
        "name": task_id.replace("_", "-"),
        "namespace": "orchestrator",
        "image": "apache/spark:3.4.1",
        "cmds": ["/bin/sh", "-c"],
        "arguments": [full_command],
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
        "env_vars": {
            "SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"
        },
    }
