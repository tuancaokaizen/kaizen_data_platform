from jobs.clickhouse_utils import run_clickhouse_setup

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
    "spark.jars.ivy": "/tmp/.ivy2",
    "spark.executor.memory": "1g",
    "spark.executor.cores": "1",
    "spark.cores.max": "2",
    "spark.driver.memory": "1g",
    "spark.sql.session.timeZone": "Asia/Ho_Chi_Minh",
    "spark.driver.extraJavaOptions": "-Duser.timezone=Asia/Ho_Chi_Minh",
    "spark.executor.extraJavaOptions": "-Duser.timezone=Asia/Ho_Chi_Minh"
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
