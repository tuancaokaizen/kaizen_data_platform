from datetime import datetime, timedelta
import sys
import os

# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.models.param import Param
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

# Allow importing configs from jobs folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from jobs.spark_generic_ingest import spark_conf

default_args = {
    'owner': 'kaizen',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 11),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define optional manual trigger form parameters
params = {
    "fromDate": Param(
        default="",
        type=["null", "string"],
        description="Start date (YYYY-MM-DD), default is yesterday"
    ),
    "toDate": Param(
        default="",
        type=["null", "string"],
        description="End date (YYYY-MM-DD), default is today"
    ),
    "count": Param(
        default=10000,
        type="integer",
        description="Number of transactions to simulate"
    )
}

with DAG(
        'spark_transaction_simulation_pipeline',
        default_args=default_args,
        description='Manually triggered DAG to run BatchTransactionSimulatorJob for Postgres transaction backfill',
        schedule_interval=None,  # Triggered manually only
        catchup=False,
        params=params
) as dag:
    # Jinja arguments template
    args_tmpl = (
        "{% if dag_run.conf and dag_run.conf.get('fromDate') %} --fromDate '{{ dag_run.conf.get('fromDate') }}'{% endif %}"
        "{% if dag_run.conf and dag_run.conf.get('toDate') %} --toDate '{{ dag_run.conf.get('toDate') }}'{% endif %}"
        " --count '{{ (dag_run.conf.get('count') if (dag_run.conf and dag_run.conf.get('count') is not none) else 10000) }}'"
    )

    spark_cmd = (
        "/opt/spark/bin/spark-submit "
        "--master 'spark://kaizen-spark-master.compute.svc.cluster.local:7077' "
        "--class 'com.kaizen.spark.utility.BatchTransactionSimulatorJob' "
    )

    for key, val in spark_conf.items():
        spark_cmd += f"--conf '{key}={val}' "

    spark_cmd += "--conf spark.driver.host=$(hostname -i) "
    spark_cmd += f"'/tmp/kaizen-batch-processing.jar'{args_tmpl}"

    download_cmd = "curl -s -f -o /tmp/kaizen-batch-processing.jar http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar"
    full_command = f"{download_cmd} && {spark_cmd}"

    run_simulation = KubernetesPodOperator(
        task_id="run_simulation",
        name="run-simulation",
        namespace="orchestrator",
        image="apache/spark:3.4.1",
        cmds=["/bin/sh", "-c"],
        arguments=[full_command],
        get_logs=True,
        in_cluster=True,
        on_finish_action="delete_succeeded_pod",
        env_vars={
            "SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"
        }
    )
