"""KubernetesPodOperator arguments for Kaizen batch jobs (package com.kaizen.batch).
Tham số KubernetesPodOperator cho các job batch Kaizen (package com.kaizen.batch).

Every job receives positional arguments / Mọi job nhận tham số theo vị trí:
    args(0) environment (PROD)
    args(1) fromDate yyyy-MM-dd
    args(2) toDate yyyy-MM-dd (excluded / không bao gồm)
    args(3..) job arguments, documented above `main` of each job / tham số riêng, ghi ở trên `main` của từng job

Spark resources go through `--conf`; secrets come only from the Kubernetes Secret as env.
Tài nguyên Spark truyền qua `--conf`; secret chỉ đến từ Kubernetes Secret dưới dạng env.
"""
import re

# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.secret import Secret

JOB_PACKAGE = "com.kaizen.batch"
ENVIRONMENT = "PROD"
NAMESPACE = "orchestrator"
IMAGE = "apache/spark:3.4.1"
SPARK_MASTER = "spark://kaizen-spark-master.compute.svc.cluster.local:7077"
JAR_URL = "http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch.jar"
LOCAL_JAR = "/tmp/kaizen-batch.jar"

# Holds AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, KAIZEN_POSTGRES_PASSWORD, KAIZEN_CLICKHOUSE_PASSWORD
# Chứa AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, KAIZEN_POSTGRES_PASSWORD, KAIZEN_CLICKHOUSE_PASSWORD
SECRET_NAME = "kaizen-batch-secrets"

# Resources only; catalog and S3 settings live in common_prod.conf of the jar
# Chỉ tài nguyên; cấu hình catalog và S3 nằm trong common_prod.conf của jar
DEFAULT_SPARK_CONF = {
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.executor.cores": "1",
    "spark.cores.max": "2",
    "spark.jars.ivy": "/tmp/.ivy2",
    "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
    "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
}

# Manual runs default to [yesterday, today); scheduled runs use the data interval
# Chạy tay mặc định [hôm qua, hôm nay); chạy theo lịch dùng data interval
FROM_DATE = (
    "{{ (dag_run.conf.get('fromDate') if dag_run.conf else None) "
    "or (macros.ds_add(ds, -1) if dag_run.external_trigger else (data_interval_start | ds)) }}"
)
TO_DATE = (
    "{{ (dag_run.conf.get('toDate') if dag_run.conf else None) "
    "or (ds if dag_run.external_trigger else (data_interval_end | ds)) }}"
)

# Job arguments are table/group names or numbers, never shell syntax
# Tham số job là tên bảng/nhóm hoặc số, không bao giờ là cú pháp shell
SAFE_ARGUMENT = re.compile(r"^[A-Za-z0-9_.,\-]+$")


def _assert_safe_argument(task_id, argument):
    """Allow literals matching SAFE_ARGUMENT, or Airflow Jinja (rendered before the pod starts).
    Cho phép literal khớp SAFE_ARGUMENT, hoặc Jinja Airflow (được render trước khi pod chạy).
    """
    text = str(argument)
    if SAFE_ARGUMENT.match(text):
        return
    # Jinja: {{ ... }} or {% ... %} — value after render must still be a safe literal
    # Jinja: {{ ... }} hoặc {% ... %} — giá trị sau render vẫn phải là literal an toàn
    if "{{" in text or "{%" in text:
        return
    raise ValueError(f"Unsafe job argument for {task_id}: {argument!r}")


def spark_job_task(task_id, job_class, job_arguments=(), spark_conf=None, master=SPARK_MASTER,
                   from_date=None, to_date=None):
    """Build KubernetesPodOperator kwargs that run one batch job with spark-submit.
    Tạo kwargs KubernetesPodOperator chạy một job batch bằng spark-submit.

    job_class: class under com.kaizen.batch, e.g. "processing.dim.DimShop" / class dưới com.kaizen.batch
    job_arguments: args(3..) of the job / args(3..) của job
    spark_conf: overrides DEFAULT_SPARK_CONF / ghi đè DEFAULT_SPARK_CONF
    master: Spark master, "local[1]" for jobs that only talk to Postgres / "local[1]" cho job chỉ gọi Postgres
    from_date / to_date: override the default Jinja date macros (e.g. one-shot backfill defaults)
                        / ghi đè macro ngày mặc định (vd. mặc định cho backfill một lần)
    """
    for argument in job_arguments:
        _assert_safe_argument(task_id, argument)

    window_from = from_date if from_date is not None else FROM_DATE
    window_to = to_date if to_date is not None else TO_DATE
    _assert_safe_argument(task_id, window_from)
    _assert_safe_argument(task_id, window_to)

    conf = {**DEFAULT_SPARK_CONF, **(spark_conf or {})}
    conf_flags = " ".join(f"--conf '{key}={value}'" for key, value in conf.items())

    # Executors connect back to the driver on the pod IP / Executor kết nối ngược về driver qua IP của pod
    driver_host = "--conf spark.driver.host=$(hostname -i)" if master.startswith("spark://") else ""

    positional = [ENVIRONMENT, window_from, window_to, *[str(argument) for argument in job_arguments]]
    arguments = " ".join(f"'{argument}'" for argument in positional)

    spark_submit = (
        f"/opt/spark/bin/spark-submit --master '{master}' --class '{JOB_PACKAGE}.{job_class}' "
        f"{conf_flags} {driver_host} '{LOCAL_JAR}' {arguments}"
    )
    download = f"curl -s -f -o {LOCAL_JAR} {JAR_URL}"

    return {
        "task_id": task_id,
        "name": "kaizen-" + task_id.replace("_", "-"),
        "namespace": NAMESPACE,
        "image": IMAGE,
        "cmds": ["/bin/sh", "-c"],
        "arguments": [f"{download} && {spark_submit}"],
        "secrets": [Secret("env", None, SECRET_NAME)],
        "env_vars": {"SPARK_SUBMIT_OPTS": "-Divy.default.ivy.user.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2"},
        "get_logs": True,
        "in_cluster": True,
        "on_finish_action": "delete_succeeded_pod",
    }
