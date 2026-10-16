"""spark-submit command of Kaizen batch jobs and the job classes named in the lakehouse DAGs.
Lệnh spark-submit của job batch Kaizen và các class job được gọi trong DAG lakehouse.
"""
import re
import sys
import types
from pathlib import Path

import pytest


class FakeSecret:
    """Stand-in for airflow Secret when Airflow is not installed / Thay cho Secret khi không cài Airflow."""

    def __init__(self, deploy_type, deploy_target, secret):
        self.deploy_type = deploy_type
        self.deploy_target = deploy_target
        self.secret = secret


# Register the stub before importing jobs.spark_task / Đăng ký stub trước khi import jobs.spark_task
_secret_module = types.ModuleType("airflow.providers.cncf.kubernetes.secret")
_secret_module.Secret = FakeSecret
for _name in ["airflow", "airflow.providers", "airflow.providers.cncf", "airflow.providers.cncf.kubernetes"]:
    sys.modules.setdefault(_name, types.ModuleType(_name))
sys.modules.setdefault("airflow.providers.cncf.kubernetes.secret", _secret_module)

from jobs.spark_task import FROM_DATE, TO_DATE, spark_job_task  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1]
JOB_SOURCES = DAGS_DIR.parent / "projects" / "batch-processing" / "src" / "main" / "scala" / "com" / "kaizen" / "batch"
BATCH_DAGS = [
    "kaizen_lakehouse_daily.py",
    "kaizen_lakehouse_maintenance.py",
    "spark_realtime_business_cold_migrate_pipeline.py",
    "spark_realtime_business_backfill_pipeline.py",
]


def command(task):
    return task["arguments"][0]


def test_arguments_are_positional_environment_dates_then_job_arguments():
    task = spark_job_task("bronze_master", "ingest.postgres.PostgresToBronze", ["master", "order,order_item"])
    expected = f"'/tmp/kaizen-batch.jar' 'PROD' '{FROM_DATE}' '{TO_DATE}' 'master' 'order,order_item'"
    assert command(task).endswith(expected)
    assert "--class 'com.kaizen.batch.ingest.postgres.PostgresToBronze'" in command(task)


def test_spark_conf_overrides_defaults_and_goes_through_conf_flags():
    task = spark_job_task("x", "processing.dim.DimShop", spark_conf={"spark.executor.memory": "2g"})
    assert "--conf 'spark.executor.memory=2g'" in command(task)
    assert "--conf 'spark.executor.memory=1g'" not in command(task)
    assert "--conf 'spark.driver.memory=1g'" in command(task)


def test_local_master_skips_driver_host():
    cluster = spark_job_task("x", "processing.dim.DimShop")
    local = spark_job_task("y", "maintenance.postgres.PostgresVacuum", master="local[1]")
    assert "spark.driver.host" in command(cluster)
    assert "--master 'local[1]'" in command(local)
    assert "spark.driver.host" not in command(local)


def test_secrets_come_from_the_kubernetes_secret_not_the_command():
    task = spark_job_task("x", "processing.dim.DimShop")
    assert [(s.deploy_type, s.secret) for s in task["secrets"]] == [("env", "kaizen-batch-secrets")]
    assert not re.search(r"access\.key|secret\.key|password", command(task), re.IGNORECASE)
    assert set(task["env_vars"]) == {"SPARK_SUBMIT_OPTS"}


@pytest.mark.parametrize("argument", ["a b", "x;rm", "$(id)", "'quoted'", ""])
def test_unsafe_job_arguments_are_rejected(argument):
    with pytest.raises(ValueError):
        spark_job_task("x", "processing.dim.DimShop", [argument])


def test_jinja_job_arguments_and_date_overrides_are_allowed():
    hot = "{{ (dag_run.conf.get('hotDays') if dag_run.conf else None) or '14' }}"
    seed = "{% if params.seedHot %}true{% else %}false{% endif %}"
    task = spark_job_task(
        "backfill",
        "publish.clickhouse.BackfillRealtimeBusinessCold",
        job_arguments=[hot, seed],
        from_date="{{ params.fromDate }}",
        to_date="{{ params.toDate }}",
    )
    assert f"'{hot}'" in command(task)
    assert f"'{seed}'" in command(task)
    assert "'{{ params.fromDate }}'" in command(task)
    assert "--class 'com.kaizen.batch.publish.clickhouse.BackfillRealtimeBusinessCold'" in command(task)
    assert "kaizen-batch.jar" in command(task)
    assert "kaizen-batch-processing" not in command(task)


def test_every_job_class_of_the_batch_dags_exists_in_the_batch_project():
    dag_text = "\n".join((DAGS_DIR / "pipelines" / name).read_text(encoding="utf-8") for name in BATCH_DAGS)
    qualified = re.findall(r'"((?:ingest|processing|publish|maintenance)\.[a-z.]+\.[A-Z]\w+)"', dag_text)
    # Classes built from lists, e.g. f"processing.dim.{name}" / Class ghép từ danh sách
    lists = [("processing.dim", "DIMENSIONS"), ("processing.fact", "FACTS"), ("processing.fact", "DERIVED_FACTS")]
    for package, list_name in lists:
        listed = re.search(rf"^{list_name} = [\[{{](.*?)[\]}}]", dag_text, re.DOTALL | re.MULTILINE).group(1)
        qualified += [f"{package}.{name}" for name in re.findall(r'"(\w+)"', listed)]
    # Report job classes from REPORTS tuples / Class report từ tuple REPORTS
    reports = re.search(r"^REPORTS = \[(.*?)\]", dag_text, re.DOTALL | re.MULTILINE)
    if reports:
        qualified += re.findall(
            r'"((?:processing\.report\.[a-z.]+\.[A-Z]\w+))"', reports.group(1)
        )

    assert len(qualified) >= 40
    assert "publish.clickhouse.MigrateRealtimeBusinessToCold" in qualified
    assert "publish.clickhouse.BackfillRealtimeBusinessCold" in qualified
    assert "ingest.postgres.PostgresToBronze" in qualified
    for job_class in set(qualified):
        source = JOB_SOURCES.joinpath(*job_class.split(".")).with_suffix(".scala")
        assert source.exists(), f"missing {source}"
        assert f"object {job_class.rsplit('.', 1)[1]}" in source.read_text(encoding="utf-8")


def test_lakehouse_dag_is_sequential_one_table_per_pod():
    text = (DAGS_DIR / "pipelines" / "kaizen_lakehouse_daily.py").read_text(encoding="utf-8")
    assert "max_active_tasks=1" in text
    assert "SOURCE_TABLES" in text
    assert "chain_sequential" in text
    # Per-table bronze args: group + table name / Bronze theo bảng: nhóm + tên bảng
    assert '[group, table]' in text or "[group, table]" in text
