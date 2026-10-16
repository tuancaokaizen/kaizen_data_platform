"""Manual DAG: kaizen_sim BACKFILL or FILL_GAP into Postgres for [fromDate, toDate).

After sales are written, trigger lakehouse for the same window so Iceberg/fnd catch up.
Does not replace stream for live days — use FILL_GAP only when PG is short.

DAG tay: kaizen_sim BACKFILL hoặc FILL_GAP vào Postgres cho [fromDate, toDate).

Sau khi ghi bán hàng, trigger lakehouse cùng cửa sổ để Iceberg/fnd bắt kịp.
Không thay stream cho ngày live — chỉ FILL_GAP khi PG thiếu.
"""
from datetime import datetime, timedelta
import os
import sys

import pendulum
# pyrefly: ignore [missing-import]
from airflow import DAG
# pyrefly: ignore [missing-import]
from airflow.models.param import Param
# pyrefly: ignore [missing-import]
from airflow.operators.python import PythonOperator
# pyrefly: ignore [missing-import]
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from jobs.kaizen_sim import backfill  # type: ignore  # noqa: E402

ICT = pendulum.timezone("Asia/Ho_Chi_Minh")


def run_sim_backfill(**context):
    conf = context["dag_run"].conf or {}
    params = context["params"]
    from_date = (conf.get("fromDate") or params["fromDate"]).strip()
    to_date = (conf.get("toDate") or params["toDate"]).strip()
    mode = (conf.get("mode") or params["mode"]).strip().upper()
    target = int(conf.get("targetOrdersPerDay") or params["targetOrdersPerDay"])
    run_close = bool(conf.get("dailyClose") if conf.get("dailyClose") is not None else params["dailyClose"])
    return backfill.run(
        from_date=backfill._parse_date(from_date),
        to_date=backfill._parse_date(to_date),
        mode=mode,
        target_orders_per_day=target,
        run_daily_close=run_close,
    )


default_args = {
    "owner": "kaizen",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    "sim_backfill_pipeline",
    default_args=default_args,
    description="Backfill or gap-fill sales into Postgres, then trigger lakehouse for the same window",
    schedule_interval=None,
    start_date=datetime(2026, 10, 1, tzinfo=ICT),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=24),
    tags=["kaizen", "simulation", "backfill"],
    params={
        "fromDate": Param("2026-10-01", type="string", description="Inclusive start yyyy-MM-dd"),
        "toDate": Param("2026-10-02", type="string", description="Exclusive end yyyy-MM-dd"),
        "mode": Param("BACKFILL", type="string", enum=["BACKFILL", "FILL_GAP"]),
        # Match stream ConfigDemandModel (~7000/day) so historical days are comparable
        # Khớp ConfigDemandModel stream (~7000/ngày) để các ngày lịch sử tương đương
        "targetOrdersPerDay": Param(7000, type="integer"),
        "dailyClose": Param(False, type="boolean", description="Also run supply daily_close per day"),
    },
) as dag:
    # Long window (months) can take hours / Cửa sổ dài (nhiều tháng) có thể mất hàng giờ
    simulate = PythonOperator(
        task_id="sim_backfill",
        python_callable=run_sim_backfill,
        execution_timeout=timedelta(hours=12),
    )
    lakehouse = TriggerDagRunOperator(
        task_id="trigger_lakehouse",
        trigger_dag_id="kaizen_lakehouse_daily",
        conf={
            "fromDate": "{{ (dag_run.conf.get('fromDate') if dag_run.conf else None) or params.fromDate }}",
            "toDate": "{{ (dag_run.conf.get('toDate') if dag_run.conf else None) or params.toDate }}",
        },
        wait_for_completion=False,
    )
    simulate >> lakehouse
