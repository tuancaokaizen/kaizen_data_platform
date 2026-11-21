"""Unit tests for DW → Serving stable publish.
Kiểm thử publish ổn định DW → Serving.
"""
from __future__ import annotations

from jobs.kaizen_analytics.serving_publish import (
    publish_serving_stable,
    staging_name,
    write_derived_fact,
)


class _FakeClient:
    """In-memory stand-in for ClickHouseClient / Giả lập ClickHouseClient trong bộ nhớ."""

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.tables: dict[str, list[tuple]] = {}
        self.fail_exchange = False

    def execute(self, sql: str) -> str:
        text = " ".join(sql.split())
        if self.fail_exchange and text.upper().startswith("EXCHANGE TABLES"):
            raise RuntimeError("exchange not supported")
        self.executed.append(text)
        return ""

    def truncate(self, table: str) -> None:
        self.tables[table] = []
        self.executed.append(f"TRUNCATE {table}")

    def insert_tsv(self, table: str, columns, rows) -> int:
        materialised = [tuple(row) for row in rows]
        self.tables[table] = materialised
        self.executed.append(f"INSERT {table} n={len(materialised)}")
        return len(materialised)


def test_staging_name():
    assert staging_name("report.customer_segment") == "report.customer_segment__stg"


def test_publish_writes_dw_then_exchanges_serving():
    client = _FakeClient()
    rows = [("2026-11-07", "C1", "loyal")]
    cols = ("AsOfDate", "CustomerId", "SegmentCode")
    written = publish_serving_stable(
        client,
        report_table="report.customer_segment",
        report_create_sql="CREATE TABLE IF NOT EXISTS report.customer_segment (x Int8)",
        columns=cols,
        rows=rows,
        dw_table="dw.fact_customer_segment",
        dw_create_sql="CREATE TABLE IF NOT EXISTS dw.fact_customer_segment (x Int8)",
    )
    assert written == 1
    assert client.tables["dw.fact_customer_segment"] == rows
    # After EXCHANGE, staging is truncated / Sau EXCHANGE staging bị truncate
    assert any("EXCHANGE TABLES report.customer_segment AND report.customer_segment__stg" in sql for sql in client.executed)
    assert client.tables.get("report.customer_segment__stg") == []


def test_publish_fallback_truncate_insert_when_exchange_fails():
    client = _FakeClient()
    client.fail_exchange = True
    rows = [("2026-11-07", "C2")]
    written = publish_serving_stable(
        client,
        report_table="report.association_rule",
        report_create_sql="CREATE TABLE IF NOT EXISTS report.association_rule (x Int8)",
        columns=("AsOfDate", "CustomerId"),
        rows=rows,
        dw_table="dw.fact_association_rule",
        dw_create_sql="CREATE TABLE IF NOT EXISTS dw.fact_association_rule (x Int8)",
    )
    assert written == 1
    assert client.tables["report.association_rule"] == rows
    assert client.tables["dw.fact_association_rule"] == rows


def test_write_derived_fact_only():
    client = _FakeClient()
    n = write_derived_fact(
        client,
        dw_table="dw.fact_replenishment_plan",
        dw_create_sql="CREATE TABLE IF NOT EXISTS dw.fact_replenishment_plan (x Int8)",
        columns=("ShopCode",),
        rows=[("S1",)],
    )
    assert n == 1
    assert client.tables["dw.fact_replenishment_plan"] == [("S1",)]
