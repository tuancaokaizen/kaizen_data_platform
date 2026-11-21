"""Stable publish: derived fact @ DW first, then atomic swap into Serving.
Publish ổn định: ghi derived fact vào DW trước, rồi đổi atomic sang Serving.
"""
from __future__ import annotations

import logging
from typing import Iterable, Sequence

from jobs.kaizen_analytics.clickhouse_client import ClickHouseClient

log = logging.getLogger(__name__)

# Staging suffix for zero-downtime swap / Hậu tố staging để đổi bảng không downtime
_STAGING_SUFFIX = "__stg"


def staging_name(report_table: str) -> str:
    """Build staging table name next to the Serving table.
    Tên bảng staging cạnh bảng Serving.
    """
    return f"{report_table}{_STAGING_SUFFIX}"


def ensure_table(client: ClickHouseClient, create_sql: str) -> None:
    """Create table if missing / Tạo bảng nếu chưa có."""
    client.execute(create_sql)


def ensure_clone_structure(
    client: ClickHouseClient, source_table: str, target_table: str
) -> None:
    """Clone empty structure for staging (same engine/columns as Serving).
    Clone cấu trúc rỗng cho staging (cùng engine/cột với Serving).
    """
    # IF NOT EXISTS keeps re-runs idempotent / IF NOT EXISTS giữ chạy lại an toàn
    client.execute(
        f"CREATE TABLE IF NOT EXISTS {target_table} AS {source_table}"
    )


def write_derived_fact(
    client: ClickHouseClient,
    *,
    dw_table: str,
    dw_create_sql: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> int:
    """Materialize derived fact in the warehouse (source of truth).
    Materialize derived fact trong warehouse (nguồn sự thật).
    """
    # Ensure database exists when table is schema-qualified / Đảm bảo DB tồn tại khi có schema
    if "." in dw_table:
        database = dw_table.split(".", 1)[0]
        client.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    ensure_table(client, dw_create_sql)
    client.truncate(dw_table)
    written = client.insert_tsv(dw_table, columns, rows)
    log.info("derived fact ready: %s rows=%s", dw_table, written)
    return written


def publish_serving_stable(
    client: ClickHouseClient,
    *,
    report_table: str,
    report_create_sql: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[object]],
    dw_table: str | None = None,
    dw_create_sql: str | None = None,
) -> int:
    """Write DW (optional) then swap staging → report without empty Serving window.
    Ghi DW (nếu có) rồi đổi staging → report, tránh cửa sổ Serving trống.
    """
    # 1) Serving DDL must exist before clone / DDL Serving phải có trước khi clone
    ensure_table(client, report_create_sql)
    stg = staging_name(report_table)
    ensure_clone_structure(client, report_table, stg)

    # 2) Warehouse derived fact first (canonical) / Derived fact warehouse trước (chuẩn)
    if dw_table and dw_create_sql:
        write_derived_fact(
            client,
            dw_table=dw_table,
            dw_create_sql=dw_create_sql,
            columns=columns,
            rows=rows,
        )

    # 3) Load staging fully, then atomic EXCHANGE / Nạp đủ staging rồi EXCHANGE atomic
    client.truncate(stg)
    written = client.insert_tsv(stg, columns, rows)
    try:
        client.execute(f"EXCHANGE TABLES {report_table} AND {stg}")
    except Exception as exc:
        # Fallback: truncate+insert if EXCHANGE unsupported / Fallback nếu EXCHANGE không hỗ trợ
        log.warning(
            "EXCHANGE failed for %s (%s); falling back to truncate+insert",
            report_table,
            exc,
        )
        client.truncate(report_table)
        written = client.insert_tsv(report_table, columns, rows)
    else:
        # Staging now holds old Serving data — clear for next run
        # Staging đang giữ data Serving cũ — xóa sạch cho lần sau
        try:
            client.truncate(stg)
        except Exception as clear_exc:
            log.warning("staging truncate skipped: %s", clear_exc)

    log.info("serving published: %s rows=%s (via staging)", report_table, written)
    return written


def ddl_replacing(table: str, columns_sql: str, order_by: str) -> str:
    """Shared ReplacingMergeTree DDL helper / Helper DDL ReplacingMergeTree dùng chung."""
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        {columns_sql}
    ) ENGINE = ReplacingMergeTree()
    ORDER BY {order_by}
    """
