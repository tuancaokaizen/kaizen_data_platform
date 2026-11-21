"""Thin ClickHouse HTTP client shared by analytics steps.
Client HTTP ClickHouse mỏng dùng chung cho các bước analytics.
"""
from __future__ import annotations

import configparser
import csv
import io
import logging
import os
from typing import Any, Iterable, Sequence

import requests

log = logging.getLogger(__name__)

CONFIG_PATHS = (
    os.path.join(os.path.dirname(__file__), "..", "..", "config.ini"),
    "/opt/airflow/dags/config.ini",
    "/opt/airflow/config.ini",
)


def _connection() -> dict[str, Any]:
    cfg = configparser.ConfigParser()
    for path in CONFIG_PATHS:
        if os.path.exists(path):
            cfg.read(path)
            break
    section = cfg["clickhouse"] if cfg.has_section("clickhouse") else {}
    hosts = os.getenv(
        "CLICKHOUSE_HOST",
        section.get("hosts", "clickhouse-service.databases.svc.cluster.local"),
    )
    host = hosts.split(",")[0].strip()
    return {
        "host": host,
        "port": int(os.getenv("CLICKHOUSE_HTTP_PORT", section.get("port", 8123))),
        "user": os.getenv("CLICKHOUSE_USER", section.get("username", "admin")),
        "password": os.getenv("CLICKHOUSE_PASSWORD", section.get("password", "kaizen123")),
    }


class ClickHouseClient:
    """Execute SQL and bulk-load rows through the HTTP interface.
    Chạy SQL và nạp hàng loạt qua giao diện HTTP.
    """

    def __init__(self, timeout: int = 120) -> None:
        self._cfg = _connection()
        self._timeout = timeout
        self._base = f"http://{self._cfg['host']}:{self._cfg['port']}/"

    def execute(self, sql: str) -> str:
        # Lab port-forwards drop idle HTTP — retry transient resets
        # Port-forward lab hay đứt khi HTTP idle — thử lại lỗi tạm thời
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                response = requests.post(
                    self._base,
                    params={"user": self._cfg["user"], "password": self._cfg["password"]},
                    data=sql.encode("utf-8"),
                    timeout=self._timeout,
                )
                if response.status_code != 200:
                    raise RuntimeError(f"ClickHouse HTTP {response.status_code}: {response.text[:800]}")
                return response.text
            except (requests.RequestException, RuntimeError) as exc:
                last_error = exc
                log.warning("clickhouse execute attempt %s/3 failed: %s", attempt, exc)
        assert last_error is not None
        raise last_error

    def query_rows(self, sql: str) -> list[tuple]:
        # TabSeparated keeps parsing simple for lab volumes / TabSeparated giữ parsing đơn giản cho lab
        text = self.execute(sql.rstrip(";\n ") + " FORMAT TabSeparated")
        if not text.strip():
            return []
        rows: list[tuple] = []
        for line in text.splitlines():
            if not line:
                continue
            rows.append(tuple(line.split("\t")))
        return rows

    def query_dicts(self, sql: str, columns: Sequence[str]) -> list[dict[str, Any]]:
        return [dict(zip(columns, row)) for row in self.query_rows(sql)]

    def truncate(self, table: str) -> None:
        self.execute(f"TRUNCATE TABLE IF EXISTS {table}")

    def insert_tsv(self, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]]) -> int:
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter="\t", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        count = 0
        for row in rows:
            writer.writerow(["" if value is None else value for value in row])
            count += 1
        if count == 0:
            log.warning("skip insert into %s: no rows", table)
            return 0
        col_list = ", ".join(columns)
        sql = f"INSERT INTO {table} ({col_list}) FORMAT TabSeparated\n{buffer.getvalue()}"
        self.execute(sql)
        log.info("inserted %s rows into %s", count, table)
        return count
