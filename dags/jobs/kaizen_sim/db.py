"""PostgreSQL access for the generator: connection from env or config.ini, upserts and deadlock retries.
Truy cập PostgreSQL cho bộ sinh: kết nối từ env hoặc config.ini, upsert và thử lại khi deadlock.
"""
import configparser
import json
import logging
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from psycopg2 import errors

log = logging.getLogger(__name__)

CONFIG_PATHS = (
    os.path.join(os.path.dirname(__file__), "..", "..", "config.ini"),
    "/opt/airflow/dags/config.ini",
)


def _settings() -> dict:
    cfg = configparser.ConfigParser()
    for path in CONFIG_PATHS:
        if os.path.exists(path):
            cfg.read(path)
            break
    pg = cfg["postgres"] if cfg.has_section("postgres") else {}
    # Env wins so local runs can point at a port-forward / Env được ưu tiên để chạy local qua port-forward
    return {
        "host": os.getenv("POSTGRES_HOST", pg.get("host")),
        "port": int(os.getenv("POSTGRES_PORT", pg.get("port", 5432))),
        "dbname": os.getenv("POSTGRES_DB", pg.get("database")),
        "user": os.getenv("POSTGRES_USER", pg.get("username")),
        "password": os.getenv("POSTGRES_PASSWORD", pg.get("password")),
    }


def connect():
    conn = psycopg2.connect(application_name="kaizen_sim", connect_timeout=15, **_settings())
    conn.autocommit = False
    return conn


@contextmanager
def connection():
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


def retry(conn, fn, attempts: int = 3):
    """Run fn(cur) in one transaction, retrying deadlocks and serialization failures.
    Chạy fn(cur) trong một transaction, thử lại khi deadlock hoặc lỗi tuần tự hoá.
    """
    for attempt in range(1, attempts + 1):
        try:
            with conn.cursor() as cur:
                result = fn(cur)
            conn.commit()
            return result
        except (errors.DeadlockDetected, errors.SerializationFailure):
            conn.rollback()
            if attempt == attempts:
                raise
            log.warning("deadlock, retry %s/%s", attempt, attempts)
            time.sleep(0.5 * attempt)
        except Exception:
            conn.rollback()
            raise


def upsert(cur, table: str, keys: tuple, cols: tuple, rows: list, created: bool = True) -> int:
    """Insert or update rows; ModifiedDate moves only when a value really changes, so incremental ingest
    does not re-read untouched master rows. Returns the number of rows inserted or changed.
    Chèn hoặc cập nhật; ModifiedDate chỉ đổi khi giá trị thực sự đổi, để ingest tăng dần không đọc lại dòng
    master không đụng tới. Trả về số dòng chèn hoặc đổi.
    """
    if not rows:
        return 0
    all_cols = keys + cols
    names = ", ".join(f'"{c}"' for c in all_cols)
    extra_names = ', "CreatedDate", "ModifiedDate"' if created else ', "ModifiedDate"'
    extra_values = ", now(), now()" if created else ", now()"
    conflict = ", ".join(f'"{c}"' for c in keys)
    sets = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols)
    changed = " OR ".join(f't."{c}" IS DISTINCT FROM EXCLUDED."{c}"' for c in cols)
    sql = (f"INSERT INTO {table} AS t ({names}{extra_names}) VALUES %s "
           f"ON CONFLICT ({conflict}) DO UPDATE SET {sets}, \"ModifiedDate\" = now() WHERE {changed}")
    template = "(" + ", ".join(["%s"] * len(all_cols)) + extra_values + ")"
    changed_rows = 0
    for start in range(0, len(rows), 2000):
        psycopg2.extras.execute_values(cur, sql, rows[start:start + 2000], template=template, page_size=2000)
        changed_rows += cur.rowcount
    return changed_rows


def update_rows(cur, table: str, key: str, cols: tuple, types: tuple, rows: list) -> int:
    """Update existing rows by key; ModifiedDate moves only for rows whose values change.
    Cập nhật dòng có sẵn theo khoá; ModifiedDate chỉ đổi với dòng có giá trị thay đổi.
    """
    if not rows:
        return 0
    sets = ", ".join(f'"{c}" = v."{c}"' for c in cols)
    changed = " OR ".join(f't."{c}" IS DISTINCT FROM v."{c}"' for c in cols)
    names = ", ".join(f'"{c}"' for c in (key,) + cols)
    template = "(%s, " + ", ".join(f"%s::{t}" for t in types) + ")"
    sql = (f'UPDATE {table} AS t SET {sets}, "ModifiedDate" = now() FROM (VALUES %s) AS v ({names}) '
           f'WHERE t."{key}" = v."{key}" AND ({changed})')
    psycopg2.extras.execute_values(cur, sql, rows, template=template, page_size=len(rows))
    return cur.rowcount


def apply_movement(cur, movement_id: str, movement_type: str, location_type: str, location_id: str,
                   product_id: str, lot_id: str, qty: int, unit_cost: int, ref_type: str, ref_id: str,
                   ref_line_id: str | None, reason: str | None, at: datetime):
    """inventory.apply_movement; None means the movement already exists. / None nghĩa là biến động đã có."""
    cur.execute("SELECT inventory.apply_movement(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (movement_id, movement_type, location_type, location_id, product_id, lot_id, qty, unit_cost,
                 ref_type, ref_id, ref_line_id, reason, at))
    return cur.fetchone()[0]


def outbox_logistics(cur, shipment_id: str, event_type: str, seq: int, payload: dict, at: datetime) -> None:
    cur.execute('INSERT INTO inventory."Outbox" ("Topic", "AggregateId", "EventType", "Version", "Payload", '
                '"EventTime") VALUES (%s, %s, %s, %s, %s, %s)',
                ("logistics-events", shipment_id, event_type, seq, json.dumps(payload, ensure_ascii=False), at))


def run_started(conn, run_id: str, mode: str, business_date) -> bool:
    """Record a run; False when a SUCCESS row already exists for run_id (nothing to do).
    Ghi nhận lần chạy; False khi run_id đã có dòng SUCCESS (không cần làm gì).
    """
    with conn.cursor() as cur:
        cur.execute('SELECT "Status" FROM datagen."GeneratorRun" WHERE "RunId" = %s', (run_id,))
        row = cur.fetchone()
        if row and row[0] == "SUCCESS":
            conn.rollback()
            return False
        cur.execute('INSERT INTO datagen."GeneratorRun" ("RunId", "Mode", "BusinessDate", "Status", "StartedAt") '
                    "VALUES (%s, %s, %s, 'RUNNING', now()) ON CONFLICT (\"RunId\") DO UPDATE "
                    "SET \"Status\" = 'RUNNING', \"StartedAt\" = now(), \"FinishedAt\" = NULL",
                    (run_id, mode, business_date))
    conn.commit()
    return True


def run_finished(conn, run_id: str, status: str, stats: dict) -> None:
    with conn.cursor() as cur:
        cur.execute('UPDATE datagen."GeneratorRun" SET "Status" = %s, "FinishedAt" = now(), "Stats" = %s '
                    'WHERE "RunId" = %s', (status, json.dumps(stats, default=str), run_id))
    conn.commit()


def utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc)
