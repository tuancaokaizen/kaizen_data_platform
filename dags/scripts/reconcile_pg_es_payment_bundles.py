#!/usr/bin/env python3
"""Reconcile Elasticsearch payment-bundles from PostgreSQL (source of truth).
Đồng bộ ES payment-bundles từ PostgreSQL (nguồn sự thật).

For each ICT calendar day in [from_date, to_date):
  1) delete ES docs in that day window (removes auto-id duplicates)
  2) rebuild PaymentBundle JSON from PG and index with _id = payment.Id

Usage:
  python3 reconcile_pg_es_payment_bundles.py --from-date 2026-01-01 --to-date 2026-08-09
"""

from __future__ import annotations

import argparse
import configparser
import json
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests

ICT = timezone(timedelta(hours=7))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reconcile ES payment-bundles from PG")
    p.add_argument("--from-date", required=True, help="Inclusive YYYY-MM-DD (Asia/Ho_Chi_Minh)")
    p.add_argument("--to-date", required=True, help="Exclusive YYYY-MM-DD (Asia/Ho_Chi_Minh)")
    p.add_argument("--config", default="/opt/airflow/dags/config.ini")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


def load_config(path: str) -> Tuple[dict, str]:
    cp = configparser.ConfigParser()
    if not cp.read(path):
        raise SystemExit(f"config not found: {path}")
    pg = {
        "host": cp.get("postgres", "host"),
        "port": cp.getint("postgres", "port", fallback=5432),
        "dbname": cp.get("postgres", "database"),
        "user": cp.get("postgres", "username"),
        "password": cp.get("postgres", "password"),
    }
    es = cp.get(
        "elasticsearch",
        "hosts",
        fallback="http://elasticsearch.logging.svc.cluster.local:9200",
    )
    return pg, es.rstrip("/")


def day_range(from_date: str, to_date: str) -> Iterable[Tuple[date, int, int]]:
    """Yield (day, start_ms, end_ms) for each ICT day in [from, to)."""
    start = date.fromisoformat(from_date)
    end = date.fromisoformat(to_date)
    cur = start
    while cur < end:
        start_dt = datetime(cur.year, cur.month, cur.day, tzinfo=ICT)
        end_dt = start_dt + timedelta(days=1)
        yield cur, int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)
        cur += timedelta(days=1)


def ms(ts) -> Optional[int]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return int(ts.timestamp() * 1000)
    return int(ts)


def delete_es_day(es_base: str, start_ms: int, end_ms: int, dry_run: bool) -> int:
    body = {
        "query": {
            "range": {"payment.createdDate": {"gte": start_ms, "lt": end_ms}}
        }
    }
    if dry_run:
        r = requests.post(f"{es_base}/payment-bundles/_count", json=body, timeout=60)
        r.raise_for_status()
        return int(r.json().get("count", 0))
    r = requests.post(
        f"{es_base}/payment-bundles/_delete_by_query?conflicts=proceed&refresh=true",
        json=body,
        timeout=600,
    )
    r.raise_for_status()
    return int(r.json().get("deleted", 0))


def fetch_payments(conn, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    start_ts = datetime.fromtimestamp(start_ms / 1000, tz=ICT)
    end_ts = datetime.fromtimestamp(end_ms / 1000, tz=ICT)
    sql = """
        SELECT "Id","PaymentType","Status","TotalPayment","CreatedDate","ModifiedDate"
        FROM payment."Payment"
        WHERE "CreatedDate" >= %s AND "CreatedDate" < %s
        ORDER BY "Id"
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (start_ts, end_ts))
        return [dict(r) for r in cur.fetchall()]


def fetch_transactions(conn, payment_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not payment_ids:
        return {}
    sql = """
        SELECT "Id","PaymentId","PaymentMethodId","Amount","CreatedDate","ModifiedDate"
        FROM payment."Transaction"
        WHERE "PaymentId" = ANY(%s)
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (payment_ids,))
        out: Dict[str, List[Dict[str, Any]]] = {}
        for r in cur.fetchall():
            out.setdefault(r["PaymentId"], []).append(dict(r))
        return out


def fetch_order_payments(conn, payment_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not payment_ids:
        return {}
    sql = """
        SELECT DISTINCT ON ("PaymentId")
               "Id","PaymentId","OrderId","CreatedDate","ModifiedDate"
        FROM payment."OrderPayment"
        WHERE "PaymentId" = ANY(%s)
        ORDER BY "PaymentId", "CreatedDate" DESC NULLS LAST
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (payment_ids,))
        return {r["PaymentId"]: dict(r) for r in cur.fetchall()}


def fetch_return_payments(conn, payment_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not payment_ids:
        return {}
    sql = """
        SELECT DISTINCT ON ("PaymentId")
               "Id","PaymentId","ReturnId","OrderId","CreatedDate","ModifiedDate"
        FROM payment."ReturnPayment"
        WHERE "PaymentId" = ANY(%s)
        ORDER BY "PaymentId", "CreatedDate" DESC NULLS LAST
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (payment_ids,))
        return {r["PaymentId"]: dict(r) for r in cur.fetchall()}


def to_payment_bundle(
    payment: Dict[str, Any],
    txs: List[Dict[str, Any]],
    op: Optional[Dict[str, Any]],
    rp: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    bundle: Dict[str, Any] = {
        "payment": {
            "id": payment["Id"],
            "paymentType": int(payment["PaymentType"] or 1),
            "status": payment["Status"],
            "totalPayment": payment["TotalPayment"],
            "createdDate": ms(payment["CreatedDate"]) or 0,
            "modifiedDate": ms(payment["ModifiedDate"]) or 0,
        },
        "transactions": [
            {
                "id": t["Id"],
                "paymentId": t["PaymentId"],
                "paymentMethodId": int(t["PaymentMethodId"] or 0),
                "amount": t["Amount"],
                "createdDate": ms(t["CreatedDate"]),
                "modifiedDate": ms(t["ModifiedDate"]),
            }
            for t in txs
        ],
        "orderPayment": None,
        "returnPayment": None,
    }
    if op:
        bundle["orderPayment"] = {
            "id": op["Id"],
            "paymentId": op["PaymentId"],
            "orderId": op["OrderId"],
            "createdDate": ms(op["CreatedDate"]) or 0,
            "modifiedDate": ms(op["ModifiedDate"]) or 0,
        }
    if rp:
        bundle["returnPayment"] = {
            "id": rp["Id"],
            "paymentId": rp["PaymentId"],
            "returnId": rp["ReturnId"],
            "orderId": rp["OrderId"],
            "createdDate": ms(rp["CreatedDate"]) or 0,
            "modifiedDate": ms(rp["ModifiedDate"]) or 0,
        }
    return bundle


def bulk_index(es_base: str, docs: List[Tuple[str, Dict[str, Any]]], dry_run: bool) -> int:
    if not docs:
        return 0
    if dry_run:
        return len(docs)
    lines: List[str] = []
    for doc_id, body in docs:
        lines.append(json.dumps({"index": {"_index": "payment-bundles", "_id": doc_id}}))
        lines.append(json.dumps(body, ensure_ascii=False))
    payload = "\n".join(lines) + "\n"
    r = requests.post(
        f"{es_base}/_bulk?refresh=false",
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=300,
    )
    r.raise_for_status()
    result = r.json()
    if result.get("errors"):
        errs = [it for it in result.get("items", []) if "error" in it.get("index", {})][:5]
        raise RuntimeError(f"bulk index errors: {errs}")
    return len(docs)


def reconcile_day(
    conn,
    es_base: str,
    day: date,
    start_ms: int,
    end_ms: int,
    batch_size: int,
    dry_run: bool,
) -> None:
    deleted = delete_es_day(es_base, start_ms, end_ms, dry_run)
    payments = fetch_payments(conn, start_ms, end_ms)
    print(f"[{day}] es_deleted={deleted} pg_payments={len(payments)}", flush=True)
    if not payments:
        return

    indexed = 0
    for i in range(0, len(payments), batch_size):
        chunk = payments[i : i + batch_size]
        ids = [p["Id"] for p in chunk]
        tx_map = fetch_transactions(conn, ids)
        op_map = fetch_order_payments(conn, ids)
        rp_map = fetch_return_payments(conn, ids)
        docs: List[Tuple[str, Dict[str, Any]]] = []
        for p in chunk:
            bundle = to_payment_bundle(
                p,
                tx_map.get(p["Id"], []),
                op_map.get(p["Id"]),
                rp_map.get(p["Id"]),
            )
            docs.append((p["Id"], bundle))
        indexed += bulk_index(es_base, docs, dry_run)
        print(f"  indexed {indexed}/{len(payments)}", flush=True)

    if not dry_run:
        requests.post(f"{es_base}/payment-bundles/_refresh", timeout=120).raise_for_status()
    body = {"query": {"range": {"payment.createdDate": {"gte": start_ms, "lt": end_ms}}}}
    cnt = requests.post(f"{es_base}/payment-bundles/_count", json=body, timeout=60)
    cnt.raise_for_status()
    es_count = int(cnt.json().get("count", 0))
    status = "OK" if es_count == len(payments) else "MISMATCH"
    print(f"[{day}] verify es={es_count} pg={len(payments)} → {status}", flush=True)
    if status != "OK" and not dry_run:
        raise SystemExit(f"ES/PG mismatch for {day}")


def main() -> None:
    args = parse_args()
    pg, es_base = load_config(args.config)
    print(
        f"ES={es_base} range=[{args.from_date},{args.to_date}) dry_run={args.dry_run}",
        flush=True,
    )
    conn = psycopg2.connect(**pg)
    try:
        for day, start_ms, end_ms in day_range(args.from_date, args.to_date):
            reconcile_day(conn, es_base, day, start_ms, end_ms, args.batch_size, args.dry_run)
    finally:
        conn.close()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
