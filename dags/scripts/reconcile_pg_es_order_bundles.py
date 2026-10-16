#!/usr/bin/env python3
"""Reconcile Elasticsearch order-bundles from PostgreSQL (source of truth).
Đồng bộ ES order-bundles từ PostgreSQL (nguồn sự thật).

For each ICT calendar day in [from_date, to_date):
  1) delete ES docs in that day window (removes auto-id duplicates)
  2) rebuild OrderBundle JSON from PG and index with _id = order.Id

Usage (inside cluster / chạy trong cluster):
  python3 reconcile_pg_es_order_bundles.py --from-date 2026-07-11 --to-date 2026-07-12
  python3 reconcile_pg_es_order_bundles.py --from-date 2026-07-10 --to-date 2026-08-08
"""

from __future__ import annotations

import argparse
import configparser
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests

ICT = timezone(timedelta(hours=7))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reconcile ES order-bundles from PG")
    p.add_argument("--from-date", required=True, help="Inclusive YYYY-MM-DD (Asia/Ho_Chi_Minh)")
    p.add_argument("--to-date", required=True, help="Exclusive YYYY-MM-DD (Asia/Ho_Chi_Minh)")
    p.add_argument("--config", default="/opt/airflow/dags/config.ini")
    p.add_argument("--batch-size", type=int, default=200)
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
    es = cp.get("elasticsearch", "hosts", fallback="http://elasticsearch.logging.svc.cluster.local:9200")
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
            "range": {
                "order.transactionDate": {"gte": start_ms, "lt": end_ms}
            }
        }
    }
    if dry_run:
        r = requests.post(f"{es_base}/order-bundles/_count", json=body, timeout=60)
        r.raise_for_status()
        return int(r.json().get("count", 0))
    r = requests.post(
        f"{es_base}/order-bundles/_delete_by_query?conflicts=proceed&refresh=true",
        json=body,
        timeout=600,
    )
    r.raise_for_status()
    return int(r.json().get("deleted", 0))


def fetch_orders(conn, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    start_ts = datetime.fromtimestamp(start_ms / 1000, tz=ICT)
    end_ts = datetime.fromtimestamp(end_ms / 1000, tz=ICT)
    sql = """
        SELECT "Id","CustomerId","ShopCode","TransactionDate","TotalBill","TotalDiscount",
               "TotalPayment","EmployeeSaleId","Status","VoucherSeriesCode","CreatedDate","ModifiedDate"
        FROM "order"."Order"
        WHERE "TransactionDate" >= %s AND "TransactionDate" < %s
        ORDER BY "Id"
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (start_ts, end_ts))
        rows = cur.fetchall()
    return [dict(r) for r in rows]


def fetch_items(conn, order_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not order_ids:
        return {}
    sql = """
        SELECT "Id","OrderId","ProductId","Quantity","UnitPrice","CreatedDate","ModifiedDate"
        FROM "order"."OrderItem"
        WHERE "OrderId" = ANY(%s)
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (order_ids,))
        rows = cur.fetchall()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        out.setdefault(r["OrderId"], []).append(dict(r))
    return out


def fetch_returns(conn, order_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """One return per order (latest by CreatedDate) — matches stream bundle shape.
    Một return / order (mới nhất) — khớp shape bundle stream.
    """
    if not order_ids:
        return {}
    sql = """
        SELECT DISTINCT ON ("OrderId")
               "Id","OrderId","ReturnAmount","CreatedDate","ModifiedDate"
        FROM "order"."Return"
        WHERE "OrderId" = ANY(%s)
        ORDER BY "OrderId", "CreatedDate" DESC NULLS LAST
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (order_ids,))
        rows = cur.fetchall()
    return {r["OrderId"]: dict(r) for r in rows}


def fetch_return_items(conn, return_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not return_ids:
        return {}
    sql = """
        SELECT "Id","ReturnId","ProductId","Quantity","UnitPrice","OrderItemId","OrderId",
               "TotalAmount","CreatedDate","ModifiedDate"
        FROM "order"."ReturnItem"
        WHERE "ReturnId" = ANY(%s)
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (return_ids,))
        rows = cur.fetchall()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        out.setdefault(r["ReturnId"], []).append(dict(r))
    return out


def to_order_bundle(order: Dict[str, Any], items: List[Dict[str, Any]],
                    ret: Optional[Dict[str, Any]], ret_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    bundle: Dict[str, Any] = {
        "order": {
            "id": order["Id"],
            "customerId": order["CustomerId"],
            "shopCode": order["ShopCode"],
            "transactionDate": ms(order["TransactionDate"]),
            "totalBill": order["TotalBill"],
            "totalDiscount": order["TotalDiscount"],
            "totalPayment": order["TotalPayment"],
            "employeeSaleId": order["EmployeeSaleId"],
            "status": order["Status"],
            "voucherSeriesCode": order["VoucherSeriesCode"],
            "createdDate": ms(order["CreatedDate"]) or 0,
            "modifiedDate": ms(order["ModifiedDate"]) or 0,
        },
        "orderItems": [
            {
                "id": i["Id"],
                "orderId": i["OrderId"],
                "productId": i["ProductId"],
                "quantity": i["Quantity"],
                "unitPrice": i["UnitPrice"],
                "createdDate": ms(i["CreatedDate"]),
                "modifiedDate": ms(i["ModifiedDate"]),
            }
            for i in items
        ],
        "returnRecord": None,
        "returnItems": None,
    }
    if ret:
        bundle["returnRecord"] = {
            "id": ret["Id"],
            "orderId": ret["OrderId"],
            "returnAmount": ret["ReturnAmount"],
            "createdDate": ms(ret["CreatedDate"]),
            # PG ModifiedDate may be time-of-day; store epoch when possible
            # ModifiedDate PG có thể là time-of-day; lưu epoch khi có
            "modifiedTimeMs": ms(ret["ModifiedDate"]) if isinstance(ret.get("ModifiedDate"), datetime) else None,
        }
        bundle["returnItems"] = [
            {
                "id": ri["Id"],
                "returnId": ri["ReturnId"],
                "productId": ri["ProductId"],
                "quantity": ri["Quantity"],
                "unitPrice": ri["UnitPrice"],
                "orderItemId": ri["OrderItemId"],
                "orderId": ri["OrderId"],
                "totalAmount": ri["TotalAmount"],
                "createdDate": ms(ri["CreatedDate"]),
                "modifiedDate": ms(ri["ModifiedDate"]),
            }
            for ri in ret_items
        ]
    return bundle


def bulk_index(es_base: str, docs: List[Tuple[str, Dict[str, Any]]], dry_run: bool) -> int:
    if not docs:
        return 0
    if dry_run:
        return len(docs)
    lines: List[str] = []
    for doc_id, body in docs:
        lines.append(json.dumps({"index": {"_index": "order-bundles", "_id": doc_id}}))
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
        # Surface first few item errors / Hiện vài lỗi đầu
        errs = [it for it in result.get("items", []) if "error" in it.get("index", {})][:5]
        raise RuntimeError(f"bulk index errors: {errs}")
    return len(docs)


def reconcile_day(conn, es_base: str, day: date, start_ms: int, end_ms: int,
                  batch_size: int, dry_run: bool) -> None:
    deleted = delete_es_day(es_base, start_ms, end_ms, dry_run)
    orders = fetch_orders(conn, start_ms, end_ms)
    print(f"[{day}] es_deleted={deleted} pg_orders={len(orders)}", flush=True)
    if not orders:
        return

    indexed = 0
    for i in range(0, len(orders), batch_size):
        chunk = orders[i : i + batch_size]
        ids = [o["Id"] for o in chunk]
        items_map = fetch_items(conn, ids)
        returns_map = fetch_returns(conn, ids)
        ret_ids = [r["Id"] for r in returns_map.values()]
        ret_items_map = fetch_return_items(conn, ret_ids)

        docs: List[Tuple[str, Dict[str, Any]]] = []
        for o in chunk:
            ret = returns_map.get(o["Id"])
            ri = ret_items_map.get(ret["Id"], []) if ret else []
            bundle = to_order_bundle(o, items_map.get(o["Id"], []), ret, ri)
            docs.append((o["Id"], bundle))
        indexed += bulk_index(es_base, docs, dry_run)
        print(f"  indexed {indexed}/{len(orders)}", flush=True)

    if not dry_run:
        requests.post(f"{es_base}/order-bundles/_refresh", timeout=120).raise_for_status()
    # Verify count / Kiểm tra count
    body = {"query": {"range": {"order.transactionDate": {"gte": start_ms, "lt": end_ms}}}}
    cnt = requests.post(f"{es_base}/order-bundles/_count", json=body, timeout=60)
    cnt.raise_for_status()
    es_count = int(cnt.json().get("count", 0))
    status = "OK" if es_count == len(orders) else "MISMATCH"
    print(f"[{day}] verify es={es_count} pg={len(orders)} → {status}", flush=True)
    if status != "OK" and not dry_run:
        raise SystemExit(f"ES/PG mismatch for {day}")


def main() -> None:
    args = parse_args()
    pg, es_base = load_config(args.config)
    print(f"ES={es_base} range=[{args.from_date},{args.to_date}) dry_run={args.dry_run}", flush=True)
    conn = psycopg2.connect(**pg)
    try:
        for day, start_ms, end_ms in day_range(args.from_date, args.to_date):
            reconcile_day(conn, es_base, day, start_ms, end_ms, args.batch_size, args.dry_run)
    finally:
        conn.close()
    print("DONE", flush=True)


if __name__ == "__main__":
    main()
