"""Batch backfill / gap-fill of sales into Postgres for past days (replaces BatchTransactionSimulatorJob).

Modes:
  BACKFILL  — generate target_orders_per_day for every day in [from_date, to_date)
  FILL_GAP  — only days whose existing order count is below target

After writing PG, run lakehouse ingest for the same window (Airflow DAG chains that).
Does not write Kafka: stream gaps with PG already filled are rebuilt from fnd (BackfillRealtimeBusinessCold).

Làm giàu / bù gap bán hàng vào Postgres cho ngày quá khứ (thay BatchTransactionSimulatorJob).

Chế độ:
  BACKFILL  — sinh target_orders_per_day cho mọi ngày trong [from_date, to_date)
  FILL_GAP  — chỉ các ngày có số đơn hiện có dưới target

Sau khi ghi PG, chạy lakehouse ingest cùng cửa sổ (DAG Airflow nối tiếp).
Không ghi Kafka: gap stream mà PG đã đủ thì dựng lại từ fnd (BackfillRealtimeBusinessCold).
"""
from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, datetime, time, timedelta
from typing import Iterable

from . import db, rng
from .daily_close import close_day
from .settings import ICT, SEED

log = logging.getLogger(__name__)

# Open 07:00–22:00 ICT with lunch and evening peaks / Mở 07–22 giờ VN, đỉnh trưa và tối
HOUR_WEIGHTS = {
    7: 0.04, 8: 0.06, 9: 0.08, 10: 0.12, 11: 0.11,
    12: 0.07, 13: 0.06, 14: 0.07, 15: 0.08, 16: 0.08,
    17: 0.09, 18: 0.11, 19: 0.10, 20: 0.08, 21: 0.05,
}

# Historical / backfill ids use ORD-… (data_model_v2); stream live uses SORD-… — never collide.
# Id lịch sử / backfill dùng ORD-… (data_model_v2); stream live dùng SORD-… — không trùng.
ID_PREFIX = "ORD"


def _parse_date(text: str) -> date:
    return date.fromisoformat(text.strip())


def days_in_range(from_date: date, to_date: date) -> list[date]:
    """Half-open [from_date, to_date). / Nửa mở [from_date, to_date)."""
    if from_date >= to_date:
        raise ValueError(f"from_date must be before to_date, got [{from_date}, {to_date})")
    days = []
    cursor = from_date
    while cursor < to_date:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def hour_for_index(day: date, index: int, total: int) -> int:
    """Pick an open hour by cumulative weights (deterministic). / Chọn giờ mở theo trọng số tích luỹ."""
    weights = list(HOUR_WEIGHTS.items())
    total_w = sum(w for _, w in weights)
    # Spread orders evenly across the cumulative curve / Rải đơn đều trên đường cong tích luỹ
    target = ((index + 0.5) / max(total, 1)) * total_w
    acc = 0.0
    for hour, weight in weights:
        acc += weight
        if target <= acc:
            return hour
    return weights[-1][0]


def order_timestamp(day: date, index: int, total: int) -> datetime:
    hour = hour_for_index(day, index, total)
    minute = rng.integer(0, 59, day.isoformat(), index, "minute")
    second = rng.integer(0, 59, day.isoformat(), index, "second")
    return datetime.combine(day, time(hour, minute, second), ICT)


def gap_days(existing: dict[date, int], days: Iterable[date], target: int) -> dict[date, int]:
    """Days → how many orders still needed to reach target. / Ngày → số đơn còn thiếu tới target."""
    needed = {}
    for day in days:
        short = target - existing.get(day, 0)
        if short > 0:
            needed[day] = short
    return needed


def _new_id(kind: str, day: date, shop: str, index: int, line: int = 0) -> str:
    """Build a deterministic id: ORD-/OI-/PAY-/TX-/OP- with yyyymmdd + shop + seq (legacy style).
    Sinh id tất định: ORD-/OI-/PAY-/TX-/OP- với yyyymmdd + shop + seq (kiểu legacy).
    """
    ymd = day.strftime("%Y%m%d")
    seq = f"{index:05d}"
    if kind == "ORD":
        return f"ORD-{ymd}-{shop}-{seq}"
    if kind == "OI":
        return f"OI-{ymd}-{shop}-{seq}-{line:02d}"
    if kind == "PAY":
        return f"PAY-{ymd}-{shop}-{seq}"
    if kind == "TX":
        return f"TX-{ymd}-{shop}-{seq}"
    if kind == "OP":
        return f"OP-{ymd}-{shop}-{seq}"
    # Fallback kept deterministic / Dự phòng vẫn tất định
    digest = uuid.uuid5(uuid.NAMESPACE_URL, f"{SEED}|{kind}|{ymd}|{shop}|{index}|{line}").hex[:12]
    return f"{ID_PREFIX}-{kind}-{digest}"


def _load_masters(cur) -> dict:
    cur.execute("""SELECT "Code" FROM shop."Shop" WHERE "CloseDate" IS NULL ORDER BY "Code" """)
    shops = [row[0] for row in cur.fetchall()]
    # Employees live under shop schema (not hr) / Nhân viên nằm schema shop (không phải hr)
    cur.execute("""SELECT e."Id", e."ShopCode" FROM shop."Employee" e WHERE e."Status" IS TRUE ORDER BY e."Id" """)
    employees = cur.fetchall()
    cur.execute("""SELECT "Id" FROM customer."Customer" ORDER BY "Id" LIMIT 5000""")
    customers = [row[0] for row in cur.fetchall()]
    cur.execute("""SELECT "Id", "RetailPrice", "StockPrice" FROM product."Product"
                   WHERE "Status" IS TRUE AND "RetailPrice" > 0 ORDER BY "Id" """)
    products = cur.fetchall()
    cur.execute("""SELECT "Id" FROM payment."PaymentMethod" ORDER BY "Id" """)
    methods = [row[0] for row in cur.fetchall()]
    if not shops or not employees or not products or not methods:
        raise RuntimeError("Missing master data (shop / employee / product / payment method)")
    by_shop = {}
    for emp_id, shop in employees:
        by_shop.setdefault(shop, []).append(emp_id)
    return {
        "shops": [s for s in shops if s in by_shop],
        "employees_by_shop": by_shop,
        "customers": customers or [None],
        "products": products,
        "methods": methods,
    }


def _existing_counts(cur, from_date: date, to_date: date) -> dict[date, int]:
    cur.execute("""SELECT "OrderDate", count(*) FROM "order"."Order"
                   WHERE "OrderDate" >= %s AND "OrderDate" < %s
                   GROUP BY 1""", (from_date, to_date))
    return {row[0]: int(row[1]) for row in cur.fetchall()}


def _insert_orders(cur, day: date, count: int, masters: dict, start_index: int = 0) -> int:
    """Insert `count` orders; seq starts at start_index so FILL_GAP does not reuse ORD-…-seq.
    Chèn `count` đơn; seq bắt đầu từ start_index để FILL_GAP không tái sử dụng ORD-…-seq.
    """
    if count <= 0:
        return 0
    shops = masters["shops"]
    products = masters["products"]
    methods = masters["methods"]
    customers = masters["customers"]
    inserted = 0
    total_for_day = start_index + count
    for index in range(start_index, start_index + count):
        when = order_timestamp(day, index, total_for_day)
        shop = rng.choice(shops, day.isoformat(), index, "shop")
        emp = rng.choice(masters["employees_by_shop"][shop], day.isoformat(), index, "emp")
        customer = rng.choice(customers, day.isoformat(), index, "customer")
        order_id = _new_id("ORD", day, shop, index)
        item_count = rng.integer(1, 4, day.isoformat(), index, "items")
        lines = []
        total_bill = 0
        for line_no in range(1, item_count + 1):
            product_id, retail, cost = rng.choice(products, day.isoformat(), index, line_no, "product")
            qty = rng.integer(1, 5, day.isoformat(), index, line_no, "qty")
            line_amount = qty * int(retail)
            total_bill += line_amount
            lines.append((product_id, qty, int(retail), int(cost), line_amount, line_no))

        total_payment = total_bill
        cur.execute(
            """INSERT INTO "order"."Order" ("Id", "ShopCode", "CustomerId", "EmployeeSaleId", "TransactionDate",
               "OrderDate", "TotalBill", "TotalDiscount", "TotalPayment", "VoucherSeriesCode", "Status",
               "CreatedDate", "ModifiedDate")
               VALUES (%s,%s,%s,%s,%s,%s,%s,0,%s,NULL,'Completed',%s,%s)
               ON CONFLICT ("Id") DO NOTHING""",
            (order_id, shop, customer, emp, when, day, total_bill, total_payment, when, when),
        )
        if cur.rowcount == 0:
            continue

        for product_id, qty, retail, cost, line_amount, line_no in lines:
            item_id = _new_id("OI", day, shop, index, line_no)
            cur.execute(
                """INSERT INTO "order"."OrderItem" ("Id", "OrderId", "LineNo", "ProductId", "Quantity", "UnitPrice",
                   "DiscountAmount", "LineAmount", "IsGift", "UnitCost", "OrderDate", "CreatedDate", "ModifiedDate")
                   VALUES (%s,%s,%s,%s,%s,%s,0,%s,false,%s,%s,%s,%s)""",
                (item_id, order_id, line_no, product_id, qty, retail, line_amount, cost, day, when, when),
            )

        pay_at = when + timedelta(seconds=rng.integer(2, 20, day.isoformat(), index, "pay_lag"))
        pay_id = _new_id("PAY", day, shop, index)
        method = rng.choice(methods, day.isoformat(), index, "method")
        cur.execute(
            """INSERT INTO payment."Payment" ("Id", "PaymentType", "Status", "OrderId", "ReturnId", "ShopCode",
               "TotalPayment", "Attempt", "PaymentDate", "CreatedDate", "ModifiedDate")
               VALUES (%s,1,'SUCCESS',%s,NULL,%s,%s,1,%s,%s,%s)""",
            (pay_id, order_id, shop, total_payment, day, pay_at, pay_at),
        )
        cur.execute(
            """INSERT INTO payment."Transaction" ("Id", "PaymentId", "PaymentMethodId", "Amount", "PaymentDate",
               "CreatedDate", "ModifiedDate") VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (_new_id("TX", day, shop, index), pay_id, method, total_payment, day, pay_at, pay_at),
        )
        cur.execute(
            """INSERT INTO payment."OrderPayment" ("Id", "PaymentId", "OrderId", "PaymentDate", "CreatedDate",
               "ModifiedDate") VALUES (%s,%s,%s,%s,%s,%s)""",
            (_new_id("OP", day, shop, index), pay_id, order_id, day, pay_at, pay_at),
        )
        inserted += 1
    return inserted


def run(
    from_date: date,
    to_date: date,
    mode: str = "BACKFILL",
    target_orders_per_day: int = 10000,
    run_daily_close: bool = False,
) -> dict:
    """Generate sales for [from_date, to_date). / Sinh bán hàng cho [from_date, to_date)."""
    mode = mode.upper()
    if mode not in {"BACKFILL", "FILL_GAP"}:
        raise ValueError(f"mode must be BACKFILL or FILL_GAP, got {mode}")
    days = days_in_range(from_date, to_date)
    summary = {"mode": mode, "days": 0, "orders": 0, "per_day": {}}

    with db.connection() as conn:
        def work(cur):
            masters = _load_masters(cur)
            existing = _existing_counts(cur, from_date, to_date) if mode == "FILL_GAP" else {}
            plan = (
                gap_days(existing, days, target_orders_per_day)
                if mode == "FILL_GAP"
                else {day: target_orders_per_day for day in days}
            )
            total = 0
            for day, need in sorted(plan.items()):
                # Seq continues after rows already on that day (ORD or SORD) / Seq nối tiếp số dòng đã có trong ngày
                n = _insert_orders(cur, day, need, masters, start_index=existing.get(day, 0))
                summary["per_day"][day.isoformat()] = n
                total += n
            summary["days"] = len(plan)
            summary["orders"] = total
            return summary

        db.retry(conn, work)

    if run_daily_close:
        now = datetime.now(ICT)
        with db.connection() as conn:
            for day in days:
                log.info("daily_close for %s", day)
                close_day(conn, day, now)

    log.info("backfill done: %s", summary)
    return summary


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Kaizen sales backfill / gap-fill into Postgres")
    parser.add_argument("--from-date", required=True, help="yyyy-MM-dd inclusive start")
    parser.add_argument("--to-date", required=True, help="yyyy-MM-dd exclusive end")
    parser.add_argument("--mode", default="BACKFILL", choices=["BACKFILL", "FILL_GAP", "backfill", "fill_gap"])
    parser.add_argument("--target-orders-per-day", type=int, default=10000)
    parser.add_argument("--daily-close", action="store_true", help="Also run supply daily_close per day")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(
        from_date=_parse_date(args.from_date),
        to_date=_parse_date(args.to_date),
        mode=args.mode,
        target_orders_per_day=args.target_orders_per_day,
        run_daily_close=args.daily_close,
    )


if __name__ == "__main__":
    main()
