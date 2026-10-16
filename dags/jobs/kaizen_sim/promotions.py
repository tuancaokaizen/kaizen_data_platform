"""Voucher code lifecycle, run by the daily close. A voucher series code is single-use: the order stream marks it
USED in the transaction that writes the order. The close expires codes past their validity (CreatedDate +
ExpiryDay) and issues new codes so every running voucher keeps a buffer of unused codes, never beyond its
IssuedQuantity.
Vòng đời mã voucher, do chốt sổ chạy. Mỗi mã series chỉ dùng một lần: stream đơn hàng đánh dấu USED trong
transaction ghi đơn. Chốt sổ cho mã quá hiệu lực (CreatedDate + ExpiryDay) thành EXPIRED và phát thêm mã để mỗi
voucher đang chạy luôn còn một lượng mã chưa dùng, không bao giờ vượt IssuedQuantity.
"""
from datetime import datetime

import psycopg2.extras

from . import rng

SERIES_BUFFER = 50
EXPIRY_DAYS = (30, 60, 90)


def series_to_issue(code: str, issued_quantity: int, active: int, total: int, last_no: int,
                    buffer: int = SERIES_BUFFER) -> list:
    """New (SeriesCode, ExpiryDay) pairs that bring the unused codes of a voucher back to `buffer`.
    Các cặp (SeriesCode, ExpiryDay) mới để số mã chưa dùng của voucher trở lại mức `buffer`.
    """
    n = max(0, min(buffer - active, issued_quantity - total))
    out = []
    for no in range(last_no + 1, last_no + 1 + n):
        series = f"SR_{code}_{no:04d}"
        out.append((series, rng.choice(EXPIRY_DAYS, "voucher-series", series)))
    return out


def expire_series(cur, now: datetime) -> int:
    cur.execute("""UPDATE promotion."VoucherSeries" SET "Status" = 'EXPIRED', "ModifiedDate" = %s
                   WHERE "Status" = 'ACTIVE' AND "ExpiryDay" IS NOT NULL
                     AND "CreatedDate" + make_interval(days => "ExpiryDay") <= %s""", (now, now))
    return cur.rowcount


def issue_series(cur, now: datetime, buffer: int = SERIES_BUFFER) -> int:
    # Only vouchers running now and later; the number suffix continues per voucher
    # Chỉ voucher đang chạy và còn hạn; số thứ tự ở đuôi mã đánh tiếp theo từng voucher
    cur.execute("""SELECT v."Code", v."IssuedQuantity",
                          count(s."SeriesCode") FILTER (WHERE s."Status" = 'ACTIVE'),
                          count(s."SeriesCode"),
                          coalesce(max(substring(s."SeriesCode" FROM '_([0-9]+)$')::int), 0)
                   FROM promotion."Voucher" v
                   LEFT JOIN promotion."VoucherSeries" s ON s."VoucherCode" = v."Code"
                   WHERE v."Status" AND v."StartDate" <= %s AND v."EndDate" > %s
                   GROUP BY 1, 2 ORDER BY 1""", (now, now))
    rows = []
    for code, issued, active, total, last_no in cur.fetchall():
        for series, expiry in series_to_issue(code, issued, active, total, last_no, buffer):
            rows.append((series, code, expiry, now, now))
    if rows:
        psycopg2.extras.execute_values(
            cur, """INSERT INTO promotion."VoucherSeries"
                    ("SeriesCode", "VoucherCode", "ExpiryDay", "Status", "CreatedDate", "ModifiedDate")
                    VALUES %s ON CONFLICT ("SeriesCode") DO NOTHING""",
            rows, template="(%s, %s, %s, 'ACTIVE', %s, %s)", page_size=1000)
    return len(rows)


def refresh_vouchers(cur, now: datetime) -> dict:
    return {"voucher_series_expired": expire_series(cur, now), "voucher_series_issued": issue_series(cur, now)}
