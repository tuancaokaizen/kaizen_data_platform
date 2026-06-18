import re
import random
import os
import sys
import json
import configparser
import time
from datetime import datetime, timedelta

# ── Imports ────────────────────────────────────────────────────────────────
from faker import Faker  # type: ignore
import psycopg2  # type: ignore
from psycopg2.extras import execute_batch  # type: ignore
import requests  # type: ignore

# ── Parse config.ini ────────────────────────────────────────────────────────
config_paths = [
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config.ini')),
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'projects', 'stream-processing', 'config.ini')),
    '/opt/airflow/config.ini',
    '/opt/airflow/dags/config.ini'
]
config = configparser.ConfigParser()
parsed_path = None
for path in config_paths:
    if os.path.exists(path):
        config.read(path)
        parsed_path = path
        break

if parsed_path:
    print(f"Loaded config from: {parsed_path}")
else:
    print("WARNING: config.ini not found in standard paths. Using default fallbacks.")

db_host = os.environ.get('POSTGRES_HOST', config.get('postgres', 'host', fallback='localhost'))
db_port = os.environ.get('POSTGRES_PORT', config.get('postgres', 'port', fallback='5432'))
db_name = os.environ.get('POSTGRES_DB', config.get('postgres', 'database', fallback='kaizen_db'))
db_user = os.environ.get('POSTGRES_USER', config.get('postgres', 'username', fallback='postgres'))
db_pass = os.environ.get('POSTGRES_PASSWORD', config.get('postgres', 'password', fallback='datateam123'))

es_hosts = os.environ.get('ELASTICSEARCH_HOSTS', config.get('elasticsearch', 'hosts', fallback='http://localhost:9200'))


def get_next_month(dt):
    if dt.month == 12:
        return datetime(dt.year + 1, 1, 1)
    else:
        return datetime(dt.year, dt.month + 1, 1)


def bulk_send_to_es(es_hosts, index_name, docs, batch_size=1000):
    if not docs:
        return
    url = f"{es_hosts.rstrip('/')}/{index_name}/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}

    payload_lines = []
    for doc in docs:
        doc_id = None
        if index_name == "employee-salaries":
            doc_id = doc.get("id")
        elif index_name == "promotions":
            doc_id = doc.get("code")
        elif index_name == "vouchers":
            doc_id = doc.get("code")
        elif index_name == "gifts":
            doc_id = doc.get("id")
        elif index_name == "voucher-series":
            doc_id = doc.get("seriesCode")

        processed_doc = {}
        for k, v in doc.items():
            if isinstance(v, datetime):
                processed_doc[k] = int(v.timestamp() * 1000)
            else:
                processed_doc[k] = v

        if doc_id is not None:
            action = {"index": {"_id": str(doc_id)}}
        else:
            action = {"index": {}}

        payload_lines.append(json.dumps(action))
        payload_lines.append(json.dumps(processed_doc))

        if len(payload_lines) >= batch_size * 2:
            payload = "\n".join(payload_lines) + "\n"
            try:
                r = requests.post(url, headers=headers, data=payload)
                r.raise_for_status()
            except Exception as e:
                print(f"Error bulk indexing to ES index {index_name}: {e}")
                raise e
            payload_lines = []

    if payload_lines:
        payload = "\n".join(payload_lines) + "\n"
        try:
            r = requests.post(url, headers=headers, data=payload)
            r.raise_for_status()
            print(f"Successfully bulk indexed {len(docs)} documents to ES index '{index_name}'")
        except Exception as e:
            print(f"Error bulk indexing to ES index {index_name}: {e}")
            raise e


def generate_incremental_data():
    # ── DB Connection ────────────────────────────────────────────────────────
    conn_str = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}"
    print(f"Connecting to PostgreSQL at {db_host}:{db_port}...")
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    now_ms = int(time.time() * 1000)
    now_dt = datetime.fromtimestamp(now_ms / 1000.0)
    current_system_month = datetime(now_dt.year, now_dt.month, 1)

    # 1. Determine target months for salary & promotion increment
    print("Checking existing Employee Salaries for incremental load...")
    cur.execute('SELECT MAX("SalaryMonth") FROM shop."EmployeeSalary";')
    max_month_row = cur.fetchone()

    months = []
    if max_month_row[0] is None:
        # Fallback if bootstrap hasn't run: start from Jan 2026 up to current month
        start_dt = datetime(2026, 1, 1)
        print("No existing salary records found in DB. Starting incremental load from 2026-01-01...")
    else:
        start_dt = get_next_month(max_month_row[0])
        print(f"Last salary record month is {max_month_row[0].strftime('%Y-%m')}.")

    temp_dt = start_dt
    while temp_dt <= current_system_month:
        months.append(temp_dt)
        temp_dt = get_next_month(temp_dt)

    if not months:
        print(
            f"Data is already up to date. Current system month is {current_system_month.strftime('%Y-%m')}. No incremental generation needed.")
        cur.close()
        conn.close()
        return

    print(f"Generating incremental data for months: {[m.strftime('%Y-%m') for m in months]}")

    # 2. Load Active Employees
    cur.execute('SELECT "Id", "ShopCode", "IsManager" FROM shop."Employee" WHERE "Status" = TRUE;')
    employee_docs = [{"id": r[0], "shopCode": r[1], "isManager": r[2]} for r in cur.fetchall()]
    if not employee_docs:
        print("WARNING: No active employees found in shop.Employee table. Skipping salary generation.")

    # 3. Load Promotion products
    cur.execute('SELECT "Id" FROM product."Product" WHERE "IsPromotion" = TRUE;')
    gift_prod_ids = [r[0] for r in cur.fetchall()]
    if not gift_prod_ids:
        gift_prod_ids = ["PROD0191", "PROD0192", "PROD0193", "PROD0194"]

    # 4. Generate Employee Salaries
    salary_docs = []
    if employee_docs:
        print(f"Generating Salaries for {len(employee_docs)} employees...")
        for emp in employee_docs:
            emp_id = emp["id"]
            shop_code = emp["shopCode"]
            is_manager = emp["isManager"]

            # Base salaries
            if is_manager:
                base_salary = random.randint(20, 30) * 1000000
                commission_rate = None
            else:
                base_salary = random.randint(7, 15) * 1000000
                commission_rate = random.randint(1, 5)

            for month_dt in months:
                month_str = month_dt.strftime("%Y%m")
                sal_id = f"SAL_{emp_id}_{month_str}"

                salary_docs.append({
                    "id": sal_id,
                    "employeeId": emp_id,
                    "shopCode": shop_code,
                    "salaryMonth": month_dt,
                    "baseSalary": base_salary,
                    "commissionRate": commission_rate,
                    "createdDate": now_ms,
                    "modifiedDate": now_ms
                })

        # Save to PostgreSQL
        cur.executemany("""
                        INSERT INTO shop."EmployeeSalary" ("Id", "EmployeeId", "ShopCode", "SalaryMonth", "BaseSalary",
                                                           "CommissionRate", "CreatedDate", "ModifiedDate")
                        VALUES (%(id)s, %(employeeId)s, %(shopCode)s, %(salaryMonth)s, %(baseSalary)s,
                                %(commissionRate)s, %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                        UPDATE SET "BaseSalary" = EXCLUDED."BaseSalary", "ModifiedDate" = EXCLUDED."ModifiedDate"
                        """,
                        [{**doc, "salaryMonth": doc["salaryMonth"], "createdDate": now_dt, "modifiedDate": now_dt} for
                         doc in salary_docs])
        conn.commit()

        # Publish to ES
        bulk_send_to_es(es_hosts, "employee-salaries", salary_docs)

    # 5. Generate Incremental Promotions, Vouchers, Gifts & VoucherSeries
    # Get highest index of Promotion, Voucher, Gift
    cur.execute('SELECT "Code" FROM promotion."Promotion" WHERE "Code" LIKE \'PROMO%\' ORDER BY "Code" DESC LIMIT 1;')
    promo_row = cur.fetchone()
    max_promo_idx = int(promo_row[0][5:]) if promo_row else 20

    cur.execute('SELECT "Code" FROM promotion."Voucher" WHERE "Code" LIKE \'VCH%\' ORDER BY "Code" DESC LIMIT 1;')
    v_row = cur.fetchone()
    max_v_idx = int(v_row[0][3:]) if v_row else 60

    cur.execute('SELECT "Id" FROM promotion."Gift" WHERE "Id" LIKE \'GIFT%\' ORDER BY "Id" DESC LIMIT 1;')
    g_row = cur.fetchone()
    max_g_idx = int(g_row[0][4:]) if g_row else 20

    promotion_docs = []
    voucher_docs = []
    gift_docs = []
    voucher_series_docs = []

    amounts = [20000, 50000, 100000]
    min_vals = [100000, 200000, 500000]

    for month_dt in months:
        # Determine duration: from first second of the month to last second of the month
        promo_start_dt = month_dt
        if promo_start_dt.month == 12:
            promo_end_dt = datetime(promo_start_dt.year + 1, 1, 1) - timedelta(seconds=1)
        else:
            promo_end_dt = datetime(promo_start_dt.year, promo_start_dt.month + 1, 1) - timedelta(seconds=1)

        print(
            f"Generating promotions for period {promo_start_dt.strftime('%Y-%m-%d')} to {promo_end_dt.strftime('%Y-%m-%d')}...")

        # A. Voucher promotion
        max_promo_idx += 1
        promo_code1 = f"PROMO{str(max_promo_idx).zfill(4)}"
        promo_name1 = f"Chương trình khuyến mãi giảm giá gia tăng {max_promo_idx}"
        promotion_docs.append({
            "code": promo_code1,
            "name": promo_name1,
            "startDate": int(promo_start_dt.timestamp() * 1000),
            "endDate": int(promo_end_dt.timestamp() * 1000),
            "status": True,
            "type": "VOUCHER",
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })

        for v_idx in range(1, 7):
            max_v_idx += 1
            v_code = f"VCH{str(max_v_idx).zfill(4)}"
            amt = random.choice(amounts)
            min_val = random.choice(min_vals)
            voucher_docs.append({
                "code": v_code,
                "name": f"Voucher giảm {amt // 1000}k đơn từ {min_val // 1000}k ({promo_start_dt.strftime('%m/%Y')})",
                "promotionCode": promo_code1,
                "voucherAmount": amt,
                "minOrderValue": min_val,
                "startDate": int(promo_start_dt.timestamp() * 1000),
                "endDate": int(promo_end_dt.timestamp() * 1000),
                "issuedQuantity": 10000,
                "status": True,
                "createdDate": now_ms,
                "modifiedDate": now_ms
            })

        # B. Gift promotion
        max_promo_idx += 1
        promo_code2 = f"PROMO{str(max_promo_idx).zfill(4)}"
        promo_name2 = f"Chương trình quà tặng tri ân gia tăng {max_promo_idx - 10}"
        promotion_docs.append({
            "code": promo_code2,
            "name": promo_name2,
            "startDate": int(promo_start_dt.timestamp() * 1000),
            "endDate": int(promo_end_dt.timestamp() * 1000),
            "status": True,
            "type": "GIFT",
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })

        for g_idx in range(1, 3):
            max_g_idx += 1
            gift_id = f"GIFT{str(max_g_idx).zfill(4)}"
            prod_id = gift_prod_ids[max_g_idx % len(gift_prod_ids)]
            gift_docs.append({
                "id": gift_id,
                "productId": prod_id,
                "promotionCode": promo_code2,
                "promotionQuantity": 1000,
                "status": True,
                "createdDate": now_ms,
                "modifiedDate": now_ms
            })

    # Save Promotions to PostgreSQL
    cur.executemany("""
                    INSERT INTO promotion."Promotion" ("Code", "Name", "StartDate", "EndDate", "Status", "Type",
                                                       "CreatedDate", "ModifiedDate")
                    VALUES (%(code)s, %(name)s, %(startDate)s, %(endDate)s, %(status)s, %(type)s, %(createdDate)s,
                            %(modifiedDate)s) ON CONFLICT ("Code") DO
                    UPDATE SET "Name" = EXCLUDED."Name", "Status" = EXCLUDED."Status", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "startDate": datetime.fromtimestamp(doc["startDate"] / 1000.0),
                           "endDate": datetime.fromtimestamp(doc["endDate"] / 1000.0), "createdDate": now_dt,
                           "modifiedDate": now_dt} for doc in promotion_docs])

    # Save Vouchers to PostgreSQL
    cur.executemany("""
                    INSERT INTO promotion."Voucher" ("Code", "Name", "PromotionCode", "VoucherAmount", "MinOrderValue",
                                                     "StartDate", "EndDate", "IssuedQuantity", "Status", "CreatedDate",
                                                     "ModifiedDate")
                    VALUES (%(code)s, %(name)s, %(promotionCode)s, %(voucherAmount)s, %(minOrderValue)s, %(startDate)s,
                            %(endDate)s, %(issuedQuantity)s, %(status)s, %(createdDate)s,
                            %(modifiedDate)s) ON CONFLICT ("Code") DO
                    UPDATE SET "Name" = EXCLUDED."Name", "Status" = EXCLUDED."Status", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "startDate": datetime.fromtimestamp(doc["startDate"] / 1000.0),
                           "endDate": datetime.fromtimestamp(doc["endDate"] / 1000.0), "createdDate": now_dt,
                           "modifiedDate": now_dt} for doc in voucher_docs])

    # Generate VoucherSeries (50 per voucher)
    for v in voucher_docs:
        v_code = v["code"]
        for idx in range(1, 51):
            series_code = f"SR_{v_code}_{str(idx).zfill(4)}"
            voucher_series_docs.append({
                "seriesCode": series_code,
                "voucherCode": v_code,
                "expiryDay": random.choice([30, 60, 90]),
                "status": "ACTIVE",
                "createdDate": now_ms,
                "modifiedDate": now_ms
            })

    # Save VoucherSeries to PostgreSQL
    cur.executemany("""
                    INSERT INTO promotion."VoucherSeries" ("SeriesCode", "VoucherCode", "ExpiryDay", "Status",
                                                           "CreatedDate", "ModifiedDate")
                    VALUES (%(seriesCode)s, %(voucherCode)s, %(expiryDay)s, %(status)s, %(createdDate)s,
                            %(modifiedDate)s) ON CONFLICT ("SeriesCode") DO
                    UPDATE SET "Status" = EXCLUDED."Status", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in voucher_series_docs])

    # Save Gifts to PostgreSQL
    cur.executemany("""
                    INSERT INTO promotion."Gift" ("Id", "ProductId", "PromotionCode", "PromotionQuantity", "Status",
                                                  "CreatedDate", "ModifiedDate")
                    VALUES (%(id)s, %(productId)s, %(promotionCode)s, %(promotionQuantity)s, %(status)s,
                            %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                    UPDATE SET "Status" = EXCLUDED."Status", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in gift_docs])
    conn.commit()

    # Group vouchers, series, and gifts under their parent promotion to match the consolidated ES Promotions Schema
    unified_promotion_docs = []
    for p in promotion_docs:
        p_code = p["code"]
        p_vouchers = []
        for v in voucher_docs:
            if v["promotionCode"] == p_code:
                # Find series for this voucher
                v_series = []
                for s in voucher_series_docs:
                    if s["voucherCode"] == v["code"]:
                        v_series.append({
                            "seriesCode": s["seriesCode"],
                            "expiryDay": s["expiryDay"],
                            "status": s["status"],
                            "createdDate": s["createdDate"],
                            "modifiedDate": s["modifiedDate"]
                        })
                p_vouchers.append({
                    "code": v["code"],
                    "name": v["name"],
                    "voucherAmount": v["voucherAmount"],
                    "minOrderValue": v["minOrderValue"],
                    "startDate": v["startDate"],
                    "endDate": v["endDate"],
                    "issuedQuantity": v["issuedQuantity"],
                    "status": v["status"],
                    "createdDate": v["createdDate"],
                    "modifiedDate": v["modifiedDate"],
                    "voucherSeries": v_series
                })

        p_gifts = []
        for g in gift_docs:
            if g["promotionCode"] == p_code:
                p_gifts.append({
                    "id": g["id"],
                    "productId": g["productId"],
                    "promotionQuantity": g["promotionQuantity"],
                    "status": g["status"],
                    "createdDate": g["createdDate"],
                    "modifiedDate": g["modifiedDate"]
                })

        unified_promotion_docs.append({
            "code": p["code"],
            "name": p["name"],
            "startDate": p["startDate"],
            "endDate": p["endDate"],
            "status": p["status"],
            "type": p["type"],
            "createdDate": p["createdDate"],
            "modifiedDate": p["modifiedDate"],
            "vouchers": p_vouchers,
            "gifts": p_gifts
        })

    # Publish Promotions (which now nests Vouchers, Series, and Gifts) to ES
    bulk_send_to_es(es_hosts, "promotions", unified_promotion_docs)

    cur.close()
    conn.close()
    print("Incremental Salaries and Promotions successfully generated and synced!")


if __name__ == "__main__":
    generate_incremental_data()
