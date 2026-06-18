import re
import random
import os
import sys
import json
import configparser
import time
from datetime import datetime

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

# ── Raw Ward Data ───────────────────────────────────────────────────────────
raw_data = """1Đặc khu Côn Đảo2Phường An Đông3Phường An Hội Đông4Phường An Hội Tây5Phường An Khánh6Phường An Lạc7Phường An Nhơn8Phường An Phú9Phường An Phú Đông10Phường Bà Rịa11Phường Bàn Cờ12Phường Bảy Hiền13Phường Bến Cát14Phường Bến Thành15Phường Bình Cơ16Phường Bình Dương17Phường Bình Đông18Phường Bình Hòa19Phường Bình Hưng Hòa20Phường Bình Lợi Trung21Phường Bình Phú22Phường Bình Quới23Phường Bình Tân24Phường Bình Tây25Phường Bình Thạnh26Phường Bình Thới27Phường Bình Tiên28Phường Bình Trị Đông29Phường Bình Trưng30Phường Cát Lái31Phường Cầu Kiệu32Phường Cầu Ông Lãnh33Phường Chánh Hiệp34Phường Chánh Hưng35Phường Chánh Phú Hòa36Phường Chợ Lớn37Phường Chợ Quán38Phường Dĩ An39Phường Diên Hồng40Phường Đông Hòa41Phường Đông Hưng Thuận42Phường Đức Nhuận43Phường Gia Định44Phường Gò Vấp45Phường Hạnh Thông46Phường Hiệp Bình47Phường Hòa Bình48Phường Hòa Hưng49Phường Hòa Lợi50Phường Khánh Hội51Phường Lái Thiêu52Phường Linh Xuân53Phường Long Bình54Phường Long Hương55Phường Long Nguyên56Phường Long Phước57Phường Long Trường58Phường Minh Phụng59Phường Nhiêu Lộc60Phường Phú An61Phường Phú Định62Phường Phú Lâm63Phường Phú Lợi64Phường Phú Mỹ65Phường Phú Nhuận66Phường Phú Thạnh67Phường Phú Thọ68Phường Phú Thọ Hòa69Phường Phú Thuận70Phường Phước Long71Phường Phước Thắng72Phường Rạch Dừa73Phường Sài Gòn74Phường Tam Bình75Phường Tam Long76Phường Tam Thắng77Phường Tăng Nhơn Phú78Phường Tân Bình79Phường Tân Định80Phường Tân Đông Hiệp81Phường Tân Hải82Phường Tân Hiệp83Phường Tân Hòa84Phường Tân Hưng85Phường Tân Khánh86Phường Tân Mỹ87Phường Tân Phú88Phường Tân Phước89Phường Tân Sơn90Phường Tân Sơn Hòa91Phường Tân Sơn Nhất92Phường Tân Sơn Nhì93Phường Tân Tạo94Phường Tân Thành95Phường Tân Thới Hiệp96Phường Tân Thuận97Phường Tân Uyên98Phường Tây Nam99Phường Tây Thạnh100Phường Thạnh Mỹ Tây101Phường Thông Tây Hội102Phường Thới An103Phường Thới Hòa104Phường Thủ Dầu Một105Phường Thủ Đức106Phường Thuận An107Phường Thuận Giao108Phường Trung Mỹ Tây109Phường Vĩnh Hội110Phường Vĩnh Tân111Phường Vũng Tàu112Phường Vườn Lài113Phường Xóm Chiếu114Phường Xuân Hòa115Xã An Long116Xã An Nhơn Tây117Xã An Thới Đông118Xã Bà Điểm119Xã Bàu Bàng120Xã Bàu Lâm121Xã Bắc Tân Uyên122Xã Bình Chánh123Xã Bình Châu124Xã Bình Giã125Xã Bình Hưng126Xã Bình Khánh127Xã Bình Lợi128Xã Bình Mỹ129Xã Cần Giờ130Xã Châu Đức131Xã Châu Pha132Xã Củ Chi133Xã Dầu Tiếng134Xã Đất Đỏ135Xã Đông Thạnh136Xã Hiệp Phước137Xã Hòa Hiệp138Xã Hòa Hội139Xã Hóc Môn140Xã Hồ Tràm141Xã Hưng Long142Xã Kim Long143Xã Long Điền144Xã Long Hải145Xã Long Hòa146Xã Long Sơn147Xã Minh Thạnh148Xã Ngãi Giao149Xã Nghĩa Thành150Xã Nhà Bè151Xã Nhuận Đức152Xã Phú Giáo153Xã Phú Hòa Đông154Xã Phước Hải155Xã Phước Hòa156Xã Phước Thành157Xã Tân An Hội158Xã Tân Nhựt159Xã Tân Vĩnh Lộc160Xã Thái Mỹ161Xã Thanh An162Xã Thạnh An163Xã Thường Tân164Xã Trừ Văn Thố165Xã Vĩnh Lộc166Xã Xuân Sơn167Xã Xuân Thới Sơn168Xã Xuyên Mộc"""

ward_list = [w.strip() for w in re.findall(r'[^\d]+', raw_data) if w.strip()]
ward_ids = [f"W{str(i + 1).zfill(3)}" for i in range(len(ward_list))]
ward_map = dict(zip(ward_ids, ward_list))

# ── Supplement lists ────────────────────────────────────────────────────────
SUPPLEMENT_BRANDS = [
    'Blackmores (Úc)', 'Healthy Care (Úc)', 'DHC (Nhật Bản)', 'Orihiro (Nhật Bản)',
    'Shiseido (Nhật Bản)', 'Nature Bounty (Mỹ)', 'Kirkland Signature (Mỹ)',
    'Solgar (Mỹ)', 'Swisse (Úc)', 'Sanofi (Pháp)', 'Meiji (Nhật Bản)',
    'Fancl (Nhật Bản)', 'Puritan Pride (Mỹ)', 'Now Foods (Mỹ)',
    'Bio-Island (Úc)', 'Traphaco (Việt Nam)', 'DHG Pharma (Việt Nam)',
    'Sao Thái Dương (Việt Nam)', 'Bidiphar (Việt Nam)', 'GNC (Mỹ)'
]

SUPPLEMENT_FUNCTIONS = [
    'Bổ sung Vitamin', 'Hỗ trợ xương khớp', 'Tăng cường đề kháng',
    'Bổ mắt', 'Hỗ trợ tiêu hóa', 'Đẹp da & Tóc', 'Bổ não',
    'Hỗ trợ giấc ngủ', 'Giảm cân an toàn', 'Tăng cường sinh lực',
    'Hỗ trợ gan', 'Canxi cho bé', 'DHA cho bà bầu'
]

MEDICINE_BASES = [
    "Paracetamol", "Ibuprofen", "Amoxicillin", "Berberin", "Cetirizine",
    "Loratadine", "Salbutamol", "Metformin", "Atorvastatin", "Omeprazole",
    "Esomeprazole", "Panadol Extra", "Decolgen", "Hapacol", "Smecta",
    "Gaviscon", "Strepsils", "Eugica", "Berberal", "Loperamide"
]
MEDICINE_BRANDS = ["DHG", "Traphaco", "Imexpharm", "Pymepharco", "Sanofi", "Pharmedic", "AstraZeneca"]

# Degrees constraint: Trung cấp, Đại học, Cao đẳng, Sau đại học
DEGREES = ["Trung cấp", "Đại học", "Cao đẳng", "Sau đại học"]

# ── Faker Initialization ───────────────────────────────────────────────────
fake = Faker('vi_VN')


def clean_phone(raw_phone):
    raw_p = re.sub(r'\D', '', raw_phone)
    if raw_p.startswith('84'):
        return '0' + raw_p[2:]
    elif not raw_p.startswith('0'):
        return '0' + raw_p
    else:
        return raw_p


def bulk_send_to_es(es_hosts, index_name, docs, batch_size=1000):
    if not docs:
        return
    url = f"{es_hosts.rstrip('/')}/{index_name}/_bulk"
    headers = {"Content-Type": "application/x-ndjson"}

    payload_lines = []
    for doc in docs:
        doc_id = None
        if index_name == "geography":
            doc_id = doc.get("id")
        elif index_name == "products":
            doc_id = doc.get("id")
        elif index_name == "product-medicines":
            doc_id = doc.get("productId")
        elif index_name == "product-supplements":
            doc_id = doc.get("productId")
        elif index_name == "shops":
            doc_id = doc.get("code")
        elif index_name == "employees":
            doc_id = doc.get("id")
        elif index_name == "customers":
            doc_id = doc.get("id")
        elif index_name == "employee-salaries":
            doc_id = doc.get("id")
        elif index_name == "promotions":
            doc_id = doc.get("code")
        elif index_name == "vouchers":
            doc_id = doc.get("code")
        elif index_name == "gifts":
            doc_id = doc.get("id")
        elif index_name == "voucher-series":
            doc_id = doc.get("seriesCode")
        elif index_name == "payment-types":
            doc_id = doc.get("type")
        elif index_name == "payment-methods":
            doc_id = doc.get("id")

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


def init_es_indices(es_hosts):
    mappings_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', '..', 'schema_manager', 'elasticsearch'))
    if not os.path.exists(mappings_dir):
        # Fallback path for Airflow scheduler container context
        mappings_dir = '/opt/airflow/dags/schema_manager/elasticsearch'
        if not os.path.exists(mappings_dir):
            print("WARNING: Elasticsearch mappings directory not found. Skipping manual index initialization.")
            return

    indices = [
        "geography", "products", "product-medicines", "product-supplements",
        "shops", "employees", "employee-salaries",
        "customers", "promotions", "payment-types", "payment-methods",
        "order-bundles", "payment-bundles"
    ]

    for idx in indices:
        mapping_file = os.path.join(mappings_dir, f"{idx}.json")
        if os.path.exists(mapping_file):
            try:
                with open(mapping_file, 'r', encoding='utf-8') as f:
                    mapping_data = json.load(f)

                check_url = f"{es_hosts.rstrip('/')}/{idx}"
                r = requests.get(check_url)
                if r.status_code == 404:
                    print(f"Index '{idx}' not found. Creating index with mapping from {idx}.json...")
                    create_r = requests.put(check_url, json=mapping_data)
                    create_r.raise_for_status()
                    print(f"✔ Index '{idx}' successfully created!")
                else:
                    print(f"Index '{idx}' already exists.")
            except Exception as e:
                print(f"Error initializing ES index {idx}: {e}")
        else:
            print(f"WARNING: Mapping file {mapping_file} not found.")


def generate_all_master_data():
    # ── Initialize ES Indices with correct mappings ──────────────────────────
    init_es_indices(es_hosts)

    # ── DB Connection ────────────────────────────────────────────────────────
    conn_str = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}"
    print(f"Connecting to PostgreSQL at {db_host}:{db_port}...")
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    now_ms = int(time.time() * 1000)
    now_dt = datetime.fromtimestamp(now_ms / 1000.0)

    # 1. Geography
    print("Generating Geography (168 Wards)...")
    geography_docs = []
    for w_id, w_name in ward_map.items():
        geography_docs.append({
            "id": w_id,
            "name": w_name,
            "population": random.randint(5000, 45000),
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })

    # Save to PostgreSQL
    cur.executemany("""
                    INSERT INTO public."Geography" ("Id", "Name", "Population", "CreatedDate", "ModifiedDate")
                    VALUES (%(id)s, %(name)s, %(population)s, %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                    UPDATE SET "Name" = EXCLUDED."Name", "Population" = EXCLUDED."Population", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in geography_docs])
    conn.commit()

    # Publish to ES
    bulk_send_to_es(es_hosts, "geography", geography_docs)

    # 2. Products
    print("Generating Products (200 products)...")
    product_docs = []
    medicine_sub_docs = []
    supplement_sub_docs = []

    # 100 Medicines (Type: medicine, IsPromotion: False)
    for i in range(1, 101):
        prod_id = f"PROD{str(i).zfill(4)}"
        base = random.choice(MEDICINE_BASES)
        brand = random.choice(MEDICINE_BRANDS)
        unit = random.choice(["Hộp", "Chai", "Vỉ", "Viên"])
        name = f"Thuốc {base} {brand} {random.choice(['500mg', '250mg', '10ml', '100ml'])}"
        stock = random.randint(5000, 150000)
        retail = int(stock * random.uniform(1.1, 1.3))
        tax = 5
        product_docs.append({
            "id": prod_id,
            "name": name,
            "type": "medicine",
            "unit": unit,
            "stockPrice": stock,
            "retailPrice": retail,
            "taxRate": tax,
            "isPromotion": False,
            "status": True,
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })
        specialty = random.choice(["Tim mạch", "Tiêu hóa", "Hô hấp", "Thần kinh", "Da liễu", "Cơ xương khớp"])
        medicine_sub_docs.append({
            "productId": prod_id,
            "indication": f"Hỗ trợ điều trị bệnh về {specialty.lower()} và giảm các triệu chứng liên quan.",
            "isPrescription": random.choice([True, False]),
            "specialtyDisease": specialty
        })

    # 80 Supplements (Type: supplement, IsPromotion: False)
    for i in range(101, 181):
        prod_id = f"PROD{str(i).zfill(4)}"
        func = random.choice(SUPPLEMENT_FUNCTIONS)
        brand = random.choice(SUPPLEMENT_BRANDS)
        unit = random.choice(["Hộp", "Chai", "Viên"])
        name = f"{func} {brand} {random.choice(['30 viên', '60 viên', '90 viên', '100ml'])}"
        stock = random.randint(15000, 300000)
        retail = int(stock * random.uniform(1.15, 1.35))
        tax = random.choice([8, 10])
        product_docs.append({
            "id": prod_id,
            "name": name,
            "type": "supplement",
            "unit": unit,
            "stockPrice": stock,
            "retailPrice": retail,
            "taxRate": tax,
            "isPromotion": False,
            "status": True,
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })
        supplement_sub_docs.append({
            "productId": prod_id,
            "primaryFunction": func,
            "brand": brand
        })

    # 20 Gift Supplements (Type: supplement, IsPromotion: True)
    for i in range(181, 201):
        prod_id = f"PROD{str(i).zfill(4)}"
        func = random.choice(SUPPLEMENT_FUNCTIONS)
        brand = random.choice(SUPPLEMENT_BRANDS)
        unit = random.choice(["Hộp", "Viên"])
        name = f"[Quà Tặng] {func} {brand} {random.choice(['10 viên', '20 viên', 'vỉ thử'])}"
        stock = random.randint(2000, 20000)
        retail = 0
        tax = 0
        product_docs.append({
            "id": prod_id,
            "name": name,
            "type": "supplement",
            "unit": unit,
            "stockPrice": stock,
            "retailPrice": retail,
            "taxRate": tax,
            "isPromotion": True,
            "status": True,
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })
        supplement_sub_docs.append({
            "productId": prod_id,
            "primaryFunction": func,
            "brand": brand
        })

    # Save to PostgreSQL
    cur.executemany("""
                    INSERT INTO product."Product" ("Id", "Name", "Type", "Unit", "StockPrice", "RetailPrice", "TaxRate",
                                                   "IsPromotion", "Status", "CreatedDate", "ModifiedDate")
                    VALUES (%(id)s, %(name)s, %(type)s, %(unit)s, %(stockPrice)s, %(retailPrice)s, %(taxRate)s,
                            %(isPromotion)s, %(status)s, %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                    UPDATE SET "Name" = EXCLUDED."Name", "StockPrice" = EXCLUDED."StockPrice", "RetailPrice" = EXCLUDED."RetailPrice", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in product_docs])

    cur.executemany("""
                    INSERT INTO product."ProductMedicine" ("ProductId", "Indication", "IsPrescription", "SpecialtyDisease")
                    VALUES (%(productId)s, %(indication)s, %(isPrescription)s,
                            %(specialtyDisease)s) ON CONFLICT ("ProductId") DO
                    UPDATE SET "Indication" = EXCLUDED."Indication", "IsPrescription" = EXCLUDED."IsPrescription", "SpecialtyDisease" = EXCLUDED."SpecialtyDisease"
                    """, medicine_sub_docs)

    cur.executemany("""
                    INSERT INTO product."ProductSupplement" ("ProductId", "PrimaryFunction", "Brand")
                    VALUES (%(productId)s, %(primaryFunction)s, %(brand)s) ON CONFLICT ("ProductId") DO
                    UPDATE SET "PrimaryFunction" = EXCLUDED."PrimaryFunction", "Brand" = EXCLUDED."Brand"
                    """, supplement_sub_docs)

    conn.commit()

    # Publish to ES
    bulk_send_to_es(es_hosts, "products", product_docs)
    bulk_send_to_es(es_hosts, "product-medicines", medicine_sub_docs)
    bulk_send_to_es(es_hosts, "product-supplements", supplement_sub_docs)

    # 3. Shops (50 shops)
    print("Generating Shops (50 shops)...")
    shop_docs = []
    for i in range(1, 51):
        code = f"SHOP{str(i).zfill(4)}"
        name = f"Nhà thuốc Kaizen số {i}"
        w_id = random.choice(ward_ids)
        w_name = ward_map[w_id]
        addr = fake.address().replace('\n', ', ')
        shop_docs.append({
            "code": code,
            "name": name,
            "address": addr,
            "wardId": w_id,
            "managerId": None,
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })

    # Save to PostgreSQL (with ManagerId = None initially)
    cur.executemany("""
                    INSERT INTO shop."Shop" ("Code", "Name", "Address", "WardId", "ManagerId", "CreatedDate",
                                             "ModifiedDate")
                    VALUES (%(code)s, %(name)s, %(address)s, %(wardId)s, %(managerId)s, %(createdDate)s,
                            %(modifiedDate)s) ON CONFLICT ("Code") DO
                    UPDATE SET "Name" = EXCLUDED."Name", "Address" = EXCLUDED."Address", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in shop_docs])
    conn.commit()

    # 4. Employees (1000 employees total, 20 per shop)
    print("Generating Employees (1000 employees)...")
    employee_docs = []
    shop_managers = {}  # shop_code -> manager_emp_id

    emp_counter = 1
    for shop in shop_docs:
        shop_code = shop["code"]

        # 1 Manager
        manager_id = f"EMP{str(emp_counter).zfill(4)}"
        gender = random.choice(['Nam', 'Nữ'])
        try:
            manager_name = fake.name_male() if gender == 'Nam' else fake.name_female()
        except AttributeError:
            manager_name = fake.name()

        employee_docs.append({
            "id": manager_id,
            "name": manager_name,
            "degree": "Đại học",
            "phone": clean_phone(fake.phone_number()),
            "address": fake.address().replace('\n', ', '),
            "status": True,
            "shopCode": shop_code,
            "isManager": True,
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })
        shop_managers[shop_code] = manager_id
        emp_counter += 1

        # 19 Staff
        for _ in range(19):
            emp_id = f"EMP{str(emp_counter).zfill(4)}"
            gender = random.choice(['Nam', 'Nữ'])
            try:
                emp_name = fake.name_male() if gender == 'Nam' else fake.name_female()
            except AttributeError:
                emp_name = fake.name()
            degree = random.choice(DEGREES)
            employee_docs.append({
                "id": emp_id,
                "name": emp_name,
                "degree": degree,
                "phone": clean_phone(fake.phone_number()),
                "address": fake.address().replace('\n', ', '),
                "status": True,
                "shopCode": shop_code,
                "isManager": False,
                "createdDate": now_ms,
                "modifiedDate": now_ms
            })
            emp_counter += 1

    # Save to PostgreSQL
    cur.executemany("""
                    INSERT INTO shop."Employee" ("Id", "Name", "Degree", "Phone", "Address", "Status", "ShopCode",
                                                 "IsManager", "CreatedDate", "ModifiedDate")
                    VALUES (%(id)s, %(name)s, %(degree)s, %(phone)s, %(address)s, %(status)s, %(shopCode)s,
                            %(isManager)s, %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                    UPDATE SET "Name" = EXCLUDED."Name", "Status" = EXCLUDED."Status", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in employee_docs])
    conn.commit()

    # Publish Employees to ES
    bulk_send_to_es(es_hosts, "employees", employee_docs)

    # 4b. EmployeeSalary (Bootstrap: Jan - Jun 2026)
    print("Generating initial Employee Salaries (Jan - Jun 2026)...")
    months = [datetime(2026, m, 1) for m in range(1, 7)]

    salary_docs = []
    for emp in employee_docs:
        emp_id = emp["id"]
        shop_code = emp["shopCode"]
        is_manager = emp["isManager"]

        # Mức lương cơ sở: Manager: 20M-30M, Employee: 7M-15M
        if is_manager:
            base_salary = random.randint(20, 30) * 1000000
            commission_rate = None
        else:
            base_salary = random.randint(7, 15) * 1000000
            commission_rate = random.randint(1, 5)  # 1% - 5% hoa hồng

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

    # Lưu EmployeeSalary vào Postgres
    cur.executemany("""
                    INSERT INTO shop."EmployeeSalary" ("Id", "EmployeeId", "ShopCode", "SalaryMonth", "BaseSalary",
                                                       "CommissionRate", "CreatedDate", "ModifiedDate")
                    VALUES (%(id)s, %(employeeId)s, %(shopCode)s, %(salaryMonth)s, %(baseSalary)s, %(commissionRate)s,
                            %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                    UPDATE SET "BaseSalary" = EXCLUDED."BaseSalary", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """,
                    [{**doc, "salaryMonth": doc["salaryMonth"], "createdDate": now_dt, "modifiedDate": now_dt} for doc
                     in salary_docs])
    conn.commit()

    # Publish Employee Salaries to ES
    bulk_send_to_es(es_hosts, "employee-salaries", salary_docs)

    # 5. Update Shop Managers in PostgreSQL & Publish updated Shops to Kafka
    print("Updating Shop Manager IDs and publishing shops...")
    for shop in shop_docs:
        shop_code = shop["code"]
        shop["managerId"] = shop_managers[shop_code]
        shop["modifiedDate"] = int(time.time() * 1000)

    # Save updates to PostgreSQL
    cur.executemany("""
                    UPDATE shop."Shop"
                    SET "ManagerId"    = %(managerId)s,
                        "ModifiedDate" = %(modifiedDate)s
                    WHERE "Code" = %(code)s
                    """, [{**doc, "modifiedDate": now_dt} for doc in shop_docs])
    conn.commit()

    # Publish updated shops to ES
    bulk_send_to_es(es_hosts, "shops", shop_docs)

    # 6. Customers (500,000 customers via Faker pool)
    print("Generating Faker pools for customers...")
    pool_names_male = []
    pool_names_female = []
    pool_addresses = []

    for _ in range(15000):
        try:
            pool_names_male.append(fake.name_male())
        except AttributeError:
            pool_names_male.append(fake.name())

    for _ in range(15000):
        try:
            pool_names_female.append(fake.name_female())
        except AttributeError:
            pool_names_female.append(fake.name())

    for _ in range(30000):
        pool_addresses.append(fake.address().replace('\n', ', '))

    print("Generating and saving Customers (500,000 customers)...")
    customer_batch = []
    for i in range(1, 500001):
        cust_id = f"CUST{str(i).zfill(6)}"
        gender = random.choice(['M', 'F'])
        name = random.choice(pool_names_male) if gender == 'M' else random.choice(pool_names_female)
        phone = f"0{random.choice([3, 5, 7, 8, 9])}{random.randint(10000000, 99999999)}"
        w_id = random.choice(ward_ids)
        addr = random.choice(pool_addresses)
        yob = random.randint(1950, 2008)

        customer_doc = {
            "id": cust_id,
            "fullName": name,
            "phone": phone,
            "wardId": w_id,
            "address": addr,
            "yearOfBirth": yob,
            "gender": gender,
            "createdDate": now_ms,
            "modifiedDate": now_ms
        }
        customer_batch.append(customer_doc)

        if len(customer_batch) >= 10000:
            # Save to PostgreSQL
            cur.executemany("""
                            INSERT INTO customer."Customer" ("Id", "FullName", "Phone", "WardId", "Address",
                                                             "YearOfBirth", "Gender", "CreatedDate", "ModifiedDate")
                            VALUES (%(id)s, %(fullName)s, %(phone)s, %(wardId)s, %(address)s, %(yearOfBirth)s,
                                    %(gender)s, %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                            UPDATE SET "FullName" = EXCLUDED."FullName", "Phone" = EXCLUDED."Phone", "ModifiedDate" = EXCLUDED."ModifiedDate"
                            """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in customer_batch])
            conn.commit()

            # Publish to ES
            bulk_send_to_es(es_hosts, "customers", customer_batch)

            print(f"  Processed {i}/500000 customers...")
            customer_batch = []

    if customer_batch:
        cur.executemany("""
                        INSERT INTO customer."Customer" ("Id", "FullName", "Phone", "WardId", "Address", "YearOfBirth",
                                                         "Gender", "CreatedDate", "ModifiedDate")
                        VALUES (%(id)s, %(fullName)s, %(phone)s, %(wardId)s, %(address)s, %(yearOfBirth)s, %(gender)s,
                                %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                        UPDATE SET "FullName" = EXCLUDED."FullName", "Phone" = EXCLUDED."Phone", "ModifiedDate" = EXCLUDED."ModifiedDate"
                        """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in customer_batch])
        conn.commit()

        # Publish to ES
        bulk_send_to_es(es_hosts, "customers", customer_batch)

    # 7. Promotions, Vouchers, Gifts (Bootstrap Load: Jan - Dec 2026)
    print("Generating initial Promotions, Vouchers, and Gifts (Jan - Dec 2026)...")
    promotion_docs = []
    voucher_docs = []
    gift_docs = []
    voucher_series_docs = []

    # Truy vấn các sản phẩm làm quà tặng có IsPromotion = True
    cur.execute('SELECT "Id" FROM product."Product" WHERE "IsPromotion" = TRUE;')
    gift_prod_ids = [r[0] for r in cur.fetchall()]
    if not gift_prod_ids:
        gift_prod_ids = ["PROD0191", "PROD0192", "PROD0193", "PROD0194"]

    # === Chạy lần đầu: Sinh 20 promotions ban đầu ===
    # 10 Voucher-based promotions (1-10)
    for i in range(1, 11):
        promo_code = f"PROMO{str(i).zfill(4)}"
        promo_name = f"Chương trình khuyến mãi giảm giá {i}"
        promotion_docs.append({
            "code": promo_code,
            "name": promo_name,
            "startDate": int(datetime(2026, 1, 1).timestamp() * 1000),
            "endDate": int(datetime(2026, 12, 31).timestamp() * 1000),
            "status": True,
            "type": "VOUCHER",
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })

        # 6 vouchers per promotion
        amounts = [20000, 50000, 100000]
        min_vals = [100000, 200000, 500000]
        for v_idx in range(1, 7):
            v_code = f"VCH{str((i - 1) * 6 + v_idx).zfill(4)}"
            amt = random.choice(amounts)
            min_val = random.choice(min_vals)
            voucher_docs.append({
                "code": v_code,
                "name": f"Voucher giảm {amt // 1000}k đơn từ {min_val // 1000}k",
                "promotionCode": promo_code,
                "voucherAmount": amt,
                "minOrderValue": min_val,
                "startDate": int(datetime(2026, 1, 1).timestamp() * 1000),
                "endDate": int(datetime(2026, 12, 31).timestamp() * 1000),
                "issuedQuantity": 10000,
                "status": True,
                "createdDate": now_ms,
                "modifiedDate": now_ms
            })

    # 10 Gift-based promotions (11-20)
    for i in range(11, 21):
        promo_code = f"PROMO{str(i).zfill(4)}"
        promo_name = f"Chương trình quà tặng tri ân {i - 10}"
        promotion_docs.append({
            "code": promo_code,
            "name": promo_name,
            "startDate": int(datetime(2026, 1, 1).timestamp() * 1000),
            "endDate": int(datetime(2026, 12, 31).timestamp() * 1000),
            "status": True,
            "type": "GIFT",
            "createdDate": now_ms,
            "modifiedDate": now_ms
        })

        # 2 gifts per promotion
        for g_idx in range(1, 3):
            gift_id = f"GIFT{str((i - 11) * 2 + g_idx).zfill(4)}"
            prod_id = random.choice(gift_prod_ids)
            gift_docs.append({
                "id": gift_id,
                "productId": prod_id,
                "promotionCode": promo_code,
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

    # Generate VoucherSeries
    print(f"Generating Voucher Series (50 series per voucher, total {len(voucher_docs) * 50} series)...")
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
    # 8. Payment Master Data (PaymentType and PaymentMethod)
    print("Generating Payment Master Data (PaymentType & PaymentMethod)...")
    payment_type_docs = [
        {"type": 1, "name": "Phiếu thu", "createdDate": now_ms, "modifiedDate": now_ms},
        {"type": 2, "name": "Phiếu chi", "createdDate": now_ms, "modifiedDate": now_ms}
    ]
    payment_method_docs = [
        {"id": 1, "name": "Tiền mặt", "createdDate": now_ms, "modifiedDate": now_ms},
        {"id": 2, "name": "Thẻ ngân hàng", "createdDate": now_ms, "modifiedDate": now_ms},
        {"id": 3, "name": "Ví điện tử (MoMo/ZaloPay)", "createdDate": now_ms, "modifiedDate": now_ms}
    ]

    # Save to PostgreSQL
    cur.executemany("""
                    INSERT INTO payment."PaymentType" ("Type", "Name", "CreatedDate", "ModifiedDate")
                    VALUES (%(type)s, %(name)s, %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Type") DO
                    UPDATE SET "Name" = EXCLUDED."Name", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in payment_type_docs])

    cur.executemany("""
                    INSERT INTO payment."PaymentMethod" ("Id", "Name", "CreatedDate", "ModifiedDate")
                    VALUES (%(id)s, %(name)s, %(createdDate)s, %(modifiedDate)s) ON CONFLICT ("Id") DO
                    UPDATE SET "Name" = EXCLUDED."Name", "ModifiedDate" = EXCLUDED."ModifiedDate"
                    """, [{**doc, "createdDate": now_dt, "modifiedDate": now_dt} for doc in payment_method_docs])

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

    # Publish Payment Master Data to ES
    bulk_send_to_es(es_hosts, "payment-types", payment_type_docs)
    bulk_send_to_es(es_hosts, "payment-methods", payment_method_docs)

    cur.close()
    conn.close()
    print("Master Data successfully generated, saved to PostgreSQL, and synced to Elasticsearch!")


if __name__ == "__main__":
    generate_all_master_data()
