import re

NESSIE_URI = "http://nessie-catalog-svc.compute.svc.cluster.local:19120/api/v1"
ICEBERG_BUCKET = "iceberg-data"


def folder_from_metadata_location(location):
    """Table folder `<ns>/<table>_<uuid>` of an Iceberg metadata file, or None outside the bucket.
    Thư mục bảng `<ns>/<table>_<uuid>` của file metadata Iceberg, None nếu không thuộc bucket."""
    bucket, _, key = (location or "").split("://", 1)[-1].partition("/")
    if bucket != ICEBERG_BUCKET or "/metadata/" not in key:
        return None
    return key.split("/metadata/", 1)[0]


def table_folder_pattern(lyr, tbl):
    """Folders of exactly this table: `financial_report` must not match `financial_report_detail_<uuid>`.
    Thư mục của đúng bảng này: `financial_report` không được khớp `financial_report_detail_<uuid>`."""
    uuid = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    return re.compile(rf"^{re.escape(lyr)}/{re.escape(tbl)}_{uuid}/?$")


def newest_folder(candidates):
    """Pick the folder whose metadata changed last from (folder, last_modified) pairs.
    Chọn thư mục có metadata đổi gần nhất từ các cặp (thư mục, thời điểm sửa)."""
    dated = [(modified, folder) for folder, modified in candidates if modified is not None]
    return max(dated)[1] if dated else None


def run_clickhouse_setup(table_name, layer=None):
    import requests
    import os
    import configparser

    # 1. Parse config.ini
    config_paths = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.ini')),
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config.ini')),
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
        print("WARNING: config.ini not found. Using default fallbacks.")

    # 2. Extract configurations
    ch_hosts_str = config.get('clickhouse', 'hosts', fallback='clickhouse-service.databases.svc.cluster.local')
    hosts = [h.strip() for h in ch_hosts_str.split(',') if h.strip()]
    port = config.getint('clickhouse', 'port', fallback=8123)
    user = config.get('clickhouse', 'username', fallback='admin')
    password = config.get('clickhouse', 'password', fallback='kaizen123')

    minio_endpoint = config.get('minio', 'endpoint', fallback='http://minio.storage.svc.cluster.local:9000')
    minio_user = config.get('minio', 'access_key', fallback='admin')
    minio_pass = config.get('minio', 'secret_key', fallback='kaizen123')
    nessie_uri = config.get('nessie', 'uri', fallback=NESSIE_URI)

    params = {
        "user": user,
        "password": password
    }

    def nessie_folder(lyr, tbl):
        # Nessie knows the live folder; dropped tables leave old ones behind / Nessie biết thư mục đang dùng; bảng bị drop còn để lại thư mục cũ
        try:
            response = requests.get(f"{nessie_uri}/contents/{lyr}.{tbl}", params={"ref": "main"}, timeout=10)
            if response.status_code == 200:
                return folder_from_metadata_location(response.json().get("metadataLocation"))
            print(f"WARNING: Nessie has no {lyr}.{tbl} (HTTP {response.status_code}).")
        except Exception as e:
            print(f"WARNING: Nessie lookup for {lyr}.{tbl} failed: {e}")
        return None

    def newest_minio_folder(lyr, tbl):
        import boto3
        from botocore.client import Config
        try:
            s3 = boto3.client(
                's3',
                endpoint_url=minio_endpoint,
                aws_access_key_id=minio_user,
                aws_secret_access_key=minio_pass,
                config=Config(signature_version='s3v4'),
                verify=False
            )
            pattern = table_folder_pattern(lyr, tbl)
            candidates = []
            for page in s3.get_paginator('list_objects_v2').paginate(
                    Bucket=ICEBERG_BUCKET, Prefix=f"{lyr}/{tbl}_", Delimiter='/'):
                for entry in page.get('CommonPrefixes', []):
                    folder = entry['Prefix'].rstrip('/')
                    if not pattern.match(folder):
                        continue
                    metadata = s3.list_objects_v2(Bucket=ICEBERG_BUCKET, Prefix=f"{folder}/metadata/")
                    modified = [obj['LastModified'] for obj in metadata.get('Contents', [])]
                    candidates.append((folder, max(modified) if modified else None))
            return newest_folder(candidates)
        except Exception as e:
            print(f"WARNING: Error listing Iceberg folders via boto3: {e}")
            return None

    def resolve_iceberg_path(lyr, tbl):
        folder = nessie_folder(lyr, tbl) or newest_minio_folder(lyr, tbl)
        if not folder:
            print(f"WARNING: No Iceberg folder found for {lyr}.{tbl}. Falling back to S3 wildcard.")
            return None
        resolved = f"{minio_endpoint}/{ICEBERG_BUCKET}/{folder}"
        print(f"Resolved Iceberg path for {lyr}.{tbl} to: {resolved}")
        return resolved

    if layer == "report":
        db_name = "report"
        # Airflow syncs only /opt/airflow/dags — bundle DDL lives under dags/schema_manager
        # Airflow chỉ sync /opt/airflow/dags — file DDL được đóng gói trong dags/schema_manager
        schema_paths = [
            os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'schema_manager', 'clickhouse',
                                         'schema_clickhouse.sql')),
            os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'schema_manager', 'clickhouse',
                                         'schema_clickhouse.sql')),
            '/opt/airflow/dags/schema_manager/clickhouse/schema_clickhouse.sql',
            '/opt/airflow/schema_manager/clickhouse/schema_clickhouse.sql',
        ]

        schema_sql_path = None
        for path in schema_paths:
            if os.path.exists(path):
                schema_sql_path = path
                break

        if not schema_sql_path:
            raise FileNotFoundError("schema_clickhouse.sql not found in search paths")

        with open(schema_sql_path, 'r', encoding='utf-8') as sf:
            content = sf.read()

        # Strip full-line comments before split — semicolons inside `-- ...; ...` break naive split
        # Bỏ comment dòng trước khi split — dấu `;` trong comment làm tách statement sai
        content_no_comments = '\n'.join(
            line for line in content.split('\n') if not line.strip().startswith('--')
        )

        # Parse statements to find the specific CREATE TABLE statement
        raw_statements = content_no_comments.split(';')
        statements_to_run = [f"CREATE DATABASE IF NOT EXISTS {db_name}"]

        target_marker = f"{db_name}.{table_name}"
        found_table = False
        for stmt in raw_statements:
            stmt_clean = stmt.strip()
            if not stmt_clean:
                continue
            if "CREATE TABLE" in stmt_clean and target_marker in stmt_clean:
                statements_to_run.append(stmt_clean)
                found_table = True
                break

        if not found_table:
            raise ValueError(f"Could not find CREATE TABLE statement for {target_marker} in schema_clickhouse.sql")

        sql = ';\n'.join(statements_to_run)
    elif layer == "dw" or layer == "serving":
        db_name = layer
        resolved_path = resolve_iceberg_path(db_name, table_name)
        if resolved_path:
            sql = f"""
            CREATE DATABASE IF NOT EXISTS {db_name};
            DROP TABLE IF EXISTS {db_name}.{table_name};
            CREATE TABLE {db_name}.{table_name}
            ENGINE = Iceberg(
                '{resolved_path}',
                '{minio_user}',
                '{minio_pass}'
            );
            """
        else:
            sql = f"""
            CREATE DATABASE IF NOT EXISTS {db_name};
            DROP TABLE IF EXISTS {db_name}.{table_name};
            CREATE TABLE {db_name}.{table_name}
            ENGINE = S3(
                '{minio_endpoint}/iceberg-data/{db_name}/{table_name}_*/data/*.parquet',
                '{minio_user}',
                '{minio_pass}',
                'Parquet'
            );
            """
    else:
        resolved_stg = resolve_iceberg_path('stg', table_name)
        resolved_fnd = resolve_iceberg_path('fnd', table_name)

        stg_sql = f"""
        CREATE TABLE stg.{table_name}
        ENGINE = Iceberg(
            '{resolved_stg}',
            '{minio_user}',
            '{minio_pass}'
        );
        """ if resolved_stg else f"""
        CREATE TABLE stg.{table_name}
        ENGINE = S3(
            '{minio_endpoint}/iceberg-data/stg/{table_name}_*/data/*.parquet',
            '{minio_user}',
            '{minio_pass}',
            'Parquet'
        );
        """

        fnd_sql = f"""
        CREATE TABLE fnd.{table_name}
        ENGINE = Iceberg(
            '{resolved_fnd}',
            '{minio_user}',
            '{minio_pass}'
        );
        """ if resolved_fnd else f"""
        CREATE TABLE fnd.{table_name}
        ENGINE = S3(
            '{minio_endpoint}/iceberg-data/fnd/{table_name}_*/data/*.parquet',
            '{minio_user}',
            '{minio_pass}',
            'Parquet'
        );
        """

        sql = f"""
        CREATE DATABASE IF NOT EXISTS stg;
        CREATE DATABASE IF NOT EXISTS fnd;

        DROP TABLE IF EXISTS stg.{table_name};
        {stg_sql}

        DROP TABLE IF EXISTS fnd.{table_name};
        {fnd_sql}
        """

    queries = [q.strip() for q in sql.split(';') if q.strip()]

    last_err = None
    for host in hosts:
        url = f"http://{host}:{port}/"
        try:
            print(f"Connecting to ClickHouse on {url}...")
            for query in queries:
                print(f"Executing query: {query}")
                response = requests.post(url, params=params, data=query, timeout=30)
                if response.status_code != 200:
                    err_msg = response.text
                    if "CANNOT_EXTRACT_TABLE_STRUCTURE" in err_msg or "Code: 636" in err_msg:
                        print(
                            f"WARNING: ClickHouse table '{table_name}' could not be created because no Parquet files exist in S3 (path might be empty). Skipping table creation.")
                        return
                    raise Exception(f"ClickHouse HTTP {response.status_code}: {err_msg}")
            print(f"Successfully created ClickHouse tables for '{table_name}' via {host}")
            return
        except Exception as e:
            # If we returned early from inside the try block (e.g. on warning), we won't hit this
            last_err = e
            print(f"Failed to execute queries on ClickHouse host {host}: {e}")

    if last_err:
        raise last_err


# Active hot-layer report tables (ReplacingMergeTree) / Bảng report hot dùng cho push + view
REPORT_BASE_TABLES = [
    "financial_report",
    "financial_report_detail",
    "financial_report_product",
    "financial_report_order",
    "fund_report",
    "fund_report_shop",
    "fund_report_order",
    "inventory_health",
    "shop_capacity_utilization",
    "carrier_scorecard",
    "supplier_performance",
    "realtime_business_by_order",
]


def ensure_all_report_base_tables():
    """Create report.* native tables if missing (e.g. after clear_clickhouse_serving).
    Tạo bảng report.* nếu thiếu — tránh view/push fail với UNKNOWN_TABLE."""
    print(f"Ensuring {len(REPORT_BASE_TABLES)} ClickHouse report base table(s) exist...")
    for table_name in REPORT_BASE_TABLES:
        run_clickhouse_setup(table_name, layer="report")
    print("All ClickHouse report base tables are ready.")


def refresh_realtime_business_serving_table():
    """Point serving.realtime_business_by_order at Iceberg after MigrateRealtimeBusinessToColdJob.
    Trỏ serving.realtime_business_by_order sang Iceberg sau job migrate cold."""
    try:
        run_clickhouse_setup("realtime_business_by_order", layer="serving")
        print("serving.realtime_business_by_order refreshed as Iceberg engine table.")
    except Exception as e:
        # Cold may be empty on first days (nothing older than hot window yet)
        # Cold có thể trống những ngày đầu (chưa có dòng cũ hơn hot window)
        print(f"WARNING: could not refresh serving.realtime_business_by_order: {e}")



def run_clickhouse_views_setup():
    """Deploy hot/cold report views with Vietnamese column aliases / Triển khai view báo cáo hot-cold có alias tiếng Việt."""
    # Views reference report.* — tables may have been dropped by clear_clickhouse_serving
    # View tham chiếu report.* — bảng có thể đã bị xóa bởi clear_clickhouse_serving
    ensure_all_report_base_tables()

    import requests
    import os
    import configparser

    # Resolve config.ini from common Airflow/local paths / Tìm config.ini từ các đường dẫn Airflow/local
    config_paths = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.ini')),
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config.ini')),
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
        print("WARNING: config.ini not found. Using default fallbacks.")

    ch_hosts_str = config.get('clickhouse', 'hosts', fallback='clickhouse-service.databases.svc.cluster.local')
    hosts = [h.strip() for h in ch_hosts_str.split(',') if h.strip()]
    port = config.getint('clickhouse', 'port', fallback=8123)
    user = config.get('clickhouse', 'username', fallback='admin')
    password = config.get('clickhouse', 'password', fallback='kaizen123')

    params = {
        "user": user,
        "password": password
    }

    # Locate views DDL in schema_manager / Tìm file DDL view trong schema_manager
    schema_paths = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'schema_manager', 'clickhouse',
                                     'schema_clickhouse_views.sql')),
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'schema_manager', 'clickhouse',
                                     'schema_clickhouse_views.sql')),
        '/opt/airflow/dags/schema_manager/clickhouse/schema_clickhouse_views.sql',
        '/opt/airflow/schema_manager/clickhouse/schema_clickhouse_views.sql',
    ]

    schema_sql_path = None
    for path in schema_paths:
        if os.path.exists(path):
            schema_sql_path = path
            break

    if not schema_sql_path:
        raise FileNotFoundError("schema_clickhouse_views.sql not found in search paths")

    with open(schema_sql_path, 'r', encoding='utf-8') as sf:
        content = sf.read()

    queries = [q.strip() for q in content.split(';') if q.strip()]

    last_err = None
    for host in hosts:
        url = f"http://{host}:{port}/"
        try:
            print(f"Connecting to ClickHouse on {url}...")
            for query in queries:
                print(f"Executing query: {query}")
                response = requests.post(url, params=params, data=query, timeout=30)
                if response.status_code != 200:
                    raise Exception(f"ClickHouse HTTP {response.status_code}: {response.text}")
            print("Successfully deployed ClickHouse report views (hot/cold, Vietnamese columns).")
            return
        except Exception as e:
            last_err = e
            print(f"Failed to execute view DDL on ClickHouse host {host}: {e}")

    if last_err:
        raise last_err


# Report tables cleared with serving layer reset / Bảng báo cáo khi reset serving
SERVING_REPORT_TABLES = [
    "financial_report",
    "financial_report_detail",
    "financial_report_product",
    "financial_report_order",
    "fund_report",
    "fund_report_shop",
    "fund_report_order",
    "inventory_health",
    "shop_capacity_utilization",
    "carrier_scorecard",
    "supplier_performance",
    "realtime_business_by_order",
    # Legacy grains / Grain cũ đã bỏ
    "fund_report_detail",
    "fund_report_product",
]


def _load_clickhouse_connection():
    """Load ClickHouse hosts and credentials from config.ini / Đọc cấu hình ClickHouse."""
    import os
    import configparser

    config_paths = [
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config.ini')),
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config.ini')),
        '/opt/airflow/config.ini',
        '/opt/airflow/dags/config.ini',
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
        print("WARNING: config.ini not found. Using default fallbacks.")

    ch_hosts_str = config.get(
        'clickhouse', 'hosts', fallback='clickhouse-service.databases.svc.cluster.local'
    )
    hosts = [h.strip() for h in ch_hosts_str.split(',') if h.strip()]
    port = config.getint('clickhouse', 'port', fallback=8123)
    user = config.get('clickhouse', 'username', fallback='admin')
    password = config.get('clickhouse', 'password', fallback='kaizen123')
    return hosts, port, user, password


def _execute_clickhouse_queries(queries, success_message):
    """Run DDL/DML statements against ClickHouse HTTP API / Chạy query qua HTTP API."""
    import requests

    hosts, port, user, password = _load_clickhouse_connection()
    params = {"user": user, "password": password}

    last_err = None
    for host in hosts:
        url = f"http://{host}:{port}/"
        try:
            print(f"Connecting to ClickHouse on {url}...")
            for query in queries:
                print(f"Executing query: {query}")
                response = requests.post(url, params=params, data=query, timeout=30)
                if response.status_code != 200:
                    raise Exception(f"ClickHouse HTTP {response.status_code}: {response.text}")
            print(success_message)
            return
        except Exception as e:
            last_err = e
            print(f"Failed to execute queries on ClickHouse host {host}: {e}")

    if last_err:
        raise last_err


def run_clickhouse_clear_serving():
    """Drop report views and hot/serving tables; keeps DW/FND intact / Xóa view + bảng report/serving."""
    view_drops = [
        # Legacy view names / Tên view cũ
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_nong",
        "DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_lanh",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_nong",
        "DROP VIEW IF EXISTS serving.v_bao_cao_quy_lanh",
        # Financial views / View tài chính
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_chi_tiet_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_theo_san_pham_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_theo_don_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh",
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_chi_tiet",
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_theo_san_pham",
        "DROP VIEW IF EXISTS report.v_bao_cao_tai_chinh_theo_don",
        "DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh",
        "DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_chi_tiet",
        "DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_theo_san_pham",
        "DROP VIEW IF EXISTS serving.v_bao_cao_tai_chinh_theo_don",
        # Fund views / View quỹ
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_shop_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_don_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_san_pham_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_chi_tiet_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_shop",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_san_pham",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_chi_tiet",
        "DROP VIEW IF EXISTS report.v_bao_cao_quy_theo_don",
        "DROP VIEW IF EXISTS serving.v_bao_cao_quy",
        "DROP VIEW IF EXISTS serving.v_bao_cao_quy_theo_shop",
        "DROP VIEW IF EXISTS serving.v_bao_cao_quy_theo_san_pham",
        "DROP VIEW IF EXISTS serving.v_bao_cao_quy_chi_tiet",
        "DROP VIEW IF EXISTS serving.v_bao_cao_quy_theo_don",
        # Realtime business-by-order / Tình hình KD theo đơn realtime
        "DROP VIEW IF EXISTS report.v_bao_cao_kinh_doanh_theo_don_realtime_tong_hop",
        "DROP VIEW IF EXISTS report.v_bao_cao_kinh_doanh_theo_don_realtime",
        "DROP VIEW IF EXISTS serving.v_bao_cao_kinh_doanh_theo_don_realtime",
    ]

    table_drops = []
    for tbl in SERVING_REPORT_TABLES:
        table_drops.append(f"DROP TABLE IF EXISTS report.{tbl}")
        table_drops.append(f"DROP TABLE IF EXISTS serving.{tbl}")

    queries = [
        "CREATE DATABASE IF NOT EXISTS report",
        "CREATE DATABASE IF NOT EXISTS serving",
        *view_drops,
        *table_drops,
    ]

    _execute_clickhouse_queries(
        queries,
        "Successfully dropped ClickHouse report views + report.* hot tables + serving.* pointers.",
    )
