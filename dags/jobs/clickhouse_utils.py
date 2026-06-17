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

    params = {
        "user": user,
        "password": password
    }

    def resolve_iceberg_path(lyr, tbl):
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
            bucket_name = 'iceberg-data'
            prefix = f"{lyr}/{tbl}_"
            
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            prefixes = response.get('CommonPrefixes', [])
            if prefixes:
                folder_path = prefixes[0]['Prefix'].rstrip('/')
                resolved = f"{minio_endpoint}/{bucket_name}/{folder_path}"
                print(f"Resolved Iceberg path for {lyr}.{tbl} to: {resolved}")
                return resolved
            else:
                print(f"WARNING: No Iceberg folder found for prefix '{prefix}' in MinIO. Falling back to S3 wildcard.")
                return None
        except Exception as e:
            print(f"WARNING: Error resolving Iceberg path via boto3: {e}. Falling back to S3 wildcard.")
            return None

    if layer == "report":
        db_name = "report"
        schema_paths = [
            os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'schema_manager', 'clickhouse', 'schema_clickhouse.sql')),
            '/opt/airflow/schema_manager/clickhouse/schema_clickhouse.sql',
            '/opt/airflow/dags/schema_manager/clickhouse/schema_clickhouse.sql'
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
            
        # Parse statements to find the specific CREATE TABLE statement
        raw_statements = content.split(';')
        statements_to_run = [f"CREATE DATABASE IF NOT EXISTS {db_name}"]
        
        target_marker = f"{db_name}.{table_name}"
        found_table = False
        for stmt in raw_statements:
            stmt_clean = stmt.strip()
            if not stmt_clean:
                continue
            # Remove comments for matching check
            lines = [line for line in stmt_clean.split('\n') if not line.strip().startswith('--')]
            stmt_no_comments = '\n'.join(lines)
            if "CREATE TABLE" in stmt_no_comments and target_marker in stmt_no_comments:
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
                        print(f"WARNING: ClickHouse table '{table_name}' could not be created because no Parquet files exist in S3 (path might be empty). Skipping table creation.")
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
