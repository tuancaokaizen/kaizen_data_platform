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

    if layer == "dw":
        resolved_path = resolve_iceberg_path('dw', table_name)
        if resolved_path:
            sql = f"""
            CREATE DATABASE IF NOT EXISTS dw;
            DROP TABLE IF EXISTS dw.{table_name};
            CREATE TABLE dw.{table_name}
            ENGINE = Iceberg(
                '{resolved_path}',
                '{minio_user}',
                '{minio_pass}'
            );
            """
        else:
            sql = f"""
            CREATE DATABASE IF NOT EXISTS dw;
            DROP TABLE IF EXISTS dw.{table_name};
            CREATE TABLE dw.{table_name}
            ENGINE = S3(
                '{minio_endpoint}/iceberg-data/dw/{table_name}_*/data/*.parquet',
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
