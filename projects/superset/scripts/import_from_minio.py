#!/usr/bin/env python3
"""
Import Superset dashboard ZIPs from MinIO into target Superset cluster.
Import dashboard từ MinIO vào Superset (UAT/PROD).

Requires VERSIONED_EXPORT enabled in superset_config.py on target cluster.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import tempfile
import zipfile
from pathlib import Path

import boto3
import requests
from botocore.client import Config
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / "config" / ".env")


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://localhost:31900"),
        aws_access_key_id=env("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=env("MINIO_SECRET_KEY", "kaizen123"),
        config=Config(signature_version="s3v4"),
        verify=False,
    )


def superset_session() -> requests.Session:
    host = env("SUPERSET_HOST", "http://localhost:32288").rstrip("/")
    username = env("SUPERSET_USERNAME", "admin")
    password = env("SUPERSET_PASSWORD", "admin")

    session = requests.Session()
    login = session.post(
        f"{host}/api/v1/security/login",
        json={"username": username, "password": password, "provider": "db", "refresh": True},
        timeout=30,
    )
    login.raise_for_status()
    token = login.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    session.superset_host = host  # type: ignore[attr-defined]
    return session


def list_dashboard_zips(client, bucket: str, prefix: str) -> list[str]:
    resp = client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix.rstrip('/')}/dashboards/")
    return [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".zip")]


def import_zip(session: requests.Session, zip_bytes: bytes, overwrite: bool = True) -> None:
    host = session.superset_host  # type: ignore[attr-defined]
    files = {"formData": ("dashboard.zip", zip_bytes, "application/zip")}
    data = {"overwrite": "true" if overwrite else "false"}
    resp = session.post(f"{host}/api/v1/dashboard/import/", files=files, data=data, timeout=120)
    if resp.status_code >= 400:
        print(f"❌ Import failed: {resp.status_code} {resp.text}", file=sys.stderr)
        resp.raise_for_status()
    print(f"✔ Imported dashboard bundle")


def main() -> int:
    parser = argparse.ArgumentParser(description="Import Superset dashboards from MinIO")
    parser.add_argument("--key", help="Specific MinIO object key (dashboards/*.zip)")
    parser.add_argument("--all", action="store_true", help="Import all dashboard ZIPs under assets/dashboards/")
    args = parser.parse_args()

    bucket = env("MINIO_BUCKET", "superset")
    prefix = env("MINIO_ASSETS_PREFIX", "assets/")
    client = s3_client()
    session = superset_session()

    keys: list[str] = []
    if args.key:
        keys = [args.key]
    elif args.all:
        keys = list_dashboard_zips(client, bucket, prefix)
    else:
        manifest_key = f"{prefix.rstrip('/')}/manifest.json"
        try:
            obj = client.get_object(Bucket=bucket, Key=manifest_key)
            manifest = json.loads(obj["Body"].read())
            print(f"Manifest updated_at: {manifest.get('updated_at')}")
        except Exception:
            pass
        keys = list_dashboard_zips(client, bucket, prefix)

    if not keys:
        print("No dashboard ZIP found in MinIO. Export from Superset UI first.")
        return 1

    for key in keys:
        print(f"→ Importing {key}")
        obj = client.get_object(Bucket=bucket, Key=key)
        zip_bytes = obj["Body"].read()
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            with tempfile.TemporaryDirectory() as tmp:
                zf.extractall(tmp)
                print(f"  Files: {[p.name for p in Path(tmp).rglob('*') if p.is_file()][:5]}...")
        import_zip(session, zip_bytes)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
