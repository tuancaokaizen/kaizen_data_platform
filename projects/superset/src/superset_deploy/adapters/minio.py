"""MinIO S3 client / Client S3 MinIO."""

from __future__ import annotations

from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import EndpointConnectionError

from superset_deploy.config import env


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=env("MINIO_ENDPOINT", "http://localhost:31900"),
        aws_access_key_id=env("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=env("MINIO_SECRET_KEY", "kaizen123"),
        config=Config(signature_version="s3v4"),
        verify=False,
    )


def ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)
        print(f"✔ Created bucket: {bucket}")


def upload_bytes(client, bucket: str, key: str, body: bytes, content_type: str = "application/octet-stream") -> None:
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    print(f"✔ Uploaded s3://{bucket}/{key}")


def upload_file(client, bucket: str, key: str, local_path: Path) -> None:
    client.upload_file(str(local_path), bucket, key)
    print(f"✔ Uploaded s3://{bucket}/{key}")


def print_minio_help(endpoint: str) -> None:
    print(
        f"\n❌ Cannot reach MinIO at {endpoint}\n"
        "   MinIO thường KHÔNG expose Tailscale :9000 — cần một trong các cách:\n"
        "   1. Port-forward (terminal riêng):\n"
        "      ./scripts/port_forward.sh\n"
        "      MINIO_ENDPOINT=http://localhost:31900 python -m superset_deploy sync-minio\n"
        "   2. Hoặc set MINIO_ENDPOINT = NodePort MinIO nếu cluster có expose\n",
    )


def check_reachable(endpoint: str) -> bool:
    try:
        client = s3_client()
        client.list_buckets()
        return True
    except EndpointConnectionError:
        print_minio_help(endpoint)
        return False
    except Exception:
        return True
