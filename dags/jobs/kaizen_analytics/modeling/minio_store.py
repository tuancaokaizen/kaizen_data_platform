"""MinIO/S3 helpers for sklearn joblib artifacts.
Helper MinIO/S3 cho artifact joblib sklearn.
"""
from __future__ import annotations

import io
import json
import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import joblib

log = logging.getLogger(__name__)

DEFAULT_ENDPOINT = "http://minio.storage.svc.cluster.local:9000"
DEFAULT_BUCKET = "ml-models"
DEFAULT_PREFIX = "kaizen"


class ObjectStore(Protocol):
    """Minimal put/get interface for tests and MinIO.
    Giao diện put/get tối thiểu cho test và MinIO.
    """

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> str: ...

    def get_bytes(self, key: str) -> bytes: ...

    def put_json(self, key: str, data: dict[str, Any]) -> str: ...


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str = DEFAULT_ENDPOINT
    access_key: str = "admin"
    secret_key: str = "kaizen123"
    bucket: str = DEFAULT_BUCKET
    prefix: str = DEFAULT_PREFIX

    @classmethod
    def from_env(cls) -> "MinioSettings":
        # Prefer KAIZEN_ML_* then generic MINIO_* / Prefer KAIZEN_ML_* rồi MINIO_* chung
        return cls(
            endpoint=os.getenv("KAIZEN_ML_MINIO_ENDPOINT")
            or os.getenv("MINIO_ENDPOINT")
            or DEFAULT_ENDPOINT,
            access_key=os.getenv("KAIZEN_ML_MINIO_ACCESS_KEY")
            or os.getenv("MINIO_ACCESS_KEY")
            or os.getenv("S3_ACCESS_KEY")
            or "admin",
            secret_key=os.getenv("KAIZEN_ML_MINIO_SECRET_KEY")
            or os.getenv("MINIO_SECRET_KEY")
            or os.getenv("S3_SECRET_KEY")
            or "kaizen123",
            bucket=os.getenv("KAIZEN_ML_MINIO_BUCKET") or DEFAULT_BUCKET,
            prefix=os.getenv("KAIZEN_ML_MINIO_PREFIX") or DEFAULT_PREFIX,
        )


class MinioObjectStore:
    """boto3 S3 client pointed at in-cluster MinIO.
    Client boto3 S3 trỏ MinIO trong cluster.
    """

    def __init__(self, settings: MinioSettings | None = None) -> None:
        self.settings = settings or MinioSettings.from_env()
        # Lazy import keeps unit tests light when only InMemoryObjectStore is used
        # Import lười để unit test nhẹ khi chỉ dùng InMemoryObjectStore
        import boto3
        from botocore.client import Config

        self._s3 = boto3.client(
            "s3",
            endpoint_url=self.settings.endpoint,
            aws_access_key_id=self.settings.access_key,
            aws_secret_access_key=self.settings.secret_key,
            config=Config(signature_version="s3v4"),
            verify=False,
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        bucket = self.settings.bucket
        try:
            self._s3.head_bucket(Bucket=bucket)
        except ClientError:
            try:
                self._s3.create_bucket(Bucket=bucket)
                log.info("created MinIO bucket=%s", bucket)
            except ClientError as exc:
                # Race: another worker created it / Race: worker khác đã tạo
                log.warning("ensure bucket %s: %s", bucket, exc)

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> str:
        self._s3.put_object(
            Bucket=self.settings.bucket,
            Key=key,
            Body=payload,
            ContentType=content_type,
        )
        return self.uri_for(key)

    def get_bytes(self, key: str) -> bytes:
        response = self._s3.get_object(Bucket=self.settings.bucket, Key=key)
        return response["Body"].read()

    def put_json(self, key: str, data: dict[str, Any]) -> str:
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        return self.put_bytes(key, body, content_type="application/json")

    def uri_for(self, key: str) -> str:
        return f"s3://{self.settings.bucket}/{key}"

    def key_from_uri(self, uri: str) -> str:
        prefix = f"s3://{self.settings.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError(f"URI not in bucket {self.settings.bucket}: {uri}")
        return uri[len(prefix) :]


class InMemoryObjectStore:
    """Dict-backed store for unit tests.
    Store trong dict cho unit test.
    """

    def __init__(self, bucket: str = DEFAULT_BUCKET) -> None:
        self.bucket = bucket
        self._objects: dict[str, bytes] = {}

    def put_bytes(self, key: str, payload: bytes, content_type: str = "application/octet-stream") -> str:
        self._objects[key] = payload
        return f"s3://{self.bucket}/{key}"

    def get_bytes(self, key: str) -> bytes:
        if key not in self._objects:
            raise KeyError(key)
        return self._objects[key]

    def put_json(self, key: str, data: dict[str, Any]) -> str:
        return self.put_bytes(
            key,
            json.dumps(data, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
        )


def artifact_key(prefix: str, model_name: str, as_of: date) -> str:
    return f"{prefix.rstrip('/')}/{model_name}/{as_of.isoformat()}/artifact.joblib"


def metrics_key(prefix: str, model_name: str, as_of: date) -> str:
    return f"{prefix.rstrip('/')}/{model_name}/{as_of.isoformat()}/metrics.json"


def _joblib_dumps(obj: Any) -> bytes:
    # BytesIO keeps compatibility with joblib versions without dumps()
    # BytesIO tương thích joblib không có dumps()
    buffer = io.BytesIO()
    joblib.dump(obj, buffer)
    return buffer.getvalue()


def _joblib_loads(payload: bytes) -> Any:
    return joblib.load(io.BytesIO(payload))


def dump_artifact(store: ObjectStore, *, prefix: str, model_name: str, as_of: date, artifact: Any, metrics: dict) -> str:
    """Serialize artifact + sidecar metrics; return s3 URI of the joblib object.
    Serialize artifact + metrics kèm theo; trả URI s3 của object joblib.
    """
    key = artifact_key(prefix, model_name, as_of)
    uri = store.put_bytes(key, _joblib_dumps(artifact))
    store.put_json(metrics_key(prefix, model_name, as_of), metrics)
    return uri


def load_artifact(store: ObjectStore, *, bucket: str, uri: str) -> Any:
    """Load joblib from s3://bucket/key URI.
    Nạp joblib từ URI s3://bucket/key.
    """
    prefix = f"s3://{bucket}/"
    if not uri.startswith(prefix):
        raise ValueError(f"unexpected artifact uri={uri}")
    key = uri[len(prefix) :]
    return _joblib_loads(store.get_bytes(key))
