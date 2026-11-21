"""Checkpoint catalog in ClickHouse; binary artifacts on MinIO.
Catalog checkpoint trong ClickHouse; artifact nhị phân trên MinIO.
"""
from __future__ import annotations

import base64
import io
import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

import joblib

from jobs.kaizen_analytics.clickhouse_client import ClickHouseClient
from jobs.kaizen_analytics.modeling.minio_store import (
    MinioObjectStore,
    MinioSettings,
    ObjectStore,
    dump_artifact,
    load_artifact,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class StoredCheckpoint:
    model_name: str
    as_of: date
    algorithm: str
    metrics: dict[str, Any]
    artifact: Any
    artifact_uri: str = ""


class CheckpointStore:
    """Upload joblib to MinIO; keep URI + metrics in report.ml_checkpoint.
    Upload joblib lên MinIO; giữ URI + metrics trong report.ml_checkpoint.
    """

    def __init__(
        self,
        client: ClickHouseClient,
        *,
        object_store: ObjectStore | None = None,
        minio_settings: MinioSettings | None = None,
    ) -> None:
        self._client = client
        self._settings = minio_settings or MinioSettings.from_env()
        self._store = object_store or MinioObjectStore(self._settings)
        self._client.execute(_ddl())
        self._migrate()

    def save(
        self,
        *,
        model_name: str,
        as_of: date,
        algorithm: str,
        artifact: Any,
        metrics: dict[str, Any] | None = None,
    ) -> str:
        metrics_payload = dict(metrics or {})
        metrics_payload.setdefault("algorithm", algorithm)
        uri = dump_artifact(
            self._store,
            prefix=self._settings.prefix,
            model_name=model_name,
            as_of=as_of,
            artifact=artifact,
            metrics=metrics_payload,
        )
        metrics_json = json.dumps(metrics_payload, ensure_ascii=False)
        # Append-only catalog; load() picks newest AsOfDate per model
        # Catalog chỉ append; load() lấy AsOfDate mới nhất theo model
        self._client.insert_tsv(
            "report.ml_checkpoint",
            ("AsOfDate", "ModelName", "Algorithm", "MetricsJson", "ArtifactUri", "ArtifactBase64"),
            [(as_of.isoformat(), model_name, algorithm, metrics_json, uri, "")],
        )
        log.info("checkpoint saved model=%s uri=%s", model_name, uri)
        return uri

    def load(self, model_name: str) -> StoredCheckpoint | None:
        rows = self._client.query_rows(
            f"""
            SELECT AsOfDate, ModelName, Algorithm, MetricsJson, ArtifactUri, ArtifactBase64
            FROM report.ml_checkpoint
            WHERE ModelName = '{_escape(model_name)}'
            ORDER BY AsOfDate DESC
            LIMIT 1
            """
        )
        if not rows:
            return None
        as_of_raw, name, algorithm, metrics_json, artifact_uri, blob = rows[0]
        artifact = self._load_artifact(str(artifact_uri or ""), str(blob or ""))
        try:
            metrics = json.loads(metrics_json or "{}")
        except json.JSONDecodeError:
            metrics = {}
        return StoredCheckpoint(
            model_name=name,
            as_of=date.fromisoformat(str(as_of_raw)),
            algorithm=algorithm,
            metrics=metrics,
            artifact=artifact,
            artifact_uri=str(artifact_uri or ""),
        )

    def _load_artifact(self, uri: str, legacy_base64: str) -> Any:
        if uri:
            return load_artifact(self._store, bucket=self._settings.bucket, uri=uri)
        # Legacy rows stored the whole model as base64 in ClickHouse
        # Dòng cũ lưu cả model dạng base64 trong ClickHouse
        if legacy_base64:
            return joblib.load(io.BytesIO(base64.b64decode(legacy_base64.encode("ascii"))))
        raise ValueError("checkpoint row has neither ArtifactUri nor ArtifactBase64")

    def _migrate(self) -> None:
        try:
            self._client.execute(
                "ALTER TABLE report.ml_checkpoint ADD COLUMN IF NOT EXISTS ArtifactUri String DEFAULT ''"
            )
        except Exception as exc:
            log.warning("ml_checkpoint migrate ArtifactUri skipped: %s", exc)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _ddl() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS report.ml_checkpoint (
        AsOfDate Date,
        ModelName String,
        Algorithm LowCardinality(String),
        MetricsJson String,
        ArtifactUri String,
        ArtifactBase64 String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (ModelName, AsOfDate)
    """
