"""Checkpoint round-trip: MinIO-like object store + in-memory ClickHouse catalog.
Round-trip checkpoint: object store giống MinIO + catalog ClickHouse trong bộ nhớ.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from jobs.kaizen_analytics.modeling.checkpoint import CheckpointStore
from jobs.kaizen_analytics.modeling.minio_store import (
    InMemoryObjectStore,
    MinioSettings,
    dump_artifact,
    load_artifact,
    metrics_key,
)


class _FakeClickHouse:
    """Minimal client stub for CheckpointStore unit tests.
    Client giả tối thiểu cho unit test CheckpointStore.
    """

    def __init__(self) -> None:
        self.rows: list[tuple] = []
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def insert_tsv(self, table: str, columns: tuple[str, ...], rows: list[tuple]) -> int:
        assert table == "report.ml_checkpoint"
        self.rows.extend(rows)
        return len(rows)

    def query_rows(self, sql: str) -> list[tuple]:
        if not self.rows:
            return []
        # Newest as_of first / AsOf mới nhất trước
        ordered = sorted(self.rows, key=lambda row: str(row[0]), reverse=True)
        return [ordered[0]]


def test_dump_and_load_artifact_roundtrip():
    store = InMemoryObjectStore()
    artifact = {"kmeans": True, "centers": [1, 2, 3]}
    metrics = {"k": 3}
    as_of = date(2026, 11, 6)
    uri = dump_artifact(
        store,
        prefix="kaizen",
        model_name="customer_kmeans",
        as_of=as_of,
        artifact=artifact,
        metrics=metrics,
    )
    assert uri == "s3://ml-models/kaizen/customer_kmeans/2026-11-06/artifact.joblib"
    loaded = load_artifact(store, bucket="ml-models", uri=uri)
    assert loaded == artifact
    side = store.get_bytes(metrics_key("kaizen", "customer_kmeans", as_of))
    assert b'"k": 3' in side or b'"k":3' in side


def test_checkpoint_store_save_load_via_minio_uri():
    ch = _FakeClickHouse()
    objects = InMemoryObjectStore()
    settings = MinioSettings(
        endpoint="http://minio.test:9000",
        access_key="x",
        secret_key="y",
        bucket="ml-models",
        prefix="kaizen",
    )
    store = CheckpointStore(ch, object_store=objects, minio_settings=settings)  # type: ignore[arg-type]
    payload: dict[str, Any] = {"model": "rf", "n_estimators": 10}
    uri = store.save(
        model_name="customer_segment_rf",
        as_of=date(2026, 11, 6),
        algorithm="RandomForestClassifier",
        artifact=payload,
        metrics={"accuracy": 0.91},
    )
    assert uri.startswith("s3://ml-models/kaizen/customer_segment_rf/")
    assert ch.rows
    # Columns: AsOfDate, ModelName, Algorithm, MetricsJson, ArtifactUri, ArtifactBase64
    assert ch.rows[0][4] == uri
    assert ch.rows[0][5] == ""

    loaded = store.load("customer_segment_rf")
    assert loaded is not None
    assert loaded.artifact == payload
    assert loaded.artifact_uri == uri
    assert loaded.metrics["accuracy"] == 0.91
    assert any("ArtifactUri" in sql for sql in ch.executed)
