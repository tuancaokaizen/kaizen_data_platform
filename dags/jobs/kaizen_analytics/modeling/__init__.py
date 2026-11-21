"""Model fit/predict helpers and durable checkpoints (MinIO + CH catalog).
Huấn luyện/dự đoán và checkpoint bền vững (MinIO + catalog CH).
"""

from jobs.kaizen_analytics.modeling.checkpoint import CheckpointStore, StoredCheckpoint
from jobs.kaizen_analytics.modeling.clustering import ClusterArtifact, fit_customer_clusters
from jobs.kaizen_analytics.modeling.classifier import ClassifierArtifact, fit_segment_classifier
from jobs.kaizen_analytics.modeling.minio_store import InMemoryObjectStore, MinioObjectStore, MinioSettings

__all__ = [
    "CheckpointStore",
    "StoredCheckpoint",
    "ClusterArtifact",
    "ClassifierArtifact",
    "fit_customer_clusters",
    "fit_segment_classifier",
    "InMemoryObjectStore",
    "MinioObjectStore",
    "MinioSettings",
]
