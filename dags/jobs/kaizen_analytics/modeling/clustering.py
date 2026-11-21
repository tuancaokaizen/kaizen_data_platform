"""KMeans on RFM with train/holdout quality metrics.
KMeans trên RFM kèm metrics chất lượng train/holdout.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler


@dataclass
class ClusterArtifact:
    scaler: StandardScaler
    model: KMeans
    cluster_labels: dict[int, tuple[str, str]]  # id -> (code, name)


def fit_customer_clusters(
    recency_days: list[float],
    frequency: list[float],
    monetary: list[float],
    *,
    n_clusters: int = 5,
    random_state: int = 42,
    train_ratio: float = 0.70,
    idx_train: list[int] | None = None,
    idx_val: list[int] | None = None,
    idx_test: list[int] | None = None,
) -> tuple[ClusterArtifact, list[int], dict]:
    """Fit KMeans on a train split; score silhouette on train/val/test; label all rows.
    Fit KMeans trên tập train; đo silhouette train/val/test; gán nhãn mọi dòng.
    Prefer Feature Store splits when provided / Ưu tiên split từ Feature Store nếu có.
    """
    matrix = _feature_matrix(recency_days, frequency, monetary)
    n = len(matrix)
    if idx_train is None or idx_val is None or idx_test is None:
        indices = np.arange(n)
        # 70% train, 15% val, 15% test (relative split of holdout)
        # 70% train, 15% val, 15% test (chia tương đối phần holdout)
        idx_train, idx_temp = train_test_split(
            indices, test_size=1.0 - train_ratio, random_state=random_state
        )
        idx_val, idx_test = train_test_split(idx_temp, test_size=0.5, random_state=random_state)
    idx_train = np.asarray(idx_train, dtype=int)
    idx_val = np.asarray(idx_val, dtype=int)
    idx_test = np.asarray(idx_test, dtype=int)

    scaler = StandardScaler()
    scaled_train = scaler.fit_transform(matrix[idx_train])
    k = max(2, min(n_clusters, len(idx_train)))
    model = KMeans(n_clusters=k, random_state=random_state, n_init=10)
    model.fit(scaled_train)

    scaled_all = scaler.transform(matrix)
    labels_all = model.predict(scaled_all).tolist()
    named = _name_clusters(model.cluster_centers_, scaler)

    metrics = {
        "k": k,
        "rows": n,
        "train_rows": int(len(idx_train)),
        "val_rows": int(len(idx_val)),
        "test_rows": int(len(idx_test)),
        "inertia_train": float(model.inertia_),
        "silhouette_train": _safe_silhouette(scaled_train, model.predict(scaled_train)),
        "silhouette_val": _safe_silhouette(scaler.transform(matrix[idx_val]), model.predict(scaler.transform(matrix[idx_val]))),
        "silhouette_test": _safe_silhouette(
            scaler.transform(matrix[idx_test]), model.predict(scaler.transform(matrix[idx_test]))
        ),
        "cluster_sizes": {
            named[cid][0]: int(sum(1 for label in labels_all if label == cid)) for cid in named
        },
    }
    return ClusterArtifact(scaler=scaler, model=model, cluster_labels=named), labels_all, metrics


def predict_clusters(artifact: ClusterArtifact, recency_days, frequency, monetary) -> list[int]:
    matrix = _feature_matrix(recency_days, frequency, monetary)
    scaled = artifact.scaler.transform(matrix)
    return artifact.model.predict(scaled).tolist()


def _safe_silhouette(scaled: np.ndarray, labels: np.ndarray, *, max_samples: int = 8_000) -> float | None:
    # Need ≥2 clusters present and enough samples / Cần ≥2 cụm xuất hiện và đủ mẫu
    if len(scaled) < 3 or len(set(labels.tolist())) < 2:
        return None
    try:
        # Silhouette is O(n²) — subsample large sets for lab speed
        # Silhouette O(n²) — lấy mẫu khi tập lớn để chạy lab nhanh
        if len(scaled) > max_samples:
            rng = np.random.default_rng(42)
            pick = rng.choice(len(scaled), size=max_samples, replace=False)
            scaled = scaled[pick]
            labels = labels[pick]
            if len(set(labels.tolist())) < 2:
                return None
        return float(silhouette_score(scaled, labels))
    except Exception:
        return None


def _feature_matrix(recency_days, frequency, monetary) -> np.ndarray:
    recency = np.log1p(np.asarray(recency_days, dtype=float))
    freq = np.log1p(np.asarray(frequency, dtype=float))
    money = np.log1p(np.maximum(np.asarray(monetary, dtype=float), 0.0))
    return np.column_stack([recency, freq, money])


def _name_clusters(centers: np.ndarray, scaler: StandardScaler) -> dict[int, tuple[str, str]]:
    """Label clusters from inverse-transformed centroids (R low / F-M high = champions).
    Đặt tên cụm từ centroid đã inverse (R thấp / F-M cao = champions).
    """
    raw = scaler.inverse_transform(centers)
    scored = []
    for index, (r_log, f_log, m_log) in enumerate(raw):
        quality = (-r_log) + f_log + m_log
        scored.append((quality, index, r_log, f_log, m_log))
    scored.sort(reverse=True)

    # Codes stay English; names are Vietnamese for dashboards
    # Mã giữ tiếng Anh; tên tiếng Việt cho dashboard
    palette = [
        ("cluster_champions", "Cụm tinh hoa"),
        ("cluster_loyal", "Cụm trung thành"),
        ("cluster_potential", "Cụm tiềm năng"),
        ("cluster_at_risk", "Cụm có nguy cơ"),
        ("cluster_hibernating", "Cụm ngủ đông"),
        ("cluster_other", "Cụm khác"),
    ]
    mapping: dict[int, tuple[str, str]] = {}
    for rank, (_quality, index, _r, _f, _m) in enumerate(scored):
        mapping[index] = palette[min(rank, len(palette) - 1)]
    return mapping
