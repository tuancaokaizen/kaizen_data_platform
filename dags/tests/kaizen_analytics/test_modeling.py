"""Unit tests for KMeans clustering and RandomForest cold-start labeling.
Kiểm thử phân cụm KMeans và gán nhãn cold-start RandomForest.
"""
from jobs.kaizen_analytics.modeling.classifier import fit_segment_classifier, predict_segments
from jobs.kaizen_analytics.modeling.clustering import fit_customer_clusters


def test_fit_customer_clusters_assigns_named_labels():
    recency = [5, 10, 200, 220, 30, 40, 180, 190]
    frequency = [20, 18, 1, 2, 12, 11, 1, 1]
    monetary = [5_000_000, 4_500_000, 50_000, 60_000, 2_000_000, 1_800_000, 40_000, 45_000]
    artifact, labels, metrics = fit_customer_clusters(recency, frequency, monetary, n_clusters=3)
    assert len(labels) == len(recency)
    assert set(labels) <= set(artifact.cluster_labels)
    assert metrics["train_rows"] + metrics["val_rows"] + metrics["test_rows"] == len(recency)
    # Champions centroid should beat hibernating on quality naming
    # Centroid champions phải xếp trên hibernating khi đặt tên
    codes = {code for code, _name in artifact.cluster_labels.values()}
    assert "cluster_champions" in codes


def test_random_forest_cold_start_predicts_known_classes():
    # Train on rich-history rows (freq>=3); predict on cold-start freq=1
    # Huấn luyện trên khách đủ lịch sử (freq>=3); dự đoán cold-start freq=1
    n = 120
    recency = [10.0] * 60 + [200.0] * 60
    frequency = [8.0] * 60 + [4.0] * 60
    monetary = [1_000_000.0] * 60 + [50_000.0] * 60
    age = [30.0] * n
    gender = [1] * 60 + [0] * 60
    segments = ["loyal"] * 60 + ["hibernating"] * 60
    artifact, metrics = fit_segment_classifier(
        recency_days=recency,
        frequency=frequency,
        monetary=monetary,
        age=age,
        gender_code=gender,
        segment_codes=segments,
        min_frequency=3,
        random_state=0,
    )
    assert artifact is not None
    assert metrics["skipped"] is False
    assert metrics["train_rows"] + metrics["val_rows"] + metrics["test_rows"] == 120
    assert metrics["test"]["accuracy"] >= 0.8
    assert metrics["val"]["accuracy"] >= 0.8
    assert metrics["accuracy"] >= 0.8
    predicted = predict_segments(
        artifact,
        recency_days=[12.0, 210.0],
        frequency=[1.0, 1.0],
        monetary=[900_000.0, 40_000.0],
        age=[30.0, 30.0],
        gender_code=[1, 0],
    )
    assert predicted[0] == "loyal"
    assert predicted[1] == "hibernating"
