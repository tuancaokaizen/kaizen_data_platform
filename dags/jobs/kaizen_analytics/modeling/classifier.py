"""RandomForest segment classifier with train / val / test split.
Bộ phân loại RandomForest với tách tập train / val / test.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder


@dataclass
class ClassifierArtifact:
    model: RandomForestClassifier
    label_encoder: LabelEncoder
    feature_names: list[str]


FEATURE_NAMES = ["recency_days", "frequency", "monetary", "age", "gender_code"]


def fit_segment_classifier(
    *,
    recency_days: Sequence[float],
    frequency: Sequence[float],
    monetary: Sequence[float],
    age: Sequence[float],
    gender_code: Sequence[int],
    segment_codes: Sequence[str],
    min_frequency: int = 3,
    random_state: int = 42,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
) -> tuple[ClassifierArtifact | None, dict]:
    """Train RF on rich-history customers; report train/val/test metrics.
    Huấn luyện RF trên khách đủ lịch sử; báo metrics train/val/test.
    """
    if abs(train_ratio + val_ratio + test_ratio - 1.0) > 1e-6:
        raise ValueError("train_ratio + val_ratio + test_ratio must equal 1")

    x_rows = []
    y_rows = []
    for index, code in enumerate(segment_codes):
        if frequency[index] < min_frequency:
            continue
        x_rows.append(
            [
                float(recency_days[index]),
                float(frequency[index]),
                float(monetary[index]),
                float(age[index]),
                int(gender_code[index]),
            ]
        )
        y_rows.append(code)

    metrics: dict = {
        "train_rows": 0,
        "val_rows": 0,
        "test_rows": 0,
        "labeled_rows": len(x_rows),
        "classes": sorted(set(y_rows)),
        "split": {"train": train_ratio, "val": val_ratio, "test": test_ratio},
    }
    if len(x_rows) < 50 or len(set(y_rows)) < 2:
        return None, {**metrics, "skipped": True, "reason": "insufficient_labeled_rows"}

    encoder = LabelEncoder()
    y = encoder.fit_transform(y_rows)
    x = np.asarray(x_rows, dtype=float)
    stratify = y if _can_stratify(y) else None

    # First cut: train vs temp(val+test) / Cắt 1: train vs temp(val+test)
    x_train, x_temp, y_train, y_temp = train_test_split(
        x,
        y,
        test_size=(val_ratio + test_ratio),
        random_state=random_state,
        stratify=stratify,
    )
    # Second cut: val vs test from temp / Cắt 2: val vs test từ temp
    relative_test = test_ratio / (val_ratio + test_ratio)
    temp_stratify = y_temp if _can_stratify(y_temp) else None
    x_val, x_test, y_val, y_test = train_test_split(
        x_temp,
        y_temp,
        test_size=relative_test,
        random_state=random_state,
        stratify=temp_stratify,
    )

    model = RandomForestClassifier(
        n_estimators=120,
        max_depth=8,
        min_samples_leaf=5,
        random_state=random_state,
        n_jobs=2,
        class_weight="balanced_subsample",
    )
    # Fit only on train / Chỉ fit trên train
    model.fit(x_train, y_train)

    train_m = _score_split(model, x_train, y_train)
    val_m = _score_split(model, x_val, y_val)
    test_m = _score_split(model, x_test, y_test)
    metrics.update(
        {
            "skipped": False,
            "train_rows": int(len(x_train)),
            "val_rows": int(len(x_val)),
            "test_rows": int(len(x_test)),
            "train": train_m,
            "val": val_m,
            "test": test_m,
            # Back-compat aliases = test set / Alias tương thích = tập test
            "accuracy": test_m["accuracy"],
            "f1_macro": test_m["f1_macro"],
            "feature_importance": {
                name: float(score) for name, score in zip(FEATURE_NAMES, model.feature_importances_)
            },
        }
    )
    artifact = ClassifierArtifact(
        model=model,
        label_encoder=encoder,
        feature_names=list(FEATURE_NAMES),
    )
    return artifact, metrics


def predict_segments(
    artifact: ClassifierArtifact,
    *,
    recency_days: Sequence[float],
    frequency: Sequence[float],
    monetary: Sequence[float],
    age: Sequence[float],
    gender_code: Sequence[int],
) -> list[str]:
    matrix = np.column_stack(
        [
            np.asarray(recency_days, dtype=float),
            np.asarray(frequency, dtype=float),
            np.asarray(monetary, dtype=float),
            np.asarray(age, dtype=float),
            np.asarray(gender_code, dtype=float),
        ]
    )
    encoded = artifact.model.predict(matrix)
    return artifact.label_encoder.inverse_transform(encoded).tolist()


def _score_split(model: RandomForestClassifier, x: np.ndarray, y: np.ndarray) -> dict:
    pred = model.predict(x)
    return {
        "accuracy": float(accuracy_score(y, pred)),
        "f1_macro": float(f1_score(y, pred, average="macro", zero_division=0)),
        "rows": int(len(y)),
    }


def _can_stratify(y: np.ndarray) -> bool:
    _, counts = np.unique(y, return_counts=True)
    return bool(counts.min() >= 2)
