"""Tunable knobs for analytics steps (env overrides for lab experiments).
Tham số chỉnh được cho các bước analytics (env ghi đè khi thử nghiệm lab).
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    # Empty env keeps the default / Env rỗng giữ mặc định
    raw = os.getenv(name)
    return default if raw is None or raw.strip() == "" else int(raw)


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return default if raw is None or raw.strip() == "" else float(raw)


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class AnalyticsSettings:
    """One place for every analytics threshold.
    Một chỗ chứa mọi ngưỡng analytics.
    """

    # RFM lookback and score bins / Cửa sổ RFM và thang điểm
    rfm_lookback_days: int = 180
    rfm_score_bins: int = 5

    # KMeans cluster labels fill MaCumMoHinh / TenCum (nhóm khách hàng)
    # Bật KMeans để điền mã/tên nhóm khách hàng trên báo cáo
    enable_clustering: bool = True
    cluster_k: int = 5
    classifier_min_frequency: int = 3
    model_random_state: int = 42

    # Association mining / Khai thác luật kết hợp
    basket_lookback_days: int = 90
    basket_sample_size: int = 30_000
    # Lab catalog is wide (~200 SKUs) — 0.3% support still yields actionable pairs
    # Catalog lab rộng (~200 SKU) — support 0.3% vẫn cho cặp luật hữu ích
    min_support: float = 0.001
    min_confidence: float = 0.08
    min_lift: float = 1.02
    max_itemset_size: int = 3
    max_rules: int = 200
    # Cycle: mine rules inside each RFM segment (+ global ALL)
    # Cycle: khai thác luật trong từng phân khúc RFM (+ toàn cục ALL)
    association_per_segment: bool = True
    max_rules_per_segment: int = 50
    min_baskets_per_segment: int = 80

    # Replenishment solver / Bộ tối ưu bổ sung tồn
    demand_lookback_days: int = 28
    cover_days: int = 7
    service_z: float = 1.65
    max_shops: int = 50
    max_products: int = 200
    solver_backend: str = "auto"  # auto | greedy | pulp

    @classmethod
    def from_env(cls) -> "AnalyticsSettings":
        return cls(
            rfm_lookback_days=_int("KAIZEN_ANALYTICS_RFM_DAYS", 180),
            rfm_score_bins=_int("KAIZEN_ANALYTICS_RFM_BINS", 5),
            enable_clustering=_bool("KAIZEN_ANALYTICS_ENABLE_CLUSTERING", True),
            cluster_k=_int("KAIZEN_ANALYTICS_CLUSTER_K", 5),
            classifier_min_frequency=_int("KAIZEN_ANALYTICS_RF_MIN_FREQ", 3),
            model_random_state=_int("KAIZEN_ANALYTICS_MODEL_SEED", 42),
            basket_lookback_days=_int("KAIZEN_ANALYTICS_BASKET_DAYS", 90),
            basket_sample_size=_int("KAIZEN_ANALYTICS_BASKET_SAMPLE", 120_000),
            min_support=_float("KAIZEN_ANALYTICS_MIN_SUPPORT", 0.001),
            min_confidence=_float("KAIZEN_ANALYTICS_MIN_CONFIDENCE", 0.08),
            min_lift=_float("KAIZEN_ANALYTICS_MIN_LIFT", 1.02),
            max_itemset_size=_int("KAIZEN_ANALYTICS_MAX_ITEMSET", 3),
            max_rules=_int("KAIZEN_ANALYTICS_MAX_RULES", 200),
            association_per_segment=_bool("KAIZEN_ANALYTICS_ASSOC_PER_SEGMENT", True),
            max_rules_per_segment=_int("KAIZEN_ANALYTICS_MAX_RULES_PER_SEG", 50),
            min_baskets_per_segment=_int("KAIZEN_ANALYTICS_MIN_BASKETS_PER_SEG", 80),
            demand_lookback_days=_int("KAIZEN_ANALYTICS_DEMAND_DAYS", 28),
            cover_days=_int("KAIZEN_ANALYTICS_COVER_DAYS", 7),
            service_z=_float("KAIZEN_ANALYTICS_SERVICE_Z", 1.65),
            max_shops=_int("KAIZEN_ANALYTICS_MAX_SHOPS", 50),
            max_products=_int("KAIZEN_ANALYTICS_MAX_PRODUCTS", 200),
            solver_backend=os.getenv("KAIZEN_ANALYTICS_SOLVER", "auto").strip().lower() or "auto",
        )
