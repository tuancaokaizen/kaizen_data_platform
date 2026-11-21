#!/usr/bin/env python3
"""Pull RFM features from ClickHouse, train/val/test, print metrics JSON.
Lấy feature RFM từ ClickHouse, train/val/test, in metrics JSON.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow `python scripts/train_eval_report.py` from repo / Cho phép chạy trực tiếp từ repo
ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from jobs.kaizen_analytics.modeling.classifier import fit_segment_classifier  # noqa: E402
from jobs.kaizen_analytics.modeling.clustering import fit_customer_clusters  # noqa: E402
from jobs.kaizen_analytics.steps.customer_segments import label_segment, _quintile_scores, _gender_code  # noqa: E402
from jobs.kaizen_analytics.clickhouse_client import ClickHouseClient  # noqa: E402


def main() -> int:
    # Prefer NodePort when running outside the cluster / Ưu tiên NodePort khi chạy ngoài cluster
    os.environ.setdefault("CLICKHOUSE_HOST", os.getenv("CH_HOST", "127.0.0.1"))
    os.environ.setdefault("CLICKHOUSE_HTTP_PORT", os.getenv("CH_PORT", "30123"))

    as_of = date.fromisoformat(os.getenv("AS_OF", (date.today() - timedelta(days=1)).isoformat()))
    lookback = int(os.getenv("RFM_DAYS", "180"))
    start = as_of - timedelta(days=lookback)
    client = ClickHouseClient()
    sql = f"""
    SELECT
        o.CustomerId,
        dateDiff('day', max(o.OrderDate), toDate('{as_of.isoformat()}')) AS RecencyDays,
        count() AS Frequency,
        sum(o.NetAfterReturnAmount) AS Monetary,
        any(c.YearOfBirth) AS YearOfBirth,
        any(c.Gender) AS Gender
    FROM dw.fact_order_transaction AS o
    ANY LEFT JOIN dw.dim_customer AS c ON o.CustomerId = c.CustomerId
    WHERE o.CustomerId IS NOT NULL AND o.CustomerId != ''
      AND o.OrderDate >= toDate('{start.isoformat()}')
      AND o.OrderDate <= toDate('{as_of.isoformat()}')
    GROUP BY o.CustomerId
    """
    raw = client.query_dicts(
        sql,
        ("CustomerId", "RecencyDays", "Frequency", "Monetary", "YearOfBirth", "Gender"),
    )
    if not raw:
        print(json.dumps({"error": "no customers", "as_of": as_of.isoformat()}, indent=2))
        return 1

    recency = [float(row["RecencyDays"] or 0) for row in raw]
    frequency = [float(row["Frequency"] or 0) for row in raw]
    monetary = [float(row["Monetary"] or 0) for row in raw]
    ages = []
    gender_codes = []
    for row in raw:
        yob = int(float(row["YearOfBirth"] or 0))
        ages.append(float(max(as_of.year - yob, 0) if yob > 1900 else 0))
        gender_codes.append(_gender_code(row["Gender"] or ""))

    bins = 5
    r_scores = _quintile_scores([-v for v in recency], bins)
    f_scores = _quintile_scores(frequency, bins)
    m_scores = _quintile_scores(monetary, bins)
    rule_codes = [label_segment(r_scores[i], f_scores[i], m_scores[i])[0] for i in range(len(raw))]

    _, _, cluster_metrics = fit_customer_clusters(
        recency, frequency, monetary, n_clusters=5, random_state=42
    )
    _, clf_metrics = fit_segment_classifier(
        recency_days=recency,
        frequency=frequency,
        monetary=monetary,
        age=ages,
        gender_code=gender_codes,
        segment_codes=rule_codes,
        min_frequency=3,
        random_state=42,
    )

    from collections import Counter

    report = {
        "as_of": as_of.isoformat(),
        "lookback_days": lookback,
        "customers": len(raw),
        "rule_segment_counts": dict(Counter(rule_codes)),
        "cold_start_freq_lt_3": sum(1 for f in frequency if f < 3),
        "kmeans": cluster_metrics,
        "random_forest": clf_metrics,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
