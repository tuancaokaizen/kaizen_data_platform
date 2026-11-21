"""RFM rules + KMeans clusters + RandomForest cold-start labeling.
Luật RFM + cụm KMeans + gán nhãn cold-start bằng RandomForest.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import timedelta

import math

from jobs.kaizen_analytics.modeling.checkpoint import CheckpointStore
from jobs.kaizen_analytics.modeling.classifier import fit_segment_classifier, predict_segments
from jobs.kaizen_analytics.modeling.clustering import fit_customer_clusters
from jobs.kaizen_analytics.serving_publish import ddl_replacing, publish_serving_stable
from jobs.kaizen_analytics.steps.base import StepContext, StepResult

log = logging.getLogger(__name__)

# Derived fact includes train features + SplitSet (no separate Feature Store table)
# Derived fact gồm feature train + SplitSet (không tách bảng Feature Store)
SEGMENT_COLUMNS = (
    "AsOfDate",
    "CustomerId",
    "RecencyDays",
    "Frequency",
    "Monetary",
    "RecencyScore",
    "FrequencyScore",
    "MonetaryScore",
    "RfmScore",
    "SegmentCode",
    "SegmentName",
    "ClusterId",
    "ClusterCode",
    "ClusterName",
    "LabelSource",
    "Age",
    "Gender",
    "RecencyLog1p",
    "FrequencyLog1p",
    "MonetaryLog1p",
    "GenderCode",
    "SplitSet",
)

PROFILE_COLUMNS = (
    "AsOfDate",
    "SegmentCode",
    "SegmentName",
    "CustomerCount",
    "AvgRecencyDays",
    "AvgFrequency",
    "AvgMonetary",
    "P50Monetary",
    "SharePct",
    "AvgAge",
    "PctFemale",
    "PctMale",
    "PctOtherGender",
)

def label_segment(r: int, f: int, m: int) -> tuple[str, str]:
    """Map RFM scores → (stable English code, Vietnamese display name).
    Ánh xạ điểm RFM → (mã Anh ổn định, tên hiển thị tiếng Việt).
    """
    if r >= 4 and f >= 4 and m >= 4:
        return "champions", "Khách tinh hoa"
    if r >= 3 and f >= 3 and m >= 3:
        return "loyal", "Khách trung thành"
    if r >= 4 and f <= 2:
        return "new_high_intent", "Khách mới / tiềm năng cao"
    if r <= 2 and f >= 4:
        return "at_risk", "Khách có nguy cơ rời"
    if r <= 2 and f <= 2 and m <= 2:
        return "hibernating", "Khách ngủ đông"
    if m >= 4:
        return "big_spend", "Khách chi tiêu lớn"
    return "need_attention", "Cần chăm sóc"


def _quintile_scores(values: list[float], bins: int) -> list[int]:
    if not values:
        return []
    order = sorted(range(len(values)), key=lambda index: values[index])
    scores = [1] * len(values)
    n = len(values)
    for rank, index in enumerate(order):
        bucket = min(bins, max(1, int(rank * bins / n) + 1))
        scores[index] = bucket
    return scores


def _gender_code(value: str) -> int:
    text = (value or "").strip().upper()
    if text in {"M", "MALE", "NAM"}:
        return 1
    if text in {"F", "FEMALE", "NU", "NỮ"}:
        return 0
    return 2


class CustomerSegmentsStep:
    name = "customer_segments"

    def run(self, ctx: StepContext) -> StepResult:
        lookback = ctx.settings.rfm_lookback_days
        bins = ctx.settings.rfm_score_bins
        start = ctx.as_of - timedelta(days=lookback)
        sql = f"""
        SELECT
            o.CustomerId,
            dateDiff('day', max(o.OrderDate), toDate('{ctx.as_of.isoformat()}')) AS RecencyDays,
            count() AS Frequency,
            sum(o.NetAfterReturnAmount) AS Monetary,
            any(c.YearOfBirth) AS YearOfBirth,
            any(c.Gender) AS Gender
        FROM dw.fact_order_transaction AS o
        LEFT JOIN dw.dim_customer AS c ON o.CustomerId = c.CustomerId
        WHERE o.CustomerId IS NOT NULL AND o.CustomerId != ''
          AND o.OrderDate >= toDate('{start.isoformat()}')
          AND o.OrderDate <= toDate('{ctx.as_of.isoformat()}')
        GROUP BY o.CustomerId
        """
        raw = ctx.client.query_dicts(
            sql,
            ("CustomerId", "RecencyDays", "Frequency", "Monetary", "YearOfBirth", "Gender"),
        )
        # Ensure Serving shells exist before ALTER migrate / Tạo vỏ Serving trước khi ALTER
        ctx.client.execute(_ensure_segment_ddl("report.customer_segment"))
        ctx.client.execute(_ensure_profile_ddl("report.segment_profile"))
        _migrate_segment_columns(ctx)
        if not raw:
            # Publish empty Serving atomically so BI never sees half-old data
            # Publish Serving rỗng atomic để BI không thấy data nửa cũ
            publish_serving_stable(
                ctx.client,
                report_table="report.customer_segment",
                report_create_sql=_ensure_segment_ddl("report.customer_segment"),
                columns=SEGMENT_COLUMNS,
                rows=[],
                dw_table="dw.fact_customer_segment",
                dw_create_sql=_ensure_segment_ddl("dw.fact_customer_segment"),
            )
            publish_serving_stable(
                ctx.client,
                report_table="report.segment_profile",
                report_create_sql=_ensure_profile_ddl("report.segment_profile"),
                columns=PROFILE_COLUMNS,
                rows=[],
            )
            return StepResult(self.name, 0, "no customers in lookback")

        recency = [float(row["RecencyDays"] or 0) for row in raw]
        frequency = [float(row["Frequency"] or 0) for row in raw]
        monetary = [float(row["Monetary"] or 0) for row in raw]
        ages = []
        genders = []
        gender_codes = []
        for row in raw:
            yob = int(float(row["YearOfBirth"] or 0))
            age = max(ctx.as_of.year - yob, 0) if yob > 1900 else 0
            ages.append(float(age))
            gender = row["Gender"] or ""
            genders.append(gender)
            gender_codes.append(_gender_code(gender))

        r_scores = _quintile_scores([-value for value in recency], bins)
        f_scores = _quintile_scores(frequency, bins)
        m_scores = _quintile_scores(monetary, bins)

        rule_codes: list[str] = []
        rule_names: list[str] = []
        for index in range(len(raw)):
            code, name = label_segment(r_scores[index], f_scores[index], m_scores[index])
            rule_codes.append(code)
            rule_names.append(name)

        # Split + log features stay on the derived fact row (no separate FS table)
        # Split + feature log nằm trên dòng derived fact (không bảng FS riêng)
        split_labels = _assign_split_sets(len(raw), random_state=ctx.settings.model_random_state)
        idx_train = [i for i, s in enumerate(split_labels) if s == "train"]
        idx_val = [i for i, s in enumerate(split_labels) if s == "val"]
        idx_test = [i for i, s in enumerate(split_labels) if s == "test"]

        store = CheckpointStore(ctx.client)
        cluster_artifact = None
        cluster_ids = [0] * len(raw)
        cluster_metrics: dict = {"skipped": True, "reason": "enable_clustering=false"}
        # Optional KMeans — off by default (classification-only daily path)
        # KMeans tùy chọn — mặc định tắt (daily chỉ phân loại)
        if ctx.settings.enable_clustering:
            cluster_artifact, cluster_ids, cluster_metrics = fit_customer_clusters(
                recency,
                frequency,
                monetary,
                n_clusters=ctx.settings.cluster_k,
                random_state=ctx.settings.model_random_state,
                idx_train=idx_train,
                idx_val=idx_val,
                idx_test=idx_test,
            )
            store.save(
                model_name="customer_kmeans",
                as_of=ctx.as_of,
                algorithm="KMeans",
                artifact=cluster_artifact,
                metrics=cluster_metrics,
            )

        # RandomForest on rich-history customers; apply to cold-start
        # RandomForest trên khách đủ lịch sử; áp cho cold-start
        classifier, clf_metrics = fit_segment_classifier(
            recency_days=recency,
            frequency=frequency,
            monetary=monetary,
            age=ages,
            gender_code=gender_codes,
            segment_codes=rule_codes,
            min_frequency=ctx.settings.classifier_min_frequency,
            random_state=ctx.settings.model_random_state,
        )
        predicted = rule_codes
        if classifier is not None:
            store.save(
                model_name="customer_segment_rf",
                as_of=ctx.as_of,
                algorithm="RandomForestClassifier",
                artifact=classifier,
                metrics=clf_metrics,
            )
            predicted = predict_segments(
                classifier,
                recency_days=recency,
                frequency=frequency,
                monetary=monetary,
                age=ages,
                gender_code=gender_codes,
            )

        segment_name_by_code = {code: name for code, name in zip(rule_codes, rule_names)}
        # Ensure predicted codes still have display names
        # Đảm bảo mã dự đoán vẫn có tên hiển thị
        for code, name in zip(rule_codes, rule_names):
            segment_name_by_code.setdefault(code, name)

        rows = []
        for index, row in enumerate(raw):
            cluster_id = int(cluster_ids[index])
            if cluster_artifact is not None:
                cluster_code, cluster_name = cluster_artifact.cluster_labels[cluster_id]
            else:
                cluster_code, cluster_name = "", ""
            freq = int(frequency[index])
            if classifier is not None and freq < ctx.settings.classifier_min_frequency:
                segment_code = predicted[index]
                # Vietnamese source label for Superset filters / Nhãn nguồn tiếng Việt cho filter Superset
                label_source = "Mô hình RF"
            else:
                segment_code = rule_codes[index]
                label_source = "Luật RFM"
            segment_name = segment_name_by_code.get(segment_code, segment_code)
            r_days = float(recency[index])
            m_val = max(float(monetary[index]), 0.0)
            rows.append(
                (
                    ctx.as_of.isoformat(),
                    row["CustomerId"],
                    int(r_days),
                    freq,
                    m_val,
                    r_scores[index],
                    f_scores[index],
                    m_scores[index],
                    r_scores[index] * 100 + f_scores[index] * 10 + m_scores[index],
                    segment_code,
                    segment_name,
                    cluster_id,
                    cluster_code,
                    cluster_name,
                    label_source,
                    int(ages[index]),
                    genders[index],
                    round(math.log1p(r_days), 6),
                    round(math.log1p(freq), 6),
                    round(math.log1p(m_val), 6),
                    int(gender_codes[index]),
                    split_labels[index],
                )
            )

        # Derived fact @ DW then stable Serving swap / Derived fact @ DW rồi đổi Serving ổn định
        written = publish_serving_stable(
            ctx.client,
            report_table="report.customer_segment",
            report_create_sql=_ensure_segment_ddl("report.customer_segment"),
            columns=SEGMENT_COLUMNS,
            rows=rows,
            dw_table="dw.fact_customer_segment",
            dw_create_sql=_ensure_segment_ddl("dw.fact_customer_segment"),
        )
        profile_rows = _build_profiles(ctx.as_of.isoformat(), rows)
        # Profile is Serving aggregate only (ready-to-serve) / Profile chỉ Serving aggregate
        profile_written = publish_serving_stable(
            ctx.client,
            report_table="report.segment_profile",
            report_create_sql=_ensure_profile_ddl("report.segment_profile"),
            columns=PROFILE_COLUMNS,
            rows=profile_rows,
        )

        detail = (
            f"lookback={lookback}d derived_fact={written} "
            f"split=train:{len(idx_train)}/val:{len(idx_val)}/test:{len(idx_test)} "
            f"clusters={ctx.settings.cluster_k} "
            f"kmeans={ {k: cluster_metrics.get(k) for k in ('silhouette_train','silhouette_val','silhouette_test','train_rows','val_rows','test_rows')} } "
            f"profiles={profile_written} rf={clf_metrics}"
        )
        return StepResult(self.name, written, detail)


def _assign_split_sets(
    n: int, *, random_state: int = 42, train_ratio: float = 0.70
) -> list[str]:
    """Assign train/val/test once on derived-fact rows before fit.
    Gán train/val/test một lần trên dòng derived fact trước khi fit.
    """
    import numpy as np
    from sklearn.model_selection import train_test_split

    if n <= 0:
        return []
    if n < 5:
        return ["train"] * n
    indices = np.arange(n)
    idx_train, idx_temp = train_test_split(
        indices, test_size=1.0 - train_ratio, random_state=random_state
    )
    idx_val, idx_test = train_test_split(idx_temp, test_size=0.5, random_state=random_state)
    labels = ["train"] * n
    for i in idx_val:
        labels[int(i)] = "val"
    for i in idx_test:
        labels[int(i)] = "test"
    return labels


def _build_profiles(as_of: str, rows: list[tuple]) -> list[tuple]:
    # Indexes: Recency=2, Frequency=3, Monetary=4, SegmentCode=9, SegmentName=10, Age=15, Gender=16
    # Chỉ số: Recency=2 … Age=15, Gender=16
    buckets: dict[str, list] = defaultdict(list)
    names: dict[str, str] = {}
    for row in rows:
        code = row[9]
        names[code] = row[10]
        buckets[code].append(row)
    total = max(len(rows), 1)
    profiles = []
    for code, members in buckets.items():
        monetary = sorted(float(item[4]) for item in members)
        mid = monetary[len(monetary) // 2]
        ages = [float(item[15] or 0) for item in members]
        genders = [str(item[16] or "") for item in members]
        n = len(members)
        female = sum(1 for g in genders if _gender_code(g) == 0)
        male = sum(1 for g in genders if _gender_code(g) == 1)
        other = n - female - male
        profiles.append(
            (
                as_of,
                code,
                names[code],
                n,
                round(sum(float(item[2]) for item in members) / n, 2),
                round(sum(float(item[3]) for item in members) / n, 2),
                round(sum(float(item[4]) for item in members) / n, 2),
                round(mid, 2),
                round(100.0 * n / total, 2),
                round(sum(ages) / n, 2) if n else 0.0,
                round(100.0 * female / n, 2) if n else 0.0,
                round(100.0 * male / n, 2) if n else 0.0,
                round(100.0 * other / n, 2) if n else 0.0,
            )
        )
    profiles.sort(key=lambda item: -item[3])
    return profiles


def _migrate_segment_columns(ctx: StepContext) -> None:
    # Older RFM-only tables need additive columns before insert
    # Bảng RFM cũ cần thêm cột trước khi insert
    for ddl in (
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS ClusterId Int16 DEFAULT 0",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS ClusterCode LowCardinality(String) DEFAULT ''",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS ClusterName String DEFAULT ''",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS LabelSource LowCardinality(String) DEFAULT 'rule'",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS Age Int16 DEFAULT 0",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS Gender LowCardinality(String) DEFAULT ''",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS RecencyLog1p Float64 DEFAULT 0",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS FrequencyLog1p Float64 DEFAULT 0",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS MonetaryLog1p Float64 DEFAULT 0",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS GenderCode Int8 DEFAULT 2",
        "ALTER TABLE report.customer_segment ADD COLUMN IF NOT EXISTS SplitSet LowCardinality(String) DEFAULT 'train'",
        "ALTER TABLE report.segment_profile ADD COLUMN IF NOT EXISTS AvgAge Float64 DEFAULT 0",
        "ALTER TABLE report.segment_profile ADD COLUMN IF NOT EXISTS PctFemale Float64 DEFAULT 0",
        "ALTER TABLE report.segment_profile ADD COLUMN IF NOT EXISTS PctMale Float64 DEFAULT 0",
        "ALTER TABLE report.segment_profile ADD COLUMN IF NOT EXISTS PctOtherGender Float64 DEFAULT 0",
        # Keep DW derived fact in sync / Đồng bộ cột derived fact DW
        "ALTER TABLE dw.fact_customer_segment ADD COLUMN IF NOT EXISTS RecencyLog1p Float64 DEFAULT 0",
        "ALTER TABLE dw.fact_customer_segment ADD COLUMN IF NOT EXISTS FrequencyLog1p Float64 DEFAULT 0",
        "ALTER TABLE dw.fact_customer_segment ADD COLUMN IF NOT EXISTS MonetaryLog1p Float64 DEFAULT 0",
        "ALTER TABLE dw.fact_customer_segment ADD COLUMN IF NOT EXISTS GenderCode Int8 DEFAULT 2",
        "ALTER TABLE dw.fact_customer_segment ADD COLUMN IF NOT EXISTS SplitSet LowCardinality(String) DEFAULT 'train'",
    ):
        try:
            ctx.client.execute(ddl)
        except Exception as exc:
            log.warning("customer_segment migrate skipped: %s", exc)


def _ensure_segment_ddl(table: str) -> str:
    """DDL for segment derived fact (DW) or Serving / DDL derived fact phân khúc (DW) hoặc Serving."""
    return ddl_replacing(
        table,
        """
        AsOfDate Date,
        CustomerId String,
        RecencyDays Int32,
        Frequency Int32,
        Monetary Float64,
        RecencyScore Int8,
        FrequencyScore Int8,
        MonetaryScore Int8,
        RfmScore Int16,
        SegmentCode LowCardinality(String),
        SegmentName String,
        ClusterId Int16,
        ClusterCode LowCardinality(String),
        ClusterName String,
        LabelSource LowCardinality(String),
        Age Int16,
        Gender LowCardinality(String),
        RecencyLog1p Float64,
        FrequencyLog1p Float64,
        MonetaryLog1p Float64,
        GenderCode Int8,
        SplitSet LowCardinality(String)
        """,
        "(AsOfDate, CustomerId)",
    )


def _ensure_profile_ddl(table: str) -> str:
    """DDL for segment profile Serving mart / DDL mart Serving hồ sơ phân khúc."""
    return ddl_replacing(
        table,
        """
        AsOfDate Date,
        SegmentCode LowCardinality(String),
        SegmentName String,
        CustomerCount Int64,
        AvgRecencyDays Float64,
        AvgFrequency Float64,
        AvgMonetary Float64,
        P50Monetary Float64,
        SharePct Float64,
        AvgAge Float64,
        PctFemale Float64,
        PctMale Float64,
        PctOtherGender Float64
        """,
        "(AsOfDate, SegmentCode)",
    )

