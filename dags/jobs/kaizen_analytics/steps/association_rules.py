"""Market-basket FP-Growth, optionally cycled per RFM segment.
FP-Growth giỏ hàng, tùy chọn cycle theo từng phân khúc RFM.
"""
from __future__ import annotations

import logging
import random
from collections import defaultdict
from datetime import timedelta

from jobs.kaizen_analytics.mining.fpgrowth import mine_fpgrowth
from jobs.kaizen_analytics.serving_publish import ddl_replacing, publish_serving_stable
from jobs.kaizen_analytics.steps.base import StepContext, StepResult

log = logging.getLogger(__name__)

COLUMNS = (
    "AsOfDate",
    "SegmentCode",
    "SegmentName",
    "AntecedentProductIds",
    "ConsequentProductId",
    "AntecedentNames",
    "ConsequentName",
    "Support",
    "Confidence",
    "Lift",
    "SupportCount",
    "BasketSampleSize",
)

GLOBAL_SEGMENT = ("ALL", "Tất cả khách hàng")


class AssociationRulesStep:
    name = "association_rules"

    def run(self, ctx: StepContext) -> StepResult:
        settings = ctx.settings
        start = ctx.as_of - timedelta(days=settings.basket_lookback_days)
        # Baskets tagged with customer for segment join
        # Giỏ gắn CustomerId để join phân khúc
        sql = f"""
        SELECT
            OrderId,
            any(CustomerId) AS CustomerId,
            arrayStringConcat(groupUniqArray(ProductId), ',') AS Products
        FROM dw.fact_sale_transaction
        WHERE OrderDate >= toDate('{start.isoformat()}')
          AND OrderDate <= toDate('{ctx.as_of.isoformat()}')
          AND ProductId IS NOT NULL AND ProductId != ''
          AND (IsGift = 0 OR IsGift IS NULL)
        GROUP BY OrderId
        HAVING length(groupUniqArray(ProductId)) >= 2
        """
        raw = ctx.client.query_rows(sql)
        baskets_by_customer: list[tuple[str, list[str]]] = []
        for _order_id, customer_id, products in raw:
            items = [item for item in (products or "").split(",") if item]
            if len(items) >= 2:
                baskets_by_customer.append((customer_id or "", items))

        sample_size = settings.basket_sample_size
        if len(baskets_by_customer) > sample_size:
            rng = random.Random(ctx.as_of.toordinal())
            baskets_by_customer = rng.sample(baskets_by_customer, sample_size)

        segment_lookup = _load_segments(ctx) if settings.association_per_segment else {}
        groups: dict[tuple[str, str], list[list[str]]] = defaultdict(list)
        # Always mine a global slice / Luôn khai thác lát cắt toàn cục
        groups[GLOBAL_SEGMENT] = [items for _customer, items in baskets_by_customer]

        if settings.association_per_segment and segment_lookup:
            for customer_id, items in baskets_by_customer:
                key = segment_lookup.get(customer_id)
                if key is None:
                    continue
                groups[key].append(items)

        names = _product_names(ctx)
        rows: list[tuple] = []
        mined_groups = 0
        for (segment_code, segment_name), baskets in sorted(groups.items(), key=lambda item: item[0][0]):
            if segment_code != GLOBAL_SEGMENT[0] and len(baskets) < settings.min_baskets_per_segment:
                log.info(
                    "skip segment=%s baskets=%s < min=%s",
                    segment_code,
                    len(baskets),
                    settings.min_baskets_per_segment,
                )
                continue
            cap = (
                settings.max_rules
                if segment_code == GLOBAL_SEGMENT[0]
                else settings.max_rules_per_segment
            )
            rules = mine_fpgrowth(
                baskets,
                min_support=settings.min_support,
                min_confidence=settings.min_confidence,
                min_lift=settings.min_lift,
                max_itemset_size=settings.max_itemset_size,
                max_rules=cap,
            )
            mined_groups += 1
            for rule in rules:
                consequent = next(iter(rule.consequents))
                antecedents = sorted(rule.antecedents)
                rows.append(
                    (
                        ctx.as_of.isoformat(),
                        segment_code,
                        segment_name,
                        ",".join(antecedents),
                        consequent,
                        " + ".join(names.get(item, item) for item in antecedents),
                        names.get(consequent, consequent),
                        round(rule.support, 6),
                        round(rule.confidence, 6),
                        round(rule.lift, 6),
                        rule.support_count,
                        len(baskets),
                    )
                )

        # Soft global cap — keep diversity across segments (not only top global lift)
        # Trần toàn cục — giữ đa dạng theo phân loại (không chỉ lift toàn cục cao nhất)
        hard_cap = settings.max_rules + settings.max_rules_per_segment * max(len(groups) - 1, 0)
        if len(rows) > hard_cap:
            by_segment: dict[str, list[tuple]] = defaultdict(list)
            for row in rows:
                by_segment[str(row[1])].append(row)
            kept: list[tuple] = []
            for segment_code, seg_rows in sorted(by_segment.items()):
                seg_rows.sort(key=lambda item: (-float(item[9]), -float(item[8])))
                cap = (
                    settings.max_rules
                    if segment_code == GLOBAL_SEGMENT[0]
                    else settings.max_rules_per_segment
                )
                kept.extend(seg_rows[:cap])
            rows = kept

        ctx.client.execute(_ensure_table_sql("report.association_rule"))
        _migrate_columns(ctx)
        written = publish_serving_stable(
            ctx.client,
            report_table="report.association_rule",
            report_create_sql=_ensure_table_sql("report.association_rule"),
            columns=COLUMNS,
            rows=rows,
            dw_table="dw.fact_association_rule",
            dw_create_sql=_ensure_table_sql("dw.fact_association_rule"),
        )
        return StepResult(
            self.name,
            written,
            (
                f"baskets={len(baskets_by_customer)} groups={mined_groups} "
                f"lookback={settings.basket_lookback_days}d per_segment={settings.association_per_segment}"
            ),
        )


def _load_segments(ctx: StepContext) -> dict[str, tuple[str, str]]:
    """Map CustomerId → segment from DW derived fact (fallback Serving).
    Ánh xạ CustomerId → phân khúc từ derived fact DW (fallback Serving).
    """
    as_of = ctx.as_of.isoformat()
    # Prefer warehouse derived fact / Ưu tiên derived fact trong warehouse
    for table in ("dw.fact_customer_segment", "report.customer_segment"):
        try:
            rows = ctx.client.query_rows(
                f"""
                SELECT CustomerId, SegmentCode, SegmentName
                FROM {table}
                WHERE AsOfDate = toDate('{as_of}')
                """
            )
            if rows:
                return {
                    customer_id: (
                        segment_code or "unknown",
                        segment_name or segment_code or "unknown",
                    )
                    for customer_id, segment_code, segment_name in rows
                    if customer_id
                }
        except Exception as exc:
            log.warning("segment lookup via %s failed: %s", table, exc)
    return {}


def _product_names(ctx: StepContext) -> dict[str, str]:
    try:
        rows = ctx.client.query_rows(
            "SELECT ProductId, ProductName FROM dw.dim_product WHERE ProductId IS NOT NULL"
        )
        return {product_id: (name or product_id) for product_id, name in rows}
    except Exception as exc:
        log.warning("product name lookup failed: %s", exc)
        return {}


def _migrate_columns(ctx: StepContext) -> None:
    # Lab-safe: add segment columns if an older table shape already exists
    # An toàn lab: thêm cột phân khúc nếu bảng cũ đã tồn tại
    for ddl in (
        "ALTER TABLE report.association_rule ADD COLUMN IF NOT EXISTS SegmentCode LowCardinality(String) DEFAULT 'ALL'",
        "ALTER TABLE report.association_rule ADD COLUMN IF NOT EXISTS SegmentName String DEFAULT 'Tất cả khách hàng'",
    ):
        try:
            ctx.client.execute(ddl)
        except Exception as exc:
            log.warning("association_rule migrate skipped: %s", exc)


def _ensure_table_sql(table: str) -> str:
    """DDL for association derived fact / Serving / DDL derived fact luật bán kèm / Serving."""
    return ddl_replacing(
        table,
        """
        AsOfDate Date,
        SegmentCode LowCardinality(String),
        SegmentName String,
        AntecedentProductIds String,
        ConsequentProductId String,
        AntecedentNames String,
        ConsequentName String,
        Support Float64,
        Confidence Float64,
        Lift Float64,
        SupportCount Int64,
        BasketSampleSize Int64
        """,
        "(AsOfDate, SegmentCode, AntecedentProductIds, ConsequentProductId)",
    )
