"""Build solver input from dw facts and publish report.replenishment_plan.
Gom input solver từ fact dw và publish report.replenishment_plan.
"""
from __future__ import annotations

import logging
import math
from datetime import timedelta

from jobs.kaizen_analytics.serving_publish import ddl_replacing, publish_serving_stable
from jobs.kaizen_analytics.solvers.base import DemandSignal, ShopCapacity, SolverInput, StockPosition
from jobs.kaizen_analytics.solvers.factory import build_solver
from jobs.kaizen_analytics.steps.base import StepContext, StepResult

log = logging.getLogger(__name__)

COLUMNS = (
    "AsOfDate",
    "ShopCode",
    "ShopName",
    "ProductId",
    "ProductName",
    "OnHandQty",
    "ReorderPoint",
    "TargetLevel",
    "SuggestedQty",
    "EstimatedVolumeM3",
    "EstimatedCost",
    "SolverBackend",
    "Reason",
)


class ReplenishmentPlanStep:
    name = "replenishment_plan"

    def run(self, ctx: StepContext) -> StepResult:
        settings = ctx.settings
        demand_start = ctx.as_of - timedelta(days=settings.demand_lookback_days)

        demand_sql = f"""
        SELECT
            ShopCode,
            ProductId,
            avg(Quantity) AS DailyMean,
            stddevPop(Quantity) AS DailyStd,
            any(UnitCost) AS UnitCost
        FROM (
            SELECT
                ShopCode,
                ProductId,
                OrderDate,
                sum(Quantity) AS Quantity,
                avg(UnitCost) AS UnitCost
            FROM dw.fact_sale_transaction
            WHERE OrderDate >= toDate('{demand_start.isoformat()}')
              AND OrderDate <= toDate('{ctx.as_of.isoformat()}')
              AND ShopCode IS NOT NULL AND ProductId IS NOT NULL
              AND (IsGift = 0 OR IsGift IS NULL)
            GROUP BY ShopCode, ProductId, OrderDate
        )
        GROUP BY ShopCode, ProductId
        """
        demand_rows = ctx.client.query_dicts(
            demand_sql, ("ShopCode", "ProductId", "DailyMean", "DailyStd", "UnitCost")
        )

        stock_sql = f"""
        SELECT
            LocationId AS ShopCode,
            ProductId,
            argMax(OnHandQuantity, SnapshotDate) AS OnHandQty,
            argMax(OnHandVolumeM3, SnapshotDate) AS OnHandVolumeM3
        FROM dw.fact_inventory_snapshot
        WHERE LocationType = 'SHOP'
          AND SnapshotDate <= toDate('{ctx.as_of.isoformat()}')
          AND SnapshotDate >= toDate('{(ctx.as_of - timedelta(days=14)).isoformat()}')
        GROUP BY LocationId, ProductId
        """
        try:
            stock_rows = ctx.client.query_dicts(
                stock_sql, ("ShopCode", "ProductId", "OnHandQty", "OnHandVolumeM3")
            )
        except Exception as exc:
            log.warning("inventory snapshot unavailable: %s", exc)
            stock_rows = []

        volume_by_product: dict[str, float] = {}
        used_by_shop: dict[str, float] = {}
        for row in stock_rows:
            product_id = row["ProductId"]
            on_hand = float(row["OnHandQty"] or 0)
            volume = float(row["OnHandVolumeM3"] or 0)
            if on_hand > 0 and volume > 0:
                volume_by_product[product_id] = volume / on_hand
            used_by_shop[row["ShopCode"]] = used_by_shop.get(row["ShopCode"], 0.0) + volume

        # dim_shop carries capacity when shop_capacity serving is empty
        # dim_shop mang capacity khi serving shop_capacity đang trống
        shop_sql = """
        SELECT ShopCode, ShopName, StorageM3
        FROM dw.dim_shop
        WHERE ShopCode IS NOT NULL
        """
        try:
            shop_rows = ctx.client.query_dicts(shop_sql, ("ShopCode", "ShopName", "StorageM3"))
        except Exception:
            shop_rows = []
        shop_name = {row["ShopCode"]: row["ShopName"] or row["ShopCode"] for row in shop_rows}
        capacities = [
            ShopCapacity(
                shop_code=row["ShopCode"],
                storage_m3=float(row["StorageM3"] or 0) or 100.0,
                used_m3=used_by_shop.get(row["ShopCode"], 0.0),
            )
            for row in shop_rows
        ][: settings.max_shops]

        product_names = {}
        try:
            for product_id, name in ctx.client.query_rows(
                "SELECT ProductId, ProductName FROM dw.dim_product"
            ):
                product_names[product_id] = name or product_id
        except Exception:
            pass

        # Limit matrix size for lab CBC / Giới hạn kích thước ma trận cho CBC lab
        demand_rows = demand_rows[: settings.max_shops * settings.max_products]
        demands = [
            DemandSignal(
                shop_code=row["ShopCode"],
                product_id=row["ProductId"],
                daily_mean=max(float(row["DailyMean"] or 0), 0.0),
                daily_std=max(float(row["DailyStd"] or 0), math.sqrt(max(float(row["DailyMean"] or 0), 0.0))),
                unit_volume_m3=volume_by_product.get(row["ProductId"], 0.0005),
                unit_cost=max(float(row["UnitCost"] or 0), 0.0),
            )
            for row in demand_rows
        ]
        stocks = [
            StockPosition(
                shop_code=row["ShopCode"],
                product_id=row["ProductId"],
                on_hand=float(row["OnHandQty"] or 0),
            )
            for row in stock_rows
        ]

        solver = build_solver(settings.solver_backend)
        result = solver.solve(
            SolverInput(
                demands=demands,
                stocks=stocks,
                capacities=capacities,
                cover_days=settings.cover_days,
                service_z=settings.service_z,
            )
        )

        rows = [
            (
                ctx.as_of.isoformat(),
                line.shop_code,
                shop_name.get(line.shop_code, line.shop_code),
                line.product_id,
                product_names.get(line.product_id, line.product_id),
                line.on_hand,
                line.reorder_point,
                line.target_level,
                line.suggested_qty,
                round(line.estimated_volume_m3, 6),
                round(line.estimated_cost, 2),
                result.backend,
                line.reason,
            )
            for line in result.lines
            if line.suggested_qty > 0 or line.reason.startswith("below")
        ]

        written = publish_serving_stable(
            ctx.client,
            report_table="report.replenishment_plan",
            report_create_sql=_ensure_table_sql("report.replenishment_plan"),
            columns=COLUMNS,
            rows=rows,
            dw_table="dw.fact_replenishment_plan",
            dw_create_sql=_ensure_table_sql("dw.fact_replenishment_plan"),
        )
        return StepResult(
            self.name,
            written,
            f"backend={result.backend} objective={result.objective_cost:.0f} {result.notes}",
        )


def _ensure_table_sql(table: str) -> str:
    """DDL for replenishment derived fact / Serving / DDL derived fact bổ sung tồn / Serving."""
    return ddl_replacing(
        table,
        """
        AsOfDate Date,
        ShopCode String,
        ShopName String,
        ProductId String,
        ProductName String,
        OnHandQty Float64,
        ReorderPoint Float64,
        TargetLevel Float64,
        SuggestedQty Int64,
        EstimatedVolumeM3 Float64,
        EstimatedCost Float64,
        SolverBackend LowCardinality(String),
        Reason String
        """,
        "(AsOfDate, ShopCode, ProductId)",
    )
