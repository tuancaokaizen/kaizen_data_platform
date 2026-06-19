"""Build Superset chart params from YAML / Tạo params chart từ YAML."""

from __future__ import annotations

from typing import Any, Callable

from superset_deploy.api.session import datasource_token, sql_metric

ChartBuilder = Callable[[dict[str, Any], dict[str, Any], list[dict[str, Any]], list], dict[str, Any]]
_REGISTRY: dict[str, ChartBuilder] = {}


def register(viz: str) -> Callable[[ChartBuilder], ChartBuilder]:
    def decorator(fn: ChartBuilder) -> ChartBuilder:
        _REGISTRY[viz] = fn
        return fn

    return decorator


def _aggregate_sql(expression: str) -> str:
    """Wrap bare column names with SUM for GROUP BY queries / Bọc SUM cho cột thường khi GROUP BY."""
    expr = expression.strip()
    upper = expr.upper()
    if any(token in upper for token in ("SUM(", "COUNT(", "AVG(", "MIN(", "MAX(")):
        return expr
    return f"SUM({expr})"


def parse_metrics(spec: dict[str, Any], *, aggregate: bool = False) -> list[dict[str, Any]]:
    def to_metric(m: str) -> dict[str, Any]:
        return sql_metric(_aggregate_sql(m) if aggregate else m)

    if "metrics" in spec:
        return [to_metric(m) for m in spec["metrics"]]
    if "metric" in spec:
        return [to_metric(spec["metric"])]
    return []


def _base(spec: dict[str, Any], dataset_id: int, time_column: str) -> dict[str, Any]:
    base: dict[str, Any] = {
        "datasource": datasource_token(dataset_id),
        "viz_type": spec.get("viz", "table"),
        "time_range": "No filter",
        "adhoc_filters": [],
        "extra_form_data": {},
    }
    # Skip time axis for categorical / non-time charts / Bỏ trục thời gian cho chart phân loại
    if spec.get("categorical") or spec.get("viz") in ("pie", "treemap_v2", "table"):
        return base
    temporal_col = spec.get("time_column") or time_column
    if temporal_col:
        base["granularity_sqla"] = temporal_col
    return base


@register("big_number_total")
def _kpi(spec, base, metrics, groupby):
    base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
    base["header_font_size"] = spec.get("header_font_size", 0.55)
    base["subheader_font_size"] = spec.get("subheader_font_size", 0.18)
    return base


@register("echarts_timeseries_line")
@register("echarts_timeseries_bar")
@register("mixed_timeseries")
def _timeseries(spec, base, metrics, groupby):
    base["metrics"] = metrics or [sql_metric("COUNT(*)")]
    base["show_legend"] = True
    base["legendOrientation"] = spec.get("legend_orientation", "bottom")
    if spec.get("categorical") and spec.get("x"):
        # Top-N bar: dimension on x-axis, not time / Bar xếp hạng: dimension trên trục x
        base["x_axis"] = spec["x"]
        base["x_axis_force_categorical"] = True
        base["groupby"] = []
        base["show_legend"] = spec.get("show_legend", len(metrics) > 1)
    else:
        base["groupby"] = groupby
        if spec.get("x"):
            base["x_axis"] = spec["x"]
            base["time_grain_sqla"] = "P1D"
    if spec.get("orientation") == "horizontal":
        base["orientation"] = "horizontal"
    if spec.get("row_limit"):
        base["row_limit"] = spec["row_limit"]
    if spec.get("sort") == "desc":
        base["order_desc"] = True
    return base


@register("pie")
def _pie(spec, base, metrics, groupby):
    base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
    base["groupby"] = groupby
    base["row_limit"] = spec.get("row_limit", 25)
    base["show_legend"] = True
    base["legendOrientation"] = "bottom"
    base["label_type"] = spec.get("label_type", "key_value_percent")
    return base


@register("treemap_v2")
def _treemap(spec, base, metrics, groupby):
    base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
    base["groupby"] = groupby
    base["row_limit"] = spec.get("row_limit", 50)
    return base


@register("heatmap_v2")
def _heatmap(spec, base, metrics, groupby):
    base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
    if spec.get("x"):
        base["groupby"] = [spec["x"]]
    if spec.get("y"):
        base["groupby_columns"] = [spec["y"]]
    return base


@register("table")
def _table(spec, base, metrics, groupby):
    cols = spec.get("columns", [])
    if cols:
        base["all_columns"] = cols
        base["query_mode"] = "raw"
    else:
        base["groupby"] = spec.get("groupby", [])
        base["metrics"] = metrics
        base["query_mode"] = "aggregate"
    base["row_limit"] = spec.get("page_size", 100)
    base["include_time"] = False
    if spec.get("filter_sql"):
        base["adhoc_filters"] = [
            {"clause": "WHERE", "expressionType": "SQL", "sqlExpression": spec["filter_sql"]}
        ]
    return base


def build_chart_params(
    spec: dict[str, Any],
    dataset_id: int,
    time_column: str = "NgayBaoCao",
) -> dict[str, Any]:
    viz = spec.get("viz", "table")
    base = _base(spec, dataset_id, time_column)
    # Aggregate table: groupby without raw columns needs SUM() / Bảng tổng hợp cần SUM()
    aggregate_table = viz == "table" and spec.get("groupby") and not spec.get("columns")
    metrics = parse_metrics(spec, aggregate=aggregate_table)
    groupby = spec.get("groupby", [])
    builder = _REGISTRY.get(viz)
    if builder:
        return builder(spec, base, metrics, groupby)
    base["metrics"] = metrics or [sql_metric("COUNT(*)")]
    base["groupby"] = groupby
    return base
