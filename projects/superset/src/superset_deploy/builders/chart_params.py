"""Build Superset chart params from YAML / Tạo params chart từ YAML."""

from __future__ import annotations

import json
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


def _apply_color(spec: dict[str, Any], base: dict[str, Any]) -> None:
    """Apply categorical or continuous (gradient) color scheme / Áp màu phân loại hoặc gradient."""
    if spec.get("color_scheme"):
        base["color_scheme"] = spec["color_scheme"]
    if spec.get("linear_color_scheme"):
        base["linear_color_scheme"] = spec["linear_color_scheme"]


def _apply_metric_sort(spec: dict[str, Any], base: dict[str, Any], metrics: list[dict[str, Any]]) -> None:
    """Sort categorical bars by metric large→small (or reverse) / Sắp xếp bar theo metric lớn→nhỏ."""
    sort = spec.get("sort")
    if not sort:
        return
    descending = sort == "desc"
    ascending = not descending
    metric = metrics[0] if metrics else sql_metric("COUNT(*)")
    label = metric.get("label") or metric.get("sqlExpression") or "count"
    # Keys used across Superset 2.1–4.x for categorical X sort / Khóa sort trục phân loại
    base["order_desc"] = descending
    base["timeseries_limit_metric"] = metric
    # Sort the category axis by metric value (not alphabet) / Sort trục category theo metric, không theo ABC
    base["x_axis_sort"] = label
    base["x_axis_sort_asc"] = ascending
    # Also keep series-limit / orderby shapes some builds read / Giữ thêm shape một số bản đọc
    base["x_axis_sort_series"] = "sum"
    base["x_axis_sort_series_ascending"] = ascending
    base["sort_series_type"] = "sum"
    base["sort_series_ascending"] = ascending
    base["orderby"] = [[metric, ascending]]
    base["series_limit"] = int(spec.get("row_limit") or base.get("row_limit") or 100)


@register("echarts_timeseries_line")
@register("echarts_timeseries_bar")
@register("echarts_area")
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
    # Area / stacked contribution looks richer than plain bars / Area/stack nhìn đa dạng hơn bar thuần
    if spec.get("viz") == "echarts_area" or spec.get("area"):
        base["seriesType"] = "line"
        base["opacity"] = spec.get("opacity", 0.7)
        base["stack"] = spec.get("stack", "Stack")
    _apply_metric_sort(spec, base, metrics)
    _apply_color(spec, base)
    return base


@register("pie")
def _pie(spec, base, metrics, groupby):
    base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
    base["groupby"] = groupby
    base["row_limit"] = spec.get("row_limit", 25)
    base["show_legend"] = True
    base["legendOrientation"] = "bottom"
    base["label_type"] = spec.get("label_type", "key_value_percent")
    # Donut + sort by value / Vòng donut + sắp theo giá trị
    if spec.get("donut", True):
        base["donut"] = True
        base["innerRadius"] = spec.get("inner_radius", 40)
        base["outerRadius"] = spec.get("outer_radius", 70)
    base["sort_by_metric"] = True
    _apply_color(spec, base)
    return base


@register("treemap_v2")
def _treemap(spec, base, metrics, groupby):
    base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
    base["groupby"] = groupby
    base["row_limit"] = spec.get("row_limit", 50)
    base["show_labels"] = True
    base["show_upper_labels"] = True
    _apply_color(spec, base)
    return base


@register("heatmap_v2")
def _heatmap(spec, base, metrics, groupby):
    # Kept for older YAML; cluster UI may not register heatmap_v2 plugin.
    # Giữ cho YAML cũ; UI cluster có thể không đăng ký plugin heatmap_v2.
    # Prefer treemap_v2 / bar in new dashboards / Ưu tiên treemap_v2 / bar ở dashboard mới.
    base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
    if spec.get("x"):
        base["x_axis"] = spec["x"]
    if spec.get("y"):
        base["groupby"] = [spec["y"]]
    base.setdefault("legend_type", "continuous")
    base.setdefault("show_values", True)
    base.setdefault("show_legend", True)
    base.setdefault("sort_x_axis", "alpha_asc")
    base.setdefault("sort_y_axis", "alpha_asc")
    base.setdefault("bottom_margin", "auto")
    base.setdefault("left_margin", "auto")
    base["linear_color_scheme"] = spec.get("linear_color_scheme", "schemeYlOrRd")
    _apply_color(spec, base)
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
    # Order table rows by a column / Sắp hàng bảng theo cột
    if spec.get("order_by"):
        base["order_by_cols"] = [json_order_by(spec["order_by"], spec.get("sort", "desc") == "desc")]
    if spec.get("filter_sql"):
        base["adhoc_filters"] = [
            {"clause": "WHERE", "expressionType": "SQL", "sqlExpression": spec["filter_sql"]}
        ]
    return base


def json_order_by(column: str, descending: bool = True) -> str:
    """Superset table order_by_cols entry: [col, ascending] / Mục order_by_cols: [cột, tăng dần]."""
    ascending = not descending
    return json.dumps([column, ascending], ensure_ascii=False)


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
