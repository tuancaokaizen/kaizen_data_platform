"""Build Superset chart params + native filters from YAML spec / Tạo params chart + native filter từ YAML."""

from __future__ import annotations

from typing import Any

from lib.superset_session import datasource_token, new_filter_id, sql_metric


def parse_metrics(spec: dict[str, Any]) -> list[dict[str, Any]]:
    if "metrics" in spec:
        return [sql_metric(m) for m in spec["metrics"]]
    if "metric" in spec:
        return [sql_metric(spec["metric"])]
    return []


def build_chart_params(
    spec: dict[str, Any],
    dataset_id: int,
    time_column: str = "NgayBaoCao",
) -> dict[str, Any]:
    viz = spec.get("viz", "table")
    ds = datasource_token(dataset_id)
    base: dict[str, Any] = {
        "datasource": ds,
        "viz_type": viz,
        "time_range": "No filter",
        "adhoc_filters": [],
        "extra_form_data": {},
    }

    # Required when dashboard native time filter is applied / Cần khi dùng filter ngày trên dashboard
    temporal_col = spec.get("time_column") or time_column
    if temporal_col:
        base["granularity_sqla"] = temporal_col

    metrics = parse_metrics(spec)
    groupby = spec.get("groupby", [])

    if viz == "big_number_total":
        base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
        base["header_font_size"] = spec.get("header_font_size", 0.55)
        base["subheader_font_size"] = spec.get("subheader_font_size", 0.18)
        return base

    if viz in ("echarts_timeseries_line", "echarts_timeseries_bar", "mixed_timeseries"):
        base["metrics"] = metrics or [sql_metric("COUNT(*)")]
        base["groupby"] = groupby
        base["show_legend"] = True
        base["legendOrientation"] = spec.get("legend_orientation", "bottom")
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

    if viz == "pie":
        base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
        base["groupby"] = groupby
        base["row_limit"] = spec.get("row_limit", 25)
        base["show_legend"] = True
        base["legendOrientation"] = "bottom"
        base["label_type"] = spec.get("label_type", "key_value_percent")
        return base

    if viz == "treemap_v2":
        base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
        base["groupby"] = groupby
        base["row_limit"] = spec.get("row_limit", 50)
        return base

    if viz == "heatmap_v2":
        base["metric"] = metrics[0] if metrics else sql_metric("COUNT(*)")
        if spec.get("x"):
            base["groupby"] = [spec["x"]]
        if spec.get("y"):
            base["groupby_columns"] = [spec["y"]]
        return base

    if viz == "table":
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
                {
                    "clause": "WHERE",
                    "expressionType": "SQL",
                    "sqlExpression": spec["filter_sql"],
                }
            ]
        return base

    base["metrics"] = metrics or [sql_metric("COUNT(*)")]
    base["groupby"] = groupby
    return base


def build_native_filters(
    filter_specs: list[dict[str, Any]],
    dataset_id: int,
    default_time: str = "Last 30 days",
    filter_defaults: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    fd = filter_defaults or {}
    configs: list[dict[str, Any]] = []
    for f in filter_specs:
        if f.get("visible") is False:
            continue

        col = f["column"]
        ftype = f.get("type", "filter_select")
        name = f.get("label", col)

        target = {"datasetId": dataset_id, "column": {"name": col}}
        entry: dict[str, Any] = {
            "id": new_filter_id(),
            "name": name,
            "filterType": ftype,
            "targets": [target],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "cascadeParentIds": [],
            "controlValues": {"enableEmptyFilter": not f.get("required", False)},
        }

        if ftype == "filter_time":
            entry["defaultDataMask"] = {"filterState": {"value": f.get("default", default_time)}}
        elif ftype == "filter_select":
            multi = f.get("multi", fd.get("multi", True))
            select_all = f.get("select_all", fd.get("select_all", multi))
            entry["controlValues"]["multiSelect"] = multi
            # Select All needs searchAllOptions=false (dynamic search hides the button) / Cần tắt dynamic search
            if select_all:
                entry["controlValues"]["searchAllOptions"] = False
            else:
                entry["controlValues"]["searchAllOptions"] = f.get("search", fd.get("search", False))

        configs.append(entry)
    return configs


def resolve_chart_height(
    width: int,
    viz: str,
    spec: dict[str, Any],
    layout_heights: dict[str, Any] | None = None,
) -> int:
    """Grid height from width + viz type / Chiều cao chart theo loại viz."""
    if spec.get("height"):
        return int(spec["height"])
    h = layout_heights or {}
    if viz == "big_number_total":
        return int(h.get("kpi", 36))
    if viz == "table":
        return int(h.get("table", 78 if width >= 8 else 58))
    if viz == "heatmap_v2":
        return int(h.get("heatmap", 68))
    if viz in ("echarts_timeseries_bar",) and spec.get("orientation") == "horizontal":
        return int(h.get("bar_horizontal", 64))
    if width >= 12:
        return int(h.get("full", 72))
    if width >= 6:
        return int(h.get("half", 58))
    return int(h.get("third", 48))


def build_position_json(
    rows: list[list[tuple[int, int, int]]],
    slice_names: dict[int, str] | None = None,
) -> dict[str, Any]:
    root_id = "ROOT_ID"
    grid_id = "GRID_ID"
    slice_names = slice_names or {}
    position: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        root_id: {"type": "ROOT", "id": root_id, "children": [grid_id]},
        grid_id: {"type": "GRID", "id": grid_id, "children": [], "parents": [root_id]},
    }

    for row_idx, row in enumerate(rows):
        row_id = f"ROW-{row_idx + 1}"
        position[grid_id]["children"].append(row_id)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": [grid_id],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

        for col_idx, (chart_id, width, height) in enumerate(row):
            chart_key = f"CHART-{row_idx + 1}-{col_idx + 1}"
            position[row_id]["children"].append(chart_key)
            meta: dict[str, Any] = {
                "chartId": chart_id,
                "width": width,
                "height": height,
            }
            if chart_id in slice_names:
                meta["sliceName"] = slice_names[chart_id]
            position[chart_key] = {
                "type": "CHART",
                "id": chart_key,
                "children": [],
                "parents": [row_id],
                "meta": meta,
            }

    return position
