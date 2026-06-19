"""Dashboard grid layout / Layout lưới dashboard."""

from __future__ import annotations

from typing import Any


def flatten_layout(layout: dict[str, Any]) -> list[list[dict[str, Any]]]:
    rows: list[list[dict[str, Any]]] = []
    for key in sorted(layout.keys()):
        row = layout[key]
        if isinstance(row, list):
            rows.append(row)
    return rows


def row_widths(charts_in_row: list[dict[str, Any]]) -> list[int]:
    n = len(charts_in_row)
    if n == 1:
        return [12]
    if n == 2:
        return [6, 6]
    if n == 3:
        return [4, 4, 4]
    return [max(3, 12 // n)] * n


def resolve_chart_height(
    width: int,
    viz: str,
    spec: dict[str, Any],
    layout_heights: dict[str, Any] | None = None,
) -> int:
    if spec.get("height"):
        return int(spec["height"])
    h = layout_heights or {}
    if viz == "big_number_total":
        return int(h.get("kpi", 36))
    if viz == "table":
        return int(h.get("table", 78 if width >= 8 else 58))
    if viz == "heatmap_v2":
        return int(h.get("heatmap", 68))
    if viz == "echarts_timeseries_bar" and spec.get("orientation") == "horizontal":
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
            meta: dict[str, Any] = {"chartId": chart_id, "width": width, "height": height}
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
