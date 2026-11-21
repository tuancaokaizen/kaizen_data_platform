"""Save query_context on charts for embedded guest filters / Lưu query_context cho guest embed."""

from __future__ import annotations

import json

import requests

from superset_deploy.api.session import SupersetSession


def _list_all_charts(api: SupersetSession) -> list[dict]:
    """Paginate all charts / Lấy toàn bộ chart qua phân trang."""
    rows: list[dict] = []
    page = 0
    page_size = 100
    while True:
        q = json.dumps({"page": page, "page_size": page_size}, ensure_ascii=False)
        resp = api.get("/api/v1/chart/", params={"q": q})
        resp.raise_for_status()
        body = resp.json()
        rows.extend(body.get("result", []))
        total = body.get("count", 0)
        if (page + 1) * page_size >= total:
            break
        page += 1
    return rows


def _build_chart_data_payload(chart: dict) -> dict:
    params = json.loads(chart.get("params") or "{}")
    chart_id = chart["id"]
    ds_raw = params.get("datasource") or chart.get("datasource_name")
    if isinstance(ds_raw, str) and "__" in ds_raw:
        ds_id = int(ds_raw.split("__")[0])
        ds_type = ds_raw.split("__", 1)[1]
    else:
        ds_id = chart.get("datasource_id") or 0
        ds_type = chart.get("datasource_type") or "table"

    form_data = {
        **params,
        "slice_id": chart_id,
        "viz_type": chart.get("viz_type"),
        "datasource": params.get("datasource") or f"{ds_id}__{ds_type}",
    }
    metrics = []
    if params.get("metric"):
        metrics.append(params["metric"])
    metrics.extend(params.get("metrics") or [])

    viz = chart.get("viz_type") or params.get("viz_type")
    # heatmap_v2 columns = x_axis + groupby(Y) / Cột heatmap = trục X + groupby Y
    if viz == "heatmap_v2":
        columns = []
        if params.get("x_axis"):
            columns.append(params["x_axis"])
        gb = params.get("groupby") or []
        if isinstance(gb, str):
            columns.append(gb)
        else:
            columns.extend(gb)
    else:
        columns = list(params.get("columns") or params.get("groupby") or [])
        if viz == "table" and not columns:
            columns = list(params.get("all_columns") or [])
        # Categorical ECharts bar/line: dimension is x_axis, groupby often empty
        # Bar/line phân loại: dimension nằm ở x_axis, groupby thường rỗng
        x_axis = params.get("x_axis")
        if x_axis and x_axis not in columns and (
            params.get("x_axis_force_categorical")
            or viz
            in {
                "echarts_timeseries_bar",
                "echarts_timeseries_line",
                "echarts_area",
                "mixed_timeseries",
            }
        ):
            columns = [x_axis, *columns]

    row_limit = int(params.get("row_limit") or params.get("series_limit") or 1000)
    query: dict = {"columns": columns, "metrics": metrics, "row_limit": row_limit}
    if not metrics and not columns:
        query["metrics"] = [
            params.get("metric")
            or {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "COUNT(*)"}
        ]

    # Preserve metric sort (large→small) for ranking charts / Giữ sort metric lớn→nhỏ
    if params.get("orderby"):
        query["orderby"] = params["orderby"]
    elif params.get("timeseries_limit_metric") is not None and params.get("order_desc") is not None:
        # orderby second flag = ascending / Cờ thứ 2 = tăng dần
        query["orderby"] = [[params["timeseries_limit_metric"], not bool(params["order_desc"])]]

    # Categorical ranking is not a time series / Xếp hạng phân loại không phải chuỗi thời gian
    if params.get("x_axis_force_categorical"):
        query["is_timeseries"] = False

    return {
        "datasource": {"id": ds_id, "type": ds_type},
        "force": True,
        "queries": [query],
        "form_data": form_data,
        "result_format": "json",
        "result_type": "full",
    }


def run_save_query_context(force: bool = False) -> int:
    """Populate chart query_context; force=True overwrites existing.
    Lưu query_context; force=True ghi đè bản đã có.
    """
    api = SupersetSession()
    charts = _list_all_charts(api)
    saved = 0
    skipped = 0
    failed = 0

    print("=== Save chart query_context (embedded guest filters) ===")
    for chart in sorted(charts, key=lambda row: row.get("id", 0)):
        chart_id = chart["id"]
        # Skip only when already present and not forcing / Bỏ qua nếu đã có và không force
        if chart.get("query_context") and not force:
            skipped += 1
            continue
        try:
            payload = _build_chart_data_payload(chart)
            data_resp = api.post("/api/v1/chart/data", json=payload)
            if data_resp.status_code >= 400:
                print(f"  ❌ chart {chart_id} data: {data_resp.status_code} {data_resp.text[:120]}")
                failed += 1
                continue

            put_resp = api.put(
                f"/api/v1/chart/{chart_id}",
                json={
                    "query_context": json.dumps(payload, ensure_ascii=False, default=str),
                    "query_context_generation": True,
                },
            )
            if put_resp.status_code >= 400:
                print(f"  ❌ chart {chart_id} save: {put_resp.status_code} {put_resp.text[:120]}")
                failed += 1
                continue

            saved += 1
            name = chart.get("slice_name", "")[:60]
            print(f"  ✔ chart {chart_id}: {name}")
        except (requests.RequestException, json.JSONDecodeError, ValueError, KeyError) as exc:
            failed += 1
            print(f"  ❌ chart {chart_id}: {exc}")

    print(f"\nDone. saved={saved} skipped={skipped} failed={failed}")
    return 0 if failed == 0 else 1
