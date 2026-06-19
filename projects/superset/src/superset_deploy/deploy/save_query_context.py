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

    columns = params.get("columns") or params.get("groupby") or []
    if chart.get("viz_type") == "table" and not columns:
        columns = params.get("all_columns") or []

    query: dict = {"columns": columns, "metrics": metrics, "row_limit": 1000}
    if not metrics and not columns:
        query["metrics"] = [params.get("metric") or {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "COUNT(*)"}]

    return {
        "datasource": {"id": ds_id, "type": ds_type},
        "force": True,
        "queries": [query],
        "form_data": form_data,
        "result_format": "json",
        "result_type": "full",
    }


def run_save_query_context() -> int:
    api = SupersetSession()
    charts = _list_all_charts(api)
    saved = 0
    skipped = 0
    failed = 0

    print("=== Save chart query_context (embedded guest filters) ===")
    for chart in sorted(charts, key=lambda row: row.get("id", 0)):
        chart_id = chart["id"]
        if chart.get("query_context"):
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
