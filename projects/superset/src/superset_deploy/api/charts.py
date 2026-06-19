"""Chart CRUD / Tạo và cập nhật chart."""

from __future__ import annotations

import json
from typing import Any

import requests

from superset_deploy.api.query import query_filter
from superset_deploy.api.session import SupersetSession
from superset_deploy.builders.chart_params import build_chart_params
from superset_deploy.naming import DEPLOY_KEY_PARAM, chart_deploy_key


def chart_slice_name(dashboard_slug: str, chart_spec: dict[str, Any]) -> str:
    return chart_deploy_key(dashboard_slug, chart_spec)


def _find_chart_by_scan(api: SupersetSession, slice_name: str) -> dict[str, Any] | None:
    """Paginate charts when eq filter fails (e.g. '&' in title) / Quét trang khi filter eq lỗi."""
    page = 0
    page_size = 100
    while True:
        q = json.dumps({"page": page, "page_size": page_size}, ensure_ascii=False)
        resp = api.get("/api/v1/chart/", params={"q": q})
        resp.raise_for_status()
        body = resp.json()
        for row in body.get("result", []):
            if row.get("slice_name") == slice_name:
                return row
        total = body.get("count", 0)
        if (page + 1) * page_size >= total:
            break
        page += 1
    return None


def _find_chart_by_deploy_key_scan(api: SupersetSession, deploy_key: str) -> dict[str, Any] | None:
    """Find chart by kaizen_deploy_key or legacy slug/key slice_name / Tìm chart theo deploy key."""
    page = 0
    page_size = 100
    while True:
        q = json.dumps({"page": page, "page_size": page_size}, ensure_ascii=False)
        resp = api.get("/api/v1/chart/", params={"q": q})
        resp.raise_for_status()
        body = resp.json()
        for row in body.get("result", []):
            if row.get("slice_name") == deploy_key:
                return row
            raw_params = row.get("params") or "{}"
            try:
                params = json.loads(raw_params) if isinstance(raw_params, str) else raw_params
            except json.JSONDecodeError:
                params = {}
            if params.get(DEPLOY_KEY_PARAM) == deploy_key:
                return row
        total = body.get("count", 0)
        if (page + 1) * page_size >= total:
            break
        page += 1
    return None


def find_chart_by_name(api: SupersetSession, slice_name: str) -> dict[str, Any] | None:
    try:
        rows = query_filter(api, "/api/v1/chart/", [{"col": "slice_name", "opr": "eq", "value": slice_name}])
        if rows:
            return min(rows, key=lambda row: row["id"])
    except requests.HTTPError:
        # Superset 400 when slice_name contains '&' / Lỗi 400 khi tên chart có '&'
        pass
    return _find_chart_by_scan(api, slice_name)


def find_chart_by_deploy_key(api: SupersetSession, deploy_key: str) -> dict[str, Any] | None:
    found = find_chart_by_name(api, deploy_key)
    if found:
        return found
    return _find_chart_by_deploy_key_scan(api, deploy_key)


def upsert_chart(
    api: SupersetSession | None,
    dataset_id: int,
    deploy_key: str,
    slice_name: str,
    chart_spec: dict[str, Any],
    time_column: str,
    force: bool,
    dry_run: bool,
) -> int | None:
    existing = find_chart_by_deploy_key(api, deploy_key) if api else None
    params = build_chart_params(chart_spec, dataset_id, time_column=time_column)
    params[DEPLOY_KEY_PARAM] = deploy_key
    payload = {
        "slice_name": slice_name,
        "viz_type": chart_spec.get("viz", "table"),
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "params": json.dumps(params, ensure_ascii=False),
    }

    if dry_run:
        print(f"    [dry-run] chart: {slice_name} ({payload['viz_type']})")
        return existing["id"] if existing else -1

    assert api is not None
    if existing and not force:
        print(f"    ✔ Chart exists: {slice_name} (id={existing['id']})")
        return existing["id"]

    if existing:
        resp = api.put(f"/api/v1/chart/{existing['id']}", json=payload)
        if resp.status_code < 400:
            print(f"    ✔ Updated chart: {slice_name} (id={existing['id']})")
            return existing["id"]
        print(f"    ⚠ Update failed: {resp.status_code} {resp.text[:200]}")
        return existing["id"]

    resp = api.post("/api/v1/chart/", json=payload)
    if resp.status_code >= 400:
        print(f"    ❌ Create chart failed: {slice_name} — {resp.status_code} {resp.text[:300]}")
        return None
    chart_id = resp.json()["id"]
    print(f"    ✔ Created chart: {slice_name} (id={chart_id})")
    return chart_id


def link_charts_to_dashboard(api: SupersetSession, dash_id: int, chart_ids: list[int]) -> None:
    linked = 0
    for chart_id in chart_ids:
        resp = api.put(f"/api/v1/chart/{chart_id}", json={"dashboards": [dash_id]})
        if resp.status_code < 400:
            linked += 1
        else:
            print(f"  ⚠ Link chart {chart_id} → dashboard {dash_id}: {resp.status_code} {resp.text[:120]}")
    print(f"  ✔ Linked {linked}/{len(chart_ids)} charts to dashboard id={dash_id}")
