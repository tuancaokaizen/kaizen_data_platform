"""Dashboard CRUD / Tạo và cập nhật dashboard."""

from __future__ import annotations

import json
from typing import Any

from superset_deploy.api.charts import link_charts_to_dashboard
from superset_deploy.api.query import query_filter
from superset_deploy.api.session import SupersetSession, dumps_json
from superset_deploy.builders.layout import build_position_json
from superset_deploy.config import env


def find_dashboard_by_slug(api: SupersetSession, slug: str) -> dict[str, Any] | None:
    rows = query_filter(api, "/api/v1/dashboard/", [{"col": "slug", "opr": "eq", "value": slug}])
    return rows[0] if rows else None


def upsert_dashboard(
    api: SupersetSession | None,
    dash_spec: dict[str, Any],
    chart_rows: list[list[tuple[int, int, int]]],
    chart_names: dict[int, str],
    native_filters: list[dict[str, Any]],
    force: bool,
    dry_run: bool,
) -> None:
    slug = dash_spec["slug"]
    title = dash_spec["title"]
    existing = find_dashboard_by_slug(api, slug) if api else None
    all_chart_ids = [cid for row in chart_rows for cid, _, _ in row]

    position = build_position_json(chart_rows, chart_names)
    refresh_frequency = int(dash_spec.get("refresh_frequency_seconds", 0))
    json_metadata = {
        "native_filter_configuration": native_filters,
        "color_scheme": "",
        "label_colors": {},
        "refresh_frequency": refresh_frequency,
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "default_filters": "{}",
    }

    payload = {
        "dashboard_title": title,
        "slug": slug,
        "published": True,
        "json_metadata": dumps_json(json_metadata),
        "position_json": dumps_json(position),
    }

    if dry_run:
        total = sum(len(r) for r in chart_rows)
        print(f"  [dry-run] dashboard: {title} ({slug}) — {total} charts")
        return

    assert api is not None
    host = env("SUPERSET_HOST", "http://localhost:32288")
    if existing and not force:
        link_charts_to_dashboard(api, existing["id"], all_chart_ids)
        print(f"  ✔ Dashboard exists: {title} (id={existing['id']})")
        print(f"    → {host}/superset/dashboard/{slug}/")
        return

    if existing and force:
        resp = api.put(f"/api/v1/dashboard/{existing['id']}", json=payload)
        if resp.status_code < 400:
            link_charts_to_dashboard(api, existing["id"], all_chart_ids)
            print(f"  ✔ Updated dashboard: {title} (id={existing['id']})")
            print(f"    → {host}/superset/dashboard/{slug}/")
            return
        print(f"  ⚠ Update dashboard failed: {resp.status_code} {resp.text[:200]}")

    resp = api.post("/api/v1/dashboard/", json=payload)
    if resp.status_code >= 400:
        print(f"  ❌ Create dashboard failed: {title} — {resp.status_code} {resp.text[:400]}")
        return
    dash_id = resp.json()["id"]
    link_charts_to_dashboard(api, dash_id, all_chart_ids)
    print(f"  ✔ Created dashboard: {title} (id={dash_id})")
    print(f"    → {host}/superset/dashboard/{slug}/")


def export_dashboard(api: SupersetSession, dash_id: int, out_path) -> None:
    resp = api.get("/api/v1/dashboard/export/", params={"q": json.dumps([dash_id])})
    if resp.status_code >= 400:
        raise RuntimeError(f"Export id={dash_id} failed: {resp.status_code} {resp.text[:200]}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(resp.content)
    print(f"  ✔ Exported {out_path.name} ({len(resp.content)} bytes)")
