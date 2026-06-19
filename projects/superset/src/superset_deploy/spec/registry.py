"""Dashboard registry — titles and report_key mapping / Registry metadata dashboard."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from superset_deploy.config import CONFIG_DIR

REGISTRY_PATH = CONFIG_DIR / "dashboard_registry.yaml"


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def load_registry() -> dict[str, dict[str, str]]:
    """Return slug → {report_key, title}. / Trả về slug → metadata."""
    if not REGISTRY_PATH.exists():
        return {}
    rows = _load_yaml(REGISTRY_PATH).get("dashboards", [])
    out: dict[str, dict[str, str]] = {}
    for row in rows:
        slug = row.get("slug", "")
        if slug:
            out[slug] = {
                "report_key": row.get("report_key", ""),
                "title": row.get("title", ""),
                "chart_label": row.get("chart_label", ""),
            }
    return out


def apply_registry(dashboards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Override dashboard title from registry when present / Ghi đè title từ registry."""
    reg = load_registry()
    for dash in dashboards:
        meta = reg.get(dash.get("slug", ""))
        if meta and meta.get("title"):
            dash["title"] = meta["title"]
    return dashboards


def registry_slugs() -> list[str]:
    return list(load_registry().keys())
