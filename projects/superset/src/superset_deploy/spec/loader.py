"""Load and merge dashboard YAML specs / Load và merge spec YAML."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from superset_deploy.config import (
    DASHBOARDS_SPEC_DIR,
    DEFAULTS_PATH,
    FILTERS_PATH,
    LEGACY_SPEC_PATH,
)
from superset_deploy.spec.registry import apply_registry


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data if isinstance(data, dict) else {}


def _load_filter_presets() -> dict[str, dict[str, Any]]:
    if not FILTERS_PATH.exists():
        return {}
    data = _load_yaml(FILTERS_PATH)
    return data.get("presets", {})


def _resolve_filters(dash: dict[str, Any], presets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if dash.get("native_filters"):
        return list(dash["native_filters"])
    refs = dash.get("filters", [])
    resolved: list[dict[str, Any]] = []
    for ref in refs:
        if isinstance(ref, dict):
            resolved.append(ref)
        elif ref in presets:
            resolved.append(dict(presets[ref]))
        else:
            raise KeyError(f"Unknown filter preset: {ref!r} in dashboard {dash.get('slug')}")
    return resolved


def _load_dashboard_files() -> list[dict[str, Any]]:
    if not DASHBOARDS_SPEC_DIR.is_dir():
        return []
    dashboards: list[dict[str, Any]] = []
    for path in sorted(DASHBOARDS_SPEC_DIR.glob("*.yaml")):
        dash = _load_yaml(path)
        if dash.get("slug"):
            dashboards.append(dash)
    return dashboards


def load_spec() -> dict[str, Any]:
    """Load merged spec from split files or legacy monolith / Load spec từ file tách hoặc monolith."""
    presets = _load_filter_presets()
    dashboards = _load_dashboard_files()

    if dashboards:
        defaults = _load_yaml(DEFAULTS_PATH) if DEFAULTS_PATH.exists() else {}
        meta = _load_yaml(DEFAULTS_PATH.parent / "meta.yaml") if (DEFAULTS_PATH.parent / "meta.yaml").exists() else {}
        for dash in dashboards:
            dash["native_filters"] = _resolve_filters(dash, presets)
        apply_registry(dashboards)
        return {
            "defaults": defaults,
            "dashboards": dashboards,
            "calculated_columns": meta.get("calculated_columns", []),
            "filter_matrix": meta.get("filter_matrix", []),
        }

    if LEGACY_SPEC_PATH.exists():
        return _load_yaml(LEGACY_SPEC_PATH)

    raise FileNotFoundError(
        f"No dashboard spec found. Expected {DASHBOARDS_SPEC_DIR}/*.yaml or {LEGACY_SPEC_PATH}"
    )


def list_slugs(spec: dict[str, Any] | None = None) -> list[str]:
    data = spec or load_spec()
    return [d["slug"] for d in data.get("dashboards", [])]
