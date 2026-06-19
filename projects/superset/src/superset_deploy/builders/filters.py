"""Native filter builder / Tạo native filter Superset."""

from __future__ import annotations

from typing import Any


def stable_filter_id(dashboard_slug: str, column: str) -> str:
    """Deterministic filter ID — stable across deploys / ID filter ổn định qua các lần deploy."""
    safe = dashboard_slug.replace("/", "-")[:48]
    return f"NATIVE_FILTER-{safe}-{column}"


def build_native_filters(
    filter_specs: list[dict[str, Any]],
    dataset_id: int,
    dashboard_slug: str,
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

        entry: dict[str, Any] = {
            "id": stable_filter_id(dashboard_slug, col),
            "name": name,
            "filterType": ftype,
            "targets": [{"datasetId": dataset_id, "column": {"name": col}}],
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
            if select_all:
                entry["controlValues"]["searchAllOptions"] = False
            else:
                entry["controlValues"]["searchAllOptions"] = f.get("search", fd.get("search", False))

        configs.append(entry)
    return configs
