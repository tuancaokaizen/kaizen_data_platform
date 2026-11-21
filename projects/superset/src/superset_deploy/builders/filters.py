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
    extra_dataset_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    fd = filter_defaults or {}
    # Apply filter to every dataset used by charts on the dashboard
    # Áp filter lên mọi dataset mà chart trên dashboard dùng
    target_dataset_ids: list[int] = []
    for did in [dataset_id, *(extra_dataset_ids or [])]:
        if did and did not in target_dataset_ids:
            target_dataset_ids.append(did)
    configs: list[dict[str, Any]] = []
    for f in filter_specs:
        if f.get("visible") is False:
            continue

        col = f["column"]
        ftype = f.get("type", "filter_select")
        name = f.get("label", col)

        # Superset 4.x: control `enableEmptyFilter` label = "Filter value is required"
        # (Form.Item required=enableEmptyFilter). True = bắt buộc chọn; False = cho trống.
        # Optional empty = no restriction (avoid IN () → No data) / Tùy chọn trống = không lọc
        value_required = bool(f.get("required", False))
        entry: dict[str, Any] = {
            "id": stable_filter_id(dashboard_slug, col),
            "name": name,
            "filterType": ftype,
            "targets": [{"datasetId": did, "column": {"name": col}} for did in target_dataset_ids],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "cascadeParentIds": [],
            "controlValues": {
                "enableEmptyFilter": value_required,
                # Do not auto-pick first item (can cause unexpected empty charts)
                # Không tự chọn phần tử đầu (tránh chart trống ngoài ý muốn)
                "defaultToFirstItem": False,
            },
        }

        if ftype == "filter_time":
            entry["defaultDataMask"] = {"filterState": {"value": f.get("default", default_time)}}
        elif ftype == "filter_select":
            multi = f.get("multi", fd.get("multi", True))
            select_all = f.get("select_all", fd.get("select_all", multi))
            entry["controlValues"]["multiSelect"] = multi
            # Select All needs searchAllOptions=false / Cần tắt dynamic search để hiện Select All
            if select_all:
                entry["controlValues"]["searchAllOptions"] = False
            else:
                entry["controlValues"]["searchAllOptions"] = f.get("search", fd.get("search", False))

            # Explicit default list, else options as safe default
            # Default tường minh, hoặc dùng options làm default an toàn
            default_vals = f.get("default")
            if default_vals is None and f.get("options"):
                default_vals = list(f["options"])
            if default_vals is not None:
                if not isinstance(default_vals, list):
                    default_vals = [default_vals]
                entry["defaultDataMask"] = {
                    "extraFormData": {
                        "filters": [
                            {
                                "col": col,
                                "op": "IN",
                                "val": default_vals,
                            }
                        ]
                    },
                    "filterState": {
                        "label": ", ".join(str(v) for v in default_vals),
                        "value": default_vals,
                    },
                }

        configs.append(entry)
    return configs
