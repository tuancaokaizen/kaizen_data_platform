"""Deploy orchestration / Điều phối deploy dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from superset_deploy.api.charts import upsert_chart
from superset_deploy.naming import chart_deploy_key, resolve_chart_slice_name
from superset_deploy.api.dashboards import upsert_dashboard
from superset_deploy.api.datasets import resolve_dataset_ids
from superset_deploy.api.session import SupersetSession
from superset_deploy.builders.filters import build_native_filters
from superset_deploy.builders.layout import flatten_layout, resolve_chart_height, row_widths
from superset_deploy.config import env
from superset_deploy.deploy.save_query_context import run_save_query_context
from superset_deploy.spec.loader import load_spec, list_slugs
from superset_deploy.spec.registry import registry_slugs


@dataclass
class DeployOptions:
    force: bool = False
    dry_run: bool = False
    skip_bootstrap: bool = False
    dashboard_slug: str | None = None


def run_deploy(opts: DeployOptions) -> int:
    spec = load_spec()
    defaults = spec.get("defaults", {})
    default_time = defaults.get("time_filter", {}).get("default", "Last 30 days")
    time_column = defaults.get("time_column", "NgayBaoCao")
    layout_heights = defaults.get("layout_heights", {})
    filter_defaults = defaults.get("filter_select", {})

    print("=== Deploy Superset dashboards ===")
    print(f"Superset: {env('SUPERSET_HOST', 'http://localhost:32288')}")

    if not opts.dry_run and not opts.skip_bootstrap:
        print("\n--- Bootstrap database + datasets ---")

    try:
        if opts.dry_run:
            from superset_deploy.api.datasets import load_datasets_config

            dataset_ids = {ds["table_name"]: 0 for ds in load_datasets_config().get("datasets", [])}
        else:
            dataset_ids = resolve_dataset_ids(opts.skip_bootstrap)
    except RuntimeError as exc:
        print(f"❌ {exc}")
        return 1

    api = None if opts.dry_run else SupersetSession()
    dashboards = spec.get("dashboards", [])
    deploy_order = {slug: idx for idx, slug in enumerate(registry_slugs())}
    dashboards = sorted(dashboards, key=lambda d: deploy_order.get(d.get("slug", ""), 999))
    used_slice_names: set[str] = set()
    if opts.dashboard_slug:
        dashboards = [d for d in dashboards if d.get("slug") == opts.dashboard_slug]
        if not dashboards:
            print(f"❌ Dashboard not found in spec: {opts.dashboard_slug}")
            print(f"   Available: {', '.join(list_slugs(spec))}")
            return 1

    for dash_spec in dashboards:
        title = dash_spec["title"]
        slug = dash_spec["slug"]
        dataset_name = dash_spec["dataset"]
        dataset_id = dataset_ids.get(dataset_name)
        if dataset_id is None and not opts.dry_run:
            print(f"❌ Unknown dataset: {dataset_name} for dashboard {title}")
            continue

        print(f"\n--- {title} ({slug}) ---")
        native_filters = build_native_filters(
            dash_spec.get("native_filters", []),
            dataset_id if dataset_id else 1,
            dashboard_slug=slug,
            default_time=default_time,
            filter_defaults=filter_defaults,
        )

        chart_grid: list[list[tuple[int, int, int]]] = []
        chart_names: dict[int, str] = {}

        for row_specs in flatten_layout(dash_spec.get("layout", {})):
            widths = row_widths(row_specs)
            row_chart_ids: list[tuple[int, int, int]] = []
            for idx, chart_spec in enumerate(row_specs):
                deploy_key = chart_deploy_key(slug, chart_spec)
                slice_name = resolve_chart_slice_name(chart_spec, title, slug, used_slice_names)
                chart_id = upsert_chart(
                    api,
                    dataset_id or 0,
                    deploy_key,
                    slice_name,
                    chart_spec,
                    time_column,
                    opts.force,
                    opts.dry_run,
                )
                if chart_id is not None:
                    viz = chart_spec.get("viz", "table")
                    height = resolve_chart_height(widths[idx], viz, chart_spec, layout_heights)
                    row_chart_ids.append((chart_id, widths[idx], height))
                    chart_names[chart_id] = slice_name
            if row_chart_ids:
                chart_grid.append(row_chart_ids)

        upsert_dashboard(api, dash_spec, chart_grid, chart_names, native_filters, opts.force, opts.dry_run)

    if not opts.dry_run and api is not None:
        print("\n--- Save chart query_context (embedded filters) ---")
        run_save_query_context()

    print("\nDone.")
    return 0
