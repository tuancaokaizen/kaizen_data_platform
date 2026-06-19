"""CLI entry point / Điểm vào CLI."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from superset_deploy.adapters.clickhouse import build_clickhouse_uri, mask_uri
from superset_deploy.adapters.minio import check_reachable, ensure_bucket, s3_client, upload_bytes, upload_file
from superset_deploy.api.dashboards import export_dashboard, find_dashboard_by_slug
from superset_deploy.api.datasets import bootstrap_all, load_datasets_config
from superset_deploy.api.session import SupersetSession
from superset_deploy.config import CONFIG_DIR, DASHBOARDS_DIR, env
from superset_deploy.deploy.enable_embed import run_enable_embed
from superset_deploy.deploy.export_registry import run_export_registry
from superset_deploy.deploy.save_query_context import run_save_query_context
from superset_deploy.deploy.orchestrator import DeployOptions, run_deploy
from superset_deploy.spec.loader import list_slugs, load_spec
from superset_deploy.spec.registry import load_registry


def cmd_bootstrap(_args: argparse.Namespace) -> int:
    print("=== Superset bootstrap — ClickHouse report views ===")
    print(f"Superset : {env('SUPERSET_HOST', 'http://localhost:32288')}")
    print(f"ClickHouse URI (try): {mask_uri(build_clickhouse_uri())}")
    bootstrap_all()
    print("\nDone. Open Superset → Datasets to build charts/dashboards.")
    return 0


def cmd_deploy(args: argparse.Namespace) -> int:
    return run_deploy(
        DeployOptions(
            force=args.force,
            dry_run=args.dry_run,
            skip_bootstrap=args.skip_bootstrap,
            dashboard_slug=args.dashboard,
        )
    )


def cmd_export(args: argparse.Namespace) -> int:
    slugs = args.slug or list_slugs()
    print("=== Export Superset dashboards ===")
    print(f"Superset: {env('SUPERSET_HOST', 'http://localhost:32288')}")
    print(f"Output  : {DASHBOARDS_DIR}")

    api = SupersetSession()
    exported = 0
    for slug in slugs:
        existing = find_dashboard_by_slug(api, slug)
        if not existing:
            print(f"  ⚠ Dashboard not found: {slug}")
            continue
        export_dashboard(api, existing["id"], DASHBOARDS_DIR / f"{slug}.zip")
        exported += 1

    print(f"\nDone. Exported {exported}/{len(slugs)} dashboards.")
    return 0 if exported else 1


def _build_manifest() -> dict:
    cfg = load_datasets_config()
    return {
        "version": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "database": cfg.get("database_name"),
        "datasets": [d["table_name"] for d in cfg.get("datasets", [])],
        "dashboards": list_slugs(),
    }


def _sync_yaml_configs(client, bucket: str, prefix: str) -> None:
    files = (
        ("config/datasets.yaml", CONFIG_DIR / "datasets.yaml"),
        ("config/defaults.yaml", CONFIG_DIR / "defaults.yaml"),
        ("config/meta.yaml", CONFIG_DIR / "meta.yaml"),
    )
    for key, path in files:
        if path.exists():
            upload_file(client, bucket, f"{prefix}{key}", path)


def _sync_dashboard_zips(client, bucket: str, prefix: str) -> int:
    count = 0
    if not DASHBOARDS_DIR.is_dir():
        return 0
    for zf in sorted(DASHBOARDS_DIR.glob("*.zip")):
        upload_file(client, bucket, f"{prefix}dashboards/{zf.name}", zf)
        count += 1
    return count


def cmd_sync_minio(args: argparse.Namespace) -> int:
    bucket = env("MINIO_BUCKET", "superset")
    prefix = env("MINIO_ASSETS_PREFIX", "assets/")
    endpoint = env("MINIO_ENDPOINT", "http://localhost:31900")

    print("=== Upload to MinIO ===")
    print(f"Endpoint: {endpoint}")
    print(f"Bucket  : s3://{bucket}/{prefix}")

    if not check_reachable(endpoint):
        return 1

    client = s3_client()
    ensure_bucket(client, bucket)

    manifest = _build_manifest()
    upload_bytes(client, bucket, f"{prefix}manifest.json", json.dumps(manifest, indent=2).encode(), "application/json")
    _sync_yaml_configs(client, bucket, prefix)

    if args.sync_all:
        count = _sync_dashboard_zips(client, bucket, prefix)
        print(f"\nDone. {count} dashboard ZIP(s) on MinIO.")
    return 0


def cmd_validate(_args: argparse.Namespace) -> int:
    spec = load_spec()
    slugs = list_slugs(spec)
    ds_names = {d["table_name"] for d in load_datasets_config().get("datasets", [])}
    reg = load_registry()
    ok = True
    for dash in spec.get("dashboards", []):
        slug = dash.get("slug", "")
        if slug not in reg:
            print(f"⚠ {slug}: missing from dashboard_registry.yaml")
        if dash.get("dataset") not in ds_names:
            print(f"❌ {slug}: unknown dataset {dash['dataset']}")
            ok = False
        if not dash.get("layout"):
            print(f"❌ {slug}: missing layout")
            ok = False
    for slug in reg:
        if slug not in slugs:
            print(f"❌ registry slug not in dashboard YAML: {slug}")
            ok = False
    print(f"✔ {len(slugs)} dashboards, {len(ds_names)} datasets, {len(reg)} registry entries")
    return 0 if ok else 1


def cmd_enable_embed(args: argparse.Namespace) -> int:
    return run_enable_embed(extra_domains=args.domain or [])


def cmd_save_query_context(_args: argparse.Namespace) -> int:
    return run_save_query_context()


def cmd_export_registry(_args: argparse.Namespace) -> int:
    return run_export_registry()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="superset-deploy", description="Deploy Superset dashboards from YAML")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("bootstrap", help="Register ClickHouse DB + datasets")

    deploy_p = sub.add_parser("deploy", help="Create/update dashboards from YAML")
    deploy_p.add_argument("--force", action="store_true")
    deploy_p.add_argument("--dry-run", action="store_true")
    deploy_p.add_argument("--skip-bootstrap", action="store_true")
    deploy_p.add_argument("--dashboard", metavar="SLUG", help="Deploy single dashboard")

    export_p = sub.add_parser("export", help="Export dashboards to ZIP")
    export_p.add_argument("--slug", action="append")

    sync_p = sub.add_parser("sync-minio", help="Upload assets to MinIO")
    sync_p.add_argument("--sync-all", action="store_true", default=True)

    sub.add_parser("validate", help="Validate YAML spec")

    embed_p = sub.add_parser("enable-embed", help="Enable embedded dashboards + print UUID map")
    embed_p.add_argument("--domain", action="append", help="Extra allowed origin for embed iframe")

    sub.add_parser("save-query-context", help="Populate chart query_context for embedded filters")

    sub.add_parser("export-registry", help="Export dashboard_registry.yaml → frontend biDashboardRegistry.ts")

    args = parser.parse_args(argv)
    handlers = {
        "bootstrap": cmd_bootstrap,
        "deploy": cmd_deploy,
        "export": cmd_export,
        "sync-minio": cmd_sync_minio,
        "validate": cmd_validate,
        "enable-embed": cmd_enable_embed,
        "save-query-context": cmd_save_query_context,
        "export-registry": cmd_export_registry,
    }
    return handlers[args.command](args)


if __name__ == "__main__":
    raise SystemExit(main())
