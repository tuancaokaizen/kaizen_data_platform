"""Dataset bootstrap in Superset / Bootstrap dataset trên Superset."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any

import yaml

from superset_deploy.adapters.clickhouse import clickhouse_uri_candidates, mask_uri
from superset_deploy.api.query import query_filter
from superset_deploy.api.session import SupersetSession
from superset_deploy.config import DATASETS_PATH


@dataclass(frozen=True)
class ResourceRef:
    """Lightweight id holder / Giữ id DB hoặc dataset."""

    id: int


def load_datasets_config() -> dict[str, Any]:
    with DATASETS_PATH.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def find_database(api: SupersetSession, database_name: str) -> dict[str, Any] | None:
    rows = query_filter(api, "/api/v1/database/", [{"col": "database_name", "opr": "eq", "value": database_name}])
    return rows[0] if rows else None


def find_dataset(api: SupersetSession, table_name: str, schema: str) -> dict[str, Any] | None:
    rows = query_filter(
        api,
        "/api/v1/dataset/",
        [
            {"col": "table_name", "opr": "eq", "value": table_name},
            {"col": "schema", "opr": "eq", "value": schema},
        ],
    )
    return rows[0] if rows else None


def ensure_database(api: SupersetSession, database_name: str) -> ResourceRef:
    existing = find_database(api, database_name)
    if existing:
        print(f"✔ Database exists: {database_name} (id={existing['id']})")
        return ResourceRef(existing["id"])

    last_error = ""
    for uri in clickhouse_uri_candidates():
        payload = {
            "database_name": database_name,
            "sqlalchemy_uri": uri,
            "expose_in_sqllab": True,
            "allow_run_async": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
        }
        resp = api.post("/api/v1/database/", json=payload)
        if resp.status_code < 400:
            created = find_database(api, database_name)
            if not created:
                raise RuntimeError(f"Database created but not found: {database_name}")
            print(f"✔ Created database: {database_name} (id={created['id']})")
            print(f"  URI: {mask_uri(uri)}")
            return ResourceRef(created["id"])
        last_error = resp.text[:400]
        print(f"  ⚠ URI failed ({mask_uri(uri)}): {resp.status_code} {last_error[:120]}")

    print(
        "\n❌ Could not register ClickHouse in Superset. Common fixes:\n"
        "  1. Install driver in Superset pod:\n"
        "     pip install clickhouse-connect clickhouse-sqlalchemy\n"
        "  2. Set CLICKHOUSE_SQLALCHEMY_URI in config/.env\n"
        "  3. Verify Superset pod can reach ClickHouse\n",
        file=sys.stderr,
    )
    raise RuntimeError(f"Create database failed: {last_error}")


def configure_dataset_temporal(api: SupersetSession, dataset_id: int, dataset_cfg: dict[str, Any]) -> None:
    dttm_col = dataset_cfg.get("main_dttm_col", "NgayBaoCao")
    resp = api.get(f"/api/v1/dataset/{dataset_id}")
    resp.raise_for_status()
    detail = resp.json()["result"]

    columns = detail.get("columns") or []
    updated = False
    for col in columns:
        if col.get("column_name") == dttm_col:
            if not col.get("is_dttm"):
                col["is_dttm"] = True
                updated = True
            if not col.get("type_generic"):
                col["type_generic"] = 2
                updated = True

    payload: dict[str, Any] = {"main_dttm_col": dttm_col}
    if updated:
        payload["columns"] = columns

    if detail.get("main_dttm_col") != dttm_col or updated:
        put = api.put(f"/api/v1/dataset/{dataset_id}", json=payload)
        if put.status_code < 400:
            print(f"  ✔ Temporal column: {dttm_col} (dataset id={dataset_id})")
        else:
            print(f"  ⚠ Set temporal column failed: {put.status_code} {put.text[:160]}")


def ensure_dataset(api: SupersetSession, db: ResourceRef, dataset_cfg: dict[str, Any]) -> ResourceRef:
    table_name = dataset_cfg["table_name"]
    schema = dataset_cfg.get("schema", "report")
    existing = find_dataset(api, table_name, schema)
    if existing:
        refresh = api.put(f"/api/v1/dataset/{existing['id']}/refresh")
        if refresh.status_code >= 400:
            print(f"  ⚠ Refresh columns failed: {refresh.status_code} {refresh.text[:120]}")
        configure_dataset_temporal(api, existing["id"], dataset_cfg)
        print(f"  ✔ Dataset exists: {schema}.{table_name} (id={existing['id']})")
        return ResourceRef(existing["id"])

    payload = {"table_name": table_name, "schema": schema, "database": db.id}
    resp = api.post("/api/v1/dataset/", json=payload)
    if resp.status_code >= 400:
        print(f"  ❌ Create dataset {schema}.{table_name}: {resp.status_code} {resp.text}", file=sys.stderr)
        resp.raise_for_status()

    created = find_dataset(api, table_name, schema)
    if not created:
        raise RuntimeError(f"Dataset created but not found: {schema}.{table_name}")
    refresh = api.put(f"/api/v1/dataset/{created['id']}/refresh")
    if refresh.status_code >= 400:
        print(f"  ⚠ Refresh columns failed: {refresh.status_code} {refresh.text[:200]}")
    configure_dataset_temporal(api, created["id"], dataset_cfg)
    print(f"  ✔ Created dataset: {schema}.{table_name} (id={created['id']})")
    return ResourceRef(created["id"])


def bootstrap_all(api: SupersetSession | None = None) -> dict[str, int]:
    """Register DB + all datasets / Đăng ký DB và toàn bộ dataset."""
    client = api or SupersetSession()
    cfg = load_datasets_config()
    db = ensure_database(client, cfg.get("database_name", "kaizen_clickhouse_report"))
    mapping: dict[str, int] = {}
    for ds_cfg in cfg.get("datasets", []):
        ref = ensure_dataset(client, db, ds_cfg)
        mapping[ds_cfg["table_name"]] = ref.id
    return mapping


def resolve_dataset_ids(skip_bootstrap: bool) -> dict[str, int]:
    if not skip_bootstrap:
        return bootstrap_all()

    api = SupersetSession()
    mapping: dict[str, int] = {}
    for ds in load_datasets_config().get("datasets", []):
        found = find_dataset(api, ds["table_name"], ds.get("schema", "report"))
        if not found:
            raise RuntimeError(f"Dataset not found: {ds['table_name']}. Run: superset-deploy bootstrap")
        mapping[ds["table_name"]] = found["id"]
    return mapping
