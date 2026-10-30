"""Enable Superset embedded dashboards and print UUID mapping / Bật nhúng dashboard và in mapping UUID."""

from __future__ import annotations

import json

import requests

from superset_deploy.api.query import query_filter
from superset_deploy.api.session import SupersetSession
from superset_deploy.config import EMBED_JSON_PATH, env


DEFAULT_EMBED_DOMAINS = [
    "http://localhost:8092",
    "http://127.0.0.1:8092",
    "http://localhost:4173",
    "http://127.0.0.1:4173",
    # Report UI NodePort (prod embed parent) / Origin UI báo cáo trên NodePort
    "http://192.168.100.172:31090",
]


def _verify_embed_route(host: str, embedded_uuid: str) -> int:
    """Return HTTP status for /embedded/<uuid> (404 = feature flag likely off)."""
    for path in (f"/embedded/{embedded_uuid}", f"/superset/embedded/{embedded_uuid}"):
        try:
            resp = requests.get(f"{host}{path}", timeout=10)
            if resp.status_code != 404:
                return resp.status_code
        except requests.RequestException:
            continue
    return 404


def _upsert_embedded(api: SupersetSession, dashboard_id: int, domains: list[str]) -> requests.Response:
    # Merge with existing domains so enable-embed is additive / Gộp domain cũ để không mất origin đã đăng ký
    existing_domains: list[str] = []
    resp = api.get(f"/api/v1/dashboard/{dashboard_id}/embedded")
    if resp.status_code == 200:
        existing_domains = resp.json().get("result", {}).get("allowed_domains") or []

    merged: list[str] = []
    for domain in [*domains, *existing_domains]:
        normalized = domain.rstrip("/")
        if normalized and normalized not in merged:
            merged.append(normalized)

    payload = {"allowed_domains": merged}
    if resp.status_code == 404:
        return api.post(f"/api/v1/dashboard/{dashboard_id}/embedded", json=payload)
    if resp.status_code == 200:
        return api.put(f"/api/v1/dashboard/{dashboard_id}/embedded", json=payload)
    return resp


def run_enable_embed(extra_domains: list[str] | None = None) -> int:
    api = SupersetSession()
    domains = DEFAULT_EMBED_DOMAINS.copy()
    if extra_domains:
        domains.extend(extra_domains)
    # Parent frame origins only — do NOT add SUPERSET_HOST (32288) / Chỉ origin trang cha
    embed_domain = env("SUPERSET_EMBED_DOMAIN", "http://localhost:8092").rstrip("/")
    if embed_domain and embed_domain not in domains:
        domains.append(embed_domain)

    extra_env = env("SUPERSET_EMBED_EXTRA_DOMAINS", "")
    for part in extra_env.replace(",", " ").split():
        part = part.strip().rstrip("/")
        if part and part not in domains:
            domains.append(part)

    rows = query_filter(api, "/api/v1/dashboard/", [])

    print("=== Enable Superset embedded dashboards ===")
    print(f"Allowed parent-frame origins ({len(domains)}):")
    for domain in domains:
        print(f"  • {domain}")

    mapping: dict[str, dict[str, str | int]] = {}
    for row in sorted(rows, key=lambda item: item.get("id", 0)):
        slug = row.get("slug", "")
        dashboard_id = row["id"]
        resp = _upsert_embedded(api, dashboard_id, domains)
        if resp.status_code >= 400:
            print(f"  ❌ {slug}: {resp.status_code} {resp.text[:200]}")
            continue
        embedded_uuid = resp.json().get("result", {}).get("uuid", "")
        mapping[slug] = {"dashboardId": dashboard_id, "embeddedUuid": embedded_uuid}
        print(f"  ✔ {slug} → {embedded_uuid}")

    if not mapping:
        return 1

    sample_uuid = next(iter(mapping.values()))["embeddedUuid"]
    host = env("SUPERSET_HOST", "http://localhost:32288").rstrip("/")
    route_status = _verify_embed_route(host, str(sample_uuid))
    print(f"\n=== Embed route check: GET /embedded/{sample_uuid} → HTTP {route_status} ===")
    if route_status == 404:
        print(
            "\n❌ Superset trả 404 cho /embedded/<uuid> — thường do FEATURE_FLAGS.EMBEDDED_SUPERSET chưa bật.\n"
            "   Sửa superset_config.py trên cluster (xem config/superset_embed_server_config.py.example),\n"
            "   restart pod Superset, rồi chạy lại: python -m superset_deploy enable-embed\n"
        )
        return 1

    if route_status == 403:
        print(
            "\n⚠️  /embedded/ trả 403 — kiểm tra:\n"
            "   1) ./scripts/grant_guest_embed_permissions.sh\n"
            "   2) allowed_domains có khớp origin UI (vd. http://<ip>:8092)?\n"
            "   3) Refresh tab Phân tích BI\n"
        )

    print("\n--- Go mapping (internal/superset/dashboards.go) ---")
    print("var DashboardBySlug = map[string]EmbeddedDashboard{")
    for slug, info in mapping.items():
        print(
            f'\t"{slug}": {{DashboardID: {info["dashboardId"]}, '
            f'EmbeddedUUID: "{info["embeddedUuid"]}"}},'
        )
    print("}")

    _write_embed_json(mapping)

    print("\nJSON:", json.dumps(mapping, indent=2))
    return 0


def _write_embed_json(mapping: dict[str, dict[str, str | int]]) -> None:
    """Persist embed UUIDs for Go backend // Ghi UUID nhúng cho backend Go."""
    payload = {
        slug: {
            "dashboardId": info["dashboardId"],
            "embeddedUuid": info["embeddedUuid"],
        }
        for slug, info in sorted(mapping.items())
    }
    EMBED_JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    EMBED_JSON_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"\n✔ Wrote {EMBED_JSON_PATH}")
