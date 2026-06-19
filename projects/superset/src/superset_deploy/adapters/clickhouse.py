"""ClickHouse URI helpers / Helper URI ClickHouse."""

from __future__ import annotations

from urllib.parse import quote_plus

from superset_deploy.config import env


def clickhouse_uri_candidates() -> list[str]:
    """Build URI variants to try / Các biến thể URI ClickHouse thử lần lượt."""
    explicit = env("CLICKHOUSE_SQLALCHEMY_URI")
    if explicit:
        return [explicit]

    host = env("CLICKHOUSE_SUPERSET_HOST") or env("CLICKHOUSE_HOST", "localhost")
    user = quote_plus(env("CLICKHOUSE_USER", "admin"))
    password = quote_plus(env("CLICKHOUSE_PASSWORD", "kaizen123"))
    database = env("CLICKHOUSE_DATABASE", "report")
    auth = f"{user}:{password}@{host}"

    preferred = env("CLICKHOUSE_SUPERSET_DRIVER", "native").lower()
    preferred_port = env("CLICKHOUSE_SUPERSET_PORT") or ("9000" if preferred == "native" else "8123")

    schemes: list[tuple[str, str, str]] = [
        ("clickhouse+native", preferred_port, "native"),
        ("clickhouse+http", "8123", "http"),
        ("clickhousedb", "8123", "connect"),
        ("clickhouse+native", "9000", "native"),
    ]
    seen: set[str] = set()
    ordered = sorted(schemes, key=lambda x: 0 if x[2] == preferred and x[1] == preferred_port else 1)
    variants: list[str] = []
    for scheme, port, _ in ordered:
        uri = f"{scheme}://{auth}:{port}/{database}"
        if uri not in seen:
            seen.add(uri)
            variants.append(uri)
    return variants


def build_clickhouse_uri() -> str:
    return clickhouse_uri_candidates()[0]


def mask_uri(uri: str) -> str:
    if "@" not in uri:
        return uri
    prefix, suffix = uri.split("@", 1)
    scheme = prefix.split("://", 1)[0]
    return f"{scheme}://***@{suffix}"
