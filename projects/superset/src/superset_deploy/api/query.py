"""Superset API query helpers / Helper truy vấn API Superset."""

from __future__ import annotations

import json
from typing import Any

from superset_deploy.api.session import SupersetSession


def query_filter(api: SupersetSession, path: str, filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # ensure_ascii=False keeps Vietnamese readable in query / Giữ tiếng Việt trong query
    q = json.dumps({"filters": filters}, ensure_ascii=False)
    resp = api.get(path, params={"q": q})
    resp.raise_for_status()
    return resp.json().get("result", [])
