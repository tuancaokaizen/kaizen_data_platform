"""Superset REST session / Session REST Superset."""

from __future__ import annotations

import json
from typing import Any

import requests

from superset_deploy.config import env


class SupersetSession:
    """Authenticated Superset REST client / Client REST Superset đã đăng nhập."""

    def __init__(self) -> None:
        self.host = env("SUPERSET_HOST", "http://localhost:32288").rstrip("/")
        self.session = requests.Session()
        self._login()

    def _login(self) -> None:
        resp = self.session.post(
            f"{self.host}/api/v1/security/login",
            json={
                "username": env("SUPERSET_USERNAME", "admin"),
                "password": env("SUPERSET_PASSWORD", "admin"),
                "provider": "db",
                "refresh": True,
            },
            timeout=30,
        )
        resp.raise_for_status()
        token = resp.json()["access_token"]
        self.session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

        csrf_resp = self.session.get(f"{self.host}/api/v1/security/csrf_token/", timeout=30)
        csrf_resp.raise_for_status()
        csrf_token = csrf_resp.json()["result"]
        self.session.headers.update({"X-CSRFToken": csrf_token, "Referer": self.host})

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        return self.session.get(f"{self.host}{path}", timeout=60, **kwargs)

    def post(self, path: str, **kwargs: Any) -> requests.Response:
        return self.session.post(f"{self.host}{path}", timeout=120, **kwargs)

    def put(self, path: str, **kwargs: Any) -> requests.Response:
        return self.session.put(f"{self.host}{path}", timeout=120, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> requests.Response:
        return self.session.delete(f"{self.host}{path}", timeout=60, **kwargs)


def sql_metric(expression: str, label: str | None = None) -> dict[str, Any]:
    lbl = label or expression
    return {"expressionType": "SQL", "sqlExpression": expression, "label": lbl}


def datasource_token(dataset_id: int) -> str:
    return f"{dataset_id}__table"


def dumps_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)
