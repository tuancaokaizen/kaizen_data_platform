"""Chart and dashboard naming helpers / Quy ước đặt tên chart và dashboard."""

from __future__ import annotations

from typing import Any

from superset_deploy.spec.registry import load_registry

# Stable lookup key in Superset metadata / Khóa tra cứu ổn định trong metadata
DEPLOY_KEY_PARAM = "kaizen_deploy_key"


def chart_deploy_key(dashboard_slug: str, chart_spec: dict[str, Any]) -> str:
    """Stable id for upsert — not shown in UI / Id ổn định để upsert — không hiển thị UI."""
    chart_key = chart_spec.get("chart", "chart")
    return f"{dashboard_slug}/{chart_key}"


def chart_display_title(chart_spec: dict[str, Any]) -> str:
    """Human-readable chart title from YAML / Tiêu đề chart đọc được từ YAML."""
    return chart_spec.get("title", chart_spec.get("chart", "Chart"))


def _dashboard_hint(dashboard_title: str, dashboard_slug: str) -> str:
    """Short label when slice names collide globally / Nhãn ngắn khi trùng tên slice."""
    reg = load_registry().get(dashboard_slug, {})
    label = reg.get("chart_label", "").strip()
    if label:
        return label
    if dashboard_title:
        words = dashboard_title.split()
        if len(words) >= 3:
            return " ".join(words[:3])
        return dashboard_title
    return dashboard_slug.replace("-", " ")


def resolve_chart_slice_name(
    chart_spec: dict[str, Any],
    dashboard_title: str,
    dashboard_slug: str,
    used_names: set[str],
) -> str:
    """Unique, human-readable slice_name for Superset UI / Tên slice unique, dễ đọc trên UI."""
    title = chart_display_title(chart_spec)
    if title not in used_names:
        used_names.add(title)
        return title
    hint = _dashboard_hint(dashboard_title, dashboard_slug)
    disambiguated = f"{title} · {hint}"
    if disambiguated not in used_names:
        used_names.add(disambiguated)
        return disambiguated
    keyed = f"{title} · {dashboard_slug}"
    used_names.add(keyed)
    return keyed


def chart_slice_name(dashboard_slug: str, chart_spec: dict[str, Any]) -> str:
    """Legacy helper — prefer resolve_chart_slice_name / Helper cũ — ưu tiên resolve_chart_slice_name."""
    return chart_deploy_key(dashboard_slug, chart_spec)
