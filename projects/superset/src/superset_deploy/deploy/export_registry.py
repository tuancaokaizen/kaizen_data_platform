"""Export dashboard registry for frontend / Xuất registry sang TypeScript frontend."""

from __future__ import annotations

from pathlib import Path

from superset_deploy.config import MONOREPO_ROOT
from superset_deploy.spec.registry import load_registry

FRONTEND_REGISTRY_PATH = (
    MONOREPO_ROOT / "frontend" / "src" / "config" / "biDashboardRegistry.ts"
)


def _ts_content(entries: list[tuple[str, str, str]]) -> str:
    lines = [
        "// AUTO-GENERATED — do not edit by hand",
        "// Sinh tự động bởi: python -m superset_deploy export-registry",
        "",
        "export type BiDashboardEntry = {",
        "  slug: string;",
        "  reportKey: string;",
        "  title: string;",
        "};",
        "",
        "export const BI_DASHBOARDS: BiDashboardEntry[] = [",
    ]
    for slug, report_key, title in entries:
        lines.append(
            f'  {{ slug: "{slug}", reportKey: "{report_key}", title: "{title}" }},'
        )
    lines.extend(
        [
            "];",
            "",
            "const SLUG_BY_REPORT_KEY = new Map<string, string>(",
            "  BI_DASHBOARDS.map((d) => [d.reportKey, d.slug])",
            ");",
            "",
            "/** Resolve Superset slug from report view key / Lấy slug Superset từ report key */",
            "export function supersetSlugForReportKey(key: string): string {",
            "  const slug = SLUG_BY_REPORT_KEY.get(key);",
            '  if (!slug) throw new Error(`No Superset dashboard for report key: ${key}`);',
            "  return slug;",
            "}",
            "",
            "export function biDashboardTitle(slug: string): string | undefined {",
            "  return BI_DASHBOARDS.find((d) => d.slug === slug)?.title;",
            "}",
            "",
        ]
    )
    return "\n".join(lines)


def run_export_registry() -> int:
    reg = load_registry()
    if not reg:
        print(f"❌ Empty registry: {FRONTEND_REGISTRY_PATH}")
        return 1

    entries: list[tuple[str, str, str]] = []
    for slug in sorted(reg.keys()):
        meta = reg[slug]
        entries.append((slug, meta["report_key"], meta["title"]))

    content = _ts_content(entries)
    FRONTEND_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    FRONTEND_REGISTRY_PATH.write_text(content, encoding="utf-8")
    print(f"✔ Wrote {FRONTEND_REGISTRY_PATH.relative_to(MONOREPO_ROOT)} ({len(entries)} dashboards)")
    return 0
