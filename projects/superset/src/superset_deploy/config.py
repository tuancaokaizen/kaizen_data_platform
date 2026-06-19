"""Project paths and environment / Đường dẫn dự án và biến môi trường."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# projects/superset / Thư mục gốc superset
PROJECT_ROOT = Path(__file__).resolve().parents[2]
# projects/ — monorepo root for cross-project artifacts / Gốc monorepo cho artefact chung
MONOREPO_ROOT = PROJECT_ROOT.parent
BACKEND_SUPERSET_DIR = MONOREPO_ROOT / "backend" / "internal" / "superset"
EMBED_JSON_PATH = BACKEND_SUPERSET_DIR / "dashboards.embed.json"

CONFIG_DIR = PROJECT_ROOT / "config"
ASSETS_DIR = PROJECT_ROOT / "assets"
DASHBOARDS_DIR = ASSETS_DIR / "dashboards"

DEFAULTS_PATH = CONFIG_DIR / "defaults.yaml"
FILTERS_PATH = CONFIG_DIR / "filters" / "common.yaml"
DASHBOARDS_SPEC_DIR = CONFIG_DIR / "dashboards"
LEGACY_SPEC_PATH = CONFIG_DIR / "dashboards_spec.yaml"
DATASETS_PATH = CONFIG_DIR / "datasets.yaml"
ENV_PATH = CONFIG_DIR / ".env"

load_dotenv(ENV_PATH)


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()
