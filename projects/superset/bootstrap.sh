#!/bin/bash
# ONE-TIME: venv + ClickHouse connection + 7 datasets in Superset
# CHẠY MỘT LẦN: venv + kết nối ClickHouse + đăng ký 7 dataset
#
# Khi nào chạy:
#   - Môi trường Superset mới
#   - Đổi CLICKHOUSE_* / connection string
#   - Sửa config/datasets.yaml (thêm dataset)
#
# Sau bootstrap, chạy lần đầu tạo dashboard:
#   ./deploy_superset.sh --tailscale
#
# Usage:
#   ./bootstrap.sh --tailscale
#   ./bootstrap.sh --skip-venv
set -euo pipefail

SUPERSET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SUPERSET_ROOT"
# shellcheck disable=SC1091
source "$SUPERSET_ROOT/scripts/lib/deploy_common.sh"

USE_TAILSCALE=false
SKIP_VENV=false

usage() {
  cat <<'EOF'
bootstrap.sh — ONE-TIME Superset + ClickHouse setup

  ./bootstrap.sh [options]

Registers ClickHouse database and 7 report datasets in Superset.
Does NOT create dashboards — use deploy_superset.sh for that.

Options:
  --tailscale    Copy config/.env.tailscale.example → config/.env
  --skip-venv    Use system python3, skip venv create
  -h, --help     Show help

Next step (first time):
  ./deploy_superset.sh --tailscale
EOF
}

for arg in "$@"; do
  case "$arg" in
    --tailscale) USE_TAILSCALE=true ;;
    --skip-venv) SKIP_VENV=true ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $arg"; usage; exit 1 ;;
  esac
done

superset_log "Superset bootstrap (one-time)"
superset_ensure_env "$USE_TAILSCALE"
superset_ensure_venv "$SKIP_VENV"
superset_preflight false

superset_run_module bootstrap

echo ""
superset_log "Bootstrap complete"
echo "ClickHouse DB + datasets registered in Superset."
echo ""
echo "Next:"
echo "  ./deploy_superset.sh --tailscale          # tạo 7 dashboard lần đầu"
echo "  ./deploy_superset.sh --tailscale --force  # sau khi sửa config/dashboards/*.yaml"
