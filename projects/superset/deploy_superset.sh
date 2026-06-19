#!/bin/bash
# REGULAR DEPLOY: sync dashboards from config/dashboards/*.yaml → Superset API
# DEPLOY THƯỜNG: đồng bộ dashboard từ config/dashboards/ lên Superset
#
# Khi nào chạy:
#   - Lần đầu sau bootstrap.sh (tạo charts + dashboards)
#   - Sau khi sửa config/dashboards/*.yaml → dùng --force
#
# Usage:
#   ./deploy_superset.sh --tailscale              # tạo mới (bỏ qua nếu đã tồn tại)
#   ./deploy_superset.sh --tailscale --force      # cập nhật charts/dashboards
#   ./deploy_superset.sh --tailscale --dry-run    # preview only
set -euo pipefail

SUPERSET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SUPERSET_ROOT"
# shellcheck disable=SC1091
source "$SUPERSET_ROOT/scripts/lib/deploy_common.sh"

USE_TAILSCALE=false
FORCE=false
DRY_RUN=false
SKIP_VENV=false
SYNC_MINIO=false

usage() {
  cat <<'EOF'
deploy_superset.sh — regular Superset deploy (dashboards from YAML)

  ./deploy_superset.sh [options]

Reads config/dashboards/*.yaml and creates/updates charts + dashboards.

Options:
  --tailscale    Copy config/.env.tailscale.example → config/.env
  --force        Update existing charts/dashboards (use after editing YAML)
  --dry-run      Preview only, no API calls
  --skip-venv    Use system python3
  --minio        After deploy, export ZIP + upload to MinIO (needs MinIO reachable)
  -h, --help     Show help

Typical workflow:
  First time:  ./bootstrap.sh --tailscale && ./deploy_superset.sh --tailscale
  After edit:  ./deploy_superset.sh --force
  + MinIO:     ./deploy_superset.sh --force --minio   # auto port-forward MinIO :31900
EOF
}

for arg in "$@"; do
  case "$arg" in
    --tailscale) USE_TAILSCALE=true ;;
    --force) FORCE=true ;;
    --dry-run) DRY_RUN=true ;;
    --skip-venv) SKIP_VENV=true ;;
    --minio) SYNC_MINIO=true ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $arg"; usage; exit 1 ;;
  esac
done

superset_log "Deploy Superset dashboards"
superset_ensure_env "$USE_TAILSCALE"
superset_ensure_venv "$SKIP_VENV"
superset_preflight "$DRY_RUN"

DASH_ARGS=()
[ "$FORCE" = true ] && DASH_ARGS+=(--force)
[ "$DRY_RUN" = true ] && DASH_ARGS+=(--dry-run)

superset_run_module deploy ${DASH_ARGS[@]+"${DASH_ARGS[@]}"}

echo ""
superset_log "Deploy complete"
superset_print_dashboard_links
if [ "$FORCE" = false ] && [ "$DRY_RUN" = false ]; then
  echo ""
  echo "Dashboards already exist? Re-run with --force after editing config/dashboards/*.yaml."
fi

if [ "$SYNC_MINIO" = true ] && [ "$DRY_RUN" = false ]; then
  echo ""
  "$SUPERSET_ROOT/sync_minio.sh" ${SKIP_VENV:+--skip-venv}
fi
