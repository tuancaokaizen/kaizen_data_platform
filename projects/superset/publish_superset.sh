#!/bin/bash
# publish_superset.sh — One command: deploy YAML → (optional MinIO) → embed → sync FE/BE
# Một lệnh: deploy YAML → (tùy chọn MinIO) → nhúng BI → đồng bộ frontend/backend
#
# Dev (sửa YAML, xem tab Phân tích BI):
#   ./publish_superset.sh --tailscale --force --embed
#
# Dev + backup MinIO:
#   ./publish_superset.sh --tailscale --force --embed --minio
#
# Cluster khác — import từ MinIO rồi nhúng:
#   ./publish_superset.sh --tailscale --import-minio --embed --domain https://reports.example.com
#
set -euo pipefail

SUPERSET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SUPERSET_ROOT"
# shellcheck disable=SC1091
source "$SUPERSET_ROOT/scripts/lib/deploy_common.sh"

USE_TAILSCALE=false
FORCE=false
EMBED=false
MINIO=false
IMPORT_MINIO=false
SKIP_VENV=false
REBUILD_BACKEND=false
DRY_RUN=false
SKIP_DEPLOY=false
NO_REBUILD=false
DASHBOARD=""
declare -a USER_DOMAINS=()

usage() {
  cat <<'EOF'
publish_superset.sh — deploy Superset dashboards + optional MinIO + BI embed

  ./publish_superset.sh [options]

Workflow A — dev (YAML → Superset API → embed UI):
  ./publish_superset.sh --tailscale --force --embed

Workflow B — dev + backup artifact to MinIO:
  ./publish_superset.sh --tailscale --force --embed --minio

Workflow C — target cluster imports from MinIO (UAT/PROD):
  ./publish_superset.sh --tailscale --import-minio --embed --domain https://reports.example.com

Options:
  --tailscale         Use config/.env.tailscale.example → config/.env
  --force             Update existing charts/dashboards (required after YAML edits)
  --embed             Grant guest + enable-embed + export-registry (+ optional rebuild)
  --minio             After deploy: export ZIP + upload to MinIO (backup/GitOps)
  --import-minio      Import dashboard ZIPs from MinIO (skip YAML deploy unless --force too)
  --domain URL        Extra allowed parent-frame origin (repeatable)
  --dashboard SLUG    Deploy/import single dashboard only
  --rebuild-backend   go build kaizen-report-api after enable-embed (default with --embed)
  --no-rebuild-backend  Skip go build even when --embed
  --skip-venv         Use system python3
  --dry-run           Validate/deploy preview only (no API writes for deploy)
  -h, --help          Show help

Notes:
  - deploy --force already runs save-query-context (embedded filters).
  - After --embed, restart ./run_local.sh or rebuild backend to load dashboards.embed.json.
  - MinIO does NOT auto-sync to Superset — use --import-minio on the target cluster.
EOF
}

while [[ $# -gt 0 ]]; do
  arg="$1"
  case "$arg" in
    --tailscale) USE_TAILSCALE=true ;;
    --force) FORCE=true ;;
    --embed) EMBED=true ;;
    --minio) MINIO=true ;;
    --import-minio) IMPORT_MINIO=true ;;
    --skip-venv) SKIP_VENV=true ;;
    --rebuild-backend) REBUILD_BACKEND=true ;;
    --no-rebuild-backend) NO_REBUILD=true ;;
    --dry-run) DRY_RUN=true ;;
    --skip-deploy) SKIP_DEPLOY=true ;;
    -h|--help) usage; exit 0 ;;
    --domain)
      shift
      [ -z "${1:-}" ] && { echo "❌ --domain requires a URL"; exit 1; }
      USER_DOMAINS+=("$1")
      ;;
    --dashboard)
      shift
      [ -z "${1:-}" ] && { echo "❌ --dashboard requires a slug"; exit 1; }
      DASHBOARD="$1"
      ;;
    *)
      echo "Unknown option: $arg"
      usage
      exit 1
      ;;
  esac
  shift
done

if [ "$EMBED" = true ] && [ "$REBUILD_BACKEND" = false ] && [ "$NO_REBUILD" = false ]; then
  # Default rebuild when embedding — backend reads dashboards.embed.json / Mặc định build BE
  REBUILD_BACKEND=true
fi

if [ "$IMPORT_MINIO" = true ] && [ "$FORCE" = false ] && [ "$SKIP_DEPLOY" = false ]; then
  SKIP_DEPLOY=true
fi

if [ "$FORCE" = false ] && [ "$SKIP_DEPLOY" = false ] && [ "$IMPORT_MINIO" = false ]; then
  echo "⚠️  No --force: deploy will skip updating existing dashboards."
  echo "   After editing YAML, use: ./publish_superset.sh --tailscale --force --embed"
fi

trap superset_teardown_port_forwards EXIT INT TERM

superset_log "Publish Superset"
superset_ensure_env "$USE_TAILSCALE"
superset_ensure_venv "$SKIP_VENV"
superset_setup_kubeconfig

# --- Phase 1: YAML deploy → Superset API ---
if [ "$SKIP_DEPLOY" = false ]; then
  superset_ensure_superset_port_forward || true
  superset_preflight "$DRY_RUN"

  DEPLOY_ARGS=()
  [ "$FORCE" = true ] && DEPLOY_ARGS+=(--force)
  [ "$DRY_RUN" = true ] && DEPLOY_ARGS+=(--dry-run)
  [ -n "$DASHBOARD" ] && DEPLOY_ARGS+=(--dashboard "$DASHBOARD")

  superset_run_module deploy ${DEPLOY_ARGS[@]+"${DEPLOY_ARGS[@]}"}
  superset_print_dashboard_links
fi

# --- Phase 2: MinIO backup (export from live Superset) ---
if [ "$MINIO" = true ] && [ "$DRY_RUN" = false ]; then
  superset_log "Backup dashboards → MinIO"
  superset_ensure_minio_port_forward || {
    echo "❌ MinIO not reachable for --minio"
    exit 1
  }
  if [ -n "$DASHBOARD" ]; then
    superset_run_module export --slug "$DASHBOARD"
  else
    superset_run_module export
  fi
  superset_run_module sync-minio --sync-all
  echo "✔ MinIO backup complete (s3://${MINIO_BUCKET:-superset}/${MINIO_ASSETS_PREFIX:-assets/})"
fi

# --- Phase 3: Import from MinIO (target cluster) ---
if [ "$IMPORT_MINIO" = true ] && [ "$DRY_RUN" = false ]; then
  superset_log "Import dashboards from MinIO"
  superset_ensure_superset_port_forward || true
  superset_preflight false
  superset_ensure_minio_port_forward || {
    echo "❌ MinIO not reachable for --import-minio"
    exit 1
  }
  IMPORT_ARGS=(--all)
  if [ -n "$DASHBOARD" ]; then
    # Single ZIP key — best-effort slug → dashboards/<slug>.zip
    IMPORT_ARGS=(--key "${MINIO_ASSETS_PREFIX:-assets/}dashboards/${DASHBOARD}.zip")
  fi
  superset_run_python "$SUPERSET_ROOT/scripts/import_from_minio.py" "${IMPORT_ARGS[@]}"
  superset_log "Save query_context after import"
  superset_run_module save-query-context
fi

# --- Phase 4: BI embed (guest token + UUID + frontend registry) ---
if [ "$EMBED" = true ] && [ "$DRY_RUN" = false ]; then
  superset_ensure_superset_port_forward || true

  superset_log "Grant guest embed permissions"
  if [ -x "$SUPERSET_ROOT/scripts/grant_guest_embed_permissions.sh" ] && command -v kubectl >/dev/null; then
    if bash "$SUPERSET_ROOT/scripts/grant_guest_embed_permissions.sh"; then
      echo "✔ Guest permissions granted"
    else
      echo "⚠️  grant_guest_embed_permissions failed — tab BI may return 403"
    fi
  else
    echo "⚠️  Skipping grant_guest_embed_permissions (no kubectl or script)"
  fi

  superset_log "Enable embedded dashboards"
  declare -a EMBED_ARGS=()
  local_domain=""
  while IFS= read -r local_domain; do
    EMBED_ARGS+=(--domain "$local_domain")
  done < <(superset_collect_embed_domain_args)
  for d in "${USER_DOMAINS[@]}"; do
    EMBED_ARGS+=(--domain "$d")
  done
  if ! superset_run_module enable-embed "${EMBED_ARGS[@]}"; then
    echo "❌ enable-embed failed"
    exit 1
  fi

  superset_log "Export frontend BI registry"
  superset_run_module export-registry || true

  if [ "$REBUILD_BACKEND" = true ]; then
    BACKEND_DIR="$SUPERSET_ROOT/../backend"
    if command -v go >/dev/null && [ -d "$BACKEND_DIR/cmd/api" ]; then
      superset_log "Rebuild Go backend (dashboards.embed.json)"
      (cd "$BACKEND_DIR" && go build -o build/kaizen-report-api ./cmd/api/)
      echo "✔ Backend rebuilt: $BACKEND_DIR/build/kaizen-report-api"
      echo "   Restart ./run_local.sh if backend is already running."
    else
      echo "⚠️  go not found — rebuild backend manually after enable-embed"
    fi
  fi
fi

echo ""
superset_log "Publish complete"
if [ "$EMBED" = true ] && [ "$DRY_RUN" = false ]; then
  echo "Next: cd ../.. && cd projects && ./run_local.sh"
  echo "Open http://localhost:8092 → tab Phân tích BI"
fi
