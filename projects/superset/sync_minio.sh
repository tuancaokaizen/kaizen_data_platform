#!/bin/bash
# Export dashboards from Superset + upload to MinIO bucket superset/
# Export dashboard từ Superset + upload lên MinIO bucket superset/
#
# Auto port-forward MinIO (same pattern as deploy_batch.sh / deploy_airflow.sh):
#   - MINIO_ENDPOINT=http://localhost:31900 in config/.env
#   - Script tự kubectl port-forward svc/minio nếu port chưa mở
#
# Usage:
#   ./sync_minio.sh
#   ./sync_minio.sh --skip-venv
set -euo pipefail

SUPERSET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SUPERSET_ROOT"
# shellcheck disable=SC1091
source "$SUPERSET_ROOT/scripts/lib/deploy_common.sh"

SKIP_VENV=false
for arg in "$@"; do
  case "$arg" in
    --skip-venv) SKIP_VENV=true ;;
    -h|--help)
      echo "Usage: ./sync_minio.sh [--skip-venv]"
      echo "  Auto port-forward MinIO :31900 → upload (batch/airflow pattern)"
      exit 0
      ;;
  esac
done

trap superset_teardown_port_forwards EXIT INT TERM

superset_ensure_env false
superset_ensure_venv "$SKIP_VENV"

superset_log "Sync Superset assets → MinIO"
superset_ensure_minio_port_forward || {
  echo "❌ MinIO not reachable. Check kubectl + KUBECONFIG (config.k3s-new)."
  exit 1
}

superset_run_module export
superset_run_module sync-minio --sync-all

superset_log "MinIO sync complete"
echo "Bucket: s3://${MINIO_BUCKET:-superset}/${MINIO_ASSETS_PREFIX:-assets/}"
