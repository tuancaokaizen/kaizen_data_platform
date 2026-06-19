#!/bin/bash
# FIRST-TIME FULL SETUP: bootstrap.sh + deploy_superset.sh
# CÀI ĐẶT LẦN ĐẦU: gọi bootstrap rồi deploy dashboard
#
# Deploy thường (sau khi sửa spec):
#   ./deploy_superset.sh --tailscale --force
#
# Usage:
#   ./deploy.sh --tailscale
#   ./deploy.sh --tailscale --force   # bootstrap + force deploy dashboards
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

PASS_ARGS=()
for arg in "$@"; do
  PASS_ARGS+=("$arg")
done

echo "=== deploy.sh: full first-time setup ==="
echo "Deploy thường sau này: ./deploy_superset.sh --tailscale [--force]"
echo ""

BOOTSTRAP_ARGS=()
DEPLOY_ARGS=()
for arg in "${PASS_ARGS[@]}"; do
  case "$arg" in
    --force)
      DEPLOY_ARGS+=("$arg")
      ;;
    --dry-run)
      DEPLOY_ARGS+=("$arg")
      ;;
    *)
      BOOTSTRAP_ARGS+=("$arg")
      DEPLOY_ARGS+=("$arg")
      ;;
  esac
done

# Skip bootstrap on dry-run (no API) / Bỏ bootstrap khi dry-run
if [[ " ${DEPLOY_ARGS[*]} " != *" --dry-run "* ]]; then
  "$ROOT/bootstrap.sh" "${BOOTSTRAP_ARGS[@]}"
else
  echo "⏭ Skipping bootstrap.sh (dry-run)"
fi

"$ROOT/deploy_superset.sh" "${DEPLOY_ARGS[@]}"

echo ""
echo "=== deploy.sh complete ==="
echo "Next time (regular deploy):"
echo "  ./deploy_superset.sh --tailscale --force"
