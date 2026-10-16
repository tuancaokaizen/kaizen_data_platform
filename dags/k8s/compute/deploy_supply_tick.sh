#!/bin/bash
# Deploy the supply tick worker: code goes in ConfigMap kaizen-sim-code, a code hash in the pod template restarts
# the worker when the code changes. The PG user is read from dags/config.ini; the password stays in the Secret.
# Deploy worker tick: code nằm trong ConfigMap kaizen-sim-code, hash code trong pod template khởi động lại worker
# khi code đổi. User PG đọc từ dags/config.ini; mật khẩu chỉ nằm trong Secret.
# Usage / Cách dùng: dags/k8s/compute/deploy_supply_tick.sh [apply|logs|delete]
set -euo pipefail
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config.k3s-new}"
HERE="$(cd "$(dirname "$0")" && pwd)"
DAGS="$(cd "$HERE/../.." && pwd)"
CODE="$DAGS/jobs/kaizen_sim"
NS=compute

# Read one key from an ini section / Đọc một key trong section ini
cfg() {
  awk -F' *= *' -v sec="[$1]" -v k="$2" '$0==sec{f=1;next} /^\[/{f=0} f&&$1==k{print $2;exit}' "$DAGS/config.ini"
}

case "${1:-apply}" in
  apply)
    # Only the module files, never __pycache__ / Chỉ file module, không lấy __pycache__
    files=("$CODE"/*.py)
    args=()
    for f in "${files[@]}"; do args+=("--from-file=$f"); done
    kubectl create configmap kaizen-sim-code -n "$NS" "${args[@]}" --dry-run=client -o yaml | kubectl apply -f -
    sha="$(cat "${files[@]}" | shasum -a 256 | cut -c1-16)"
    sed -e "s/__PG_USER__/$(cfg postgres username)/" -e "s/__CODE_SHA__/$sha/" "$HERE/kaizen-supply-tick.yaml" \
      | kubectl apply -f -
    kubectl rollout status deploy/kaizen-supply-tick -n "$NS" --timeout=180s
    ;;
  logs)
    kubectl logs -n "$NS" deploy/kaizen-supply-tick --tail=50 -f
    ;;
  delete)
    kubectl delete deploy/kaizen-supply-tick configmap/kaizen-sim-code -n "$NS" --ignore-not-found
    ;;
  *)
    echo "Usage: $0 [apply|logs|delete]" >&2
    exit 1
    ;;
esac
