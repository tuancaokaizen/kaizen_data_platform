#!/bin/bash
# Deploy report UI on k3s: upload source → Job build (in-cluster) → Deployment.
# Triển khai report UI trên k3s: đẩy source → Job build (trong cluster) → Deployment.
# No Docker Desktop required / Không cần Docker Desktop.
# Usage: dags/k8s/compute/deploy_report_ui.sh [apply|logs|delete|status]
set -euo pipefail

export PATH="/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin:$PATH"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config.k3s-new}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
PROJECTS="$ROOT/projects"
NS=compute
PVC=kaizen-report-ui-data

upload_context() {
  # Echoes only the 16-char sha on the last line of stdout (captured by caller).
  # Chỉ in sha 16 ký tự ở dòng cuối stdout (caller capture).
  local sha staging helper
  sha="$(
    find "$PROJECTS/backend" "$PROJECTS/frontend" \
      \( -name node_modules -o -name dist -o -name build -o -name .vite -o -name .git \) -prune -o \
      -type f -print 2>/dev/null | sort | xargs shasum -a 256 2>/dev/null | shasum -a 256 | cut -c1-16
  )"
  echo "code-sha=$sha" >&2

  staging="$(mktemp -d /tmp/kaizen-report-ctx-XXXXXX)"
  mkdir -p "$staging/backend" "$staging/frontend"
  # Copy source without heavy dirs / Copy source bỏ thư mục nặng
  rsync -a --delete \
    --exclude node_modules --exclude dist --exclude build --exclude .vite --exclude .git \
    "$PROJECTS/backend/" "$staging/backend/"
  rsync -a --delete \
    --exclude node_modules --exclude dist --exclude .vite --exclude .git \
    "$PROJECTS/frontend/" "$staging/frontend/"

  echo "=== Ensure PVC + helper pod for upload ===" >&2
  kubectl apply -f "$HERE/kaizen-report-ui-pvc.yaml" >/dev/null
  # Wait PVC bound / Chờ PVC bound
  kubectl wait --for=condition=Bound "pvc/$PVC" -n "$NS" --timeout=120s || true

  helper="kaizen-report-ui-upload"
  kubectl delete pod "$helper" -n "$NS" --ignore-not-found --wait=true 2>/dev/null || true
  kubectl run "$helper" -n "$NS" \
    --image=busybox:1.36 \
    --restart=Never \
    --overrides="{
      \"spec\": {
        \"containers\": [{
          \"name\": \"upload\",
          \"image\": \"busybox:1.36\",
          \"command\": [\"sleep\", \"3600\"],
          \"volumeMounts\": [{\"name\": \"data\", \"mountPath\": \"/data\"}]
        }],
        \"volumes\": [{
          \"name\": \"data\",
          \"persistentVolumeClaim\": {\"claimName\": \"$PVC\"}
        }]
      }
    }" >/dev/null
  kubectl wait --for=condition=Ready "pod/$helper" -n "$NS" --timeout=120s

  echo "=== Upload context to PVC ===" >&2
  kubectl exec -n "$NS" "$helper" -- sh -c 'rm -rf /data/context && mkdir -p /data/context /data/artifacts'
  tar -C "$staging" -cf - backend frontend | kubectl exec -i -n "$NS" "$helper" -- tar -C /data/context -xf -
  kubectl delete pod "$helper" -n "$NS" --ignore-not-found --wait=false >/dev/null
  rm -rf "$staging"
  printf '%s\n' "$sha"
}

run_build_job() {
  echo "=== In-cluster build Job ==="
  kubectl delete job kaizen-report-ui-build -n "$NS" --ignore-not-found --wait=true 2>/dev/null || true
  kubectl apply -f "$HERE/kaizen-report-ui-build-job.yaml"
  # Stream logs while waiting / Stream log trong lúc chờ
  (
    for _ in $(seq 1 60); do
      pod="$(kubectl get pods -n "$NS" -l job-name=kaizen-report-ui-build -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
      if [ -n "${pod:-}" ]; then
        kubectl logs -n "$NS" "$pod" -f 2>/dev/null && break
      fi
      sleep 2
    done
  ) &
  LOG_PID=$!
  if ! kubectl wait --for=condition=complete "job/kaizen-report-ui-build" -n "$NS" --timeout=900s; then
    kill "$LOG_PID" 2>/dev/null || true
    echo "❌ Build Job failed"
    kubectl describe job/kaizen-report-ui-build -n "$NS" | tail -40
    kubectl logs -n "$NS" -l job-name=kaizen-report-ui-build --tail=80 || true
    exit 1
  fi
  kill "$LOG_PID" 2>/dev/null || true
  wait "$LOG_PID" 2>/dev/null || true
  echo "✔ Build Job complete"
}

case "${1:-apply}" in
  apply)
    # Last line only — upload_context may print noise on stdout / Chỉ lấy dòng cuối
    SHA="$(upload_context | tail -n 1 | tr -d '\r\n')"
    case "$SHA" in
      ''|*[!0-9a-fA-F]*) SHA="$(date +%s | shasum -a 256 | cut -c1-16)" ;;
    esac
    run_build_job
    echo "=== Apply Deployment/Service ==="
    # Escape &/| for sed / Escape ký tự đặc biệt cho sed
    SAFE_SHA="$(printf '%s' "$SHA" | sed 's/[&|]/\\&/g')"
    sed -e "s/__CODE_SHA__/${SAFE_SHA}/g" "$HERE/kaizen-report-ui.yaml" | kubectl apply -f -
    kubectl rollout restart deploy/kaizen-report-ui -n "$NS" 2>/dev/null || true
    kubectl rollout status deploy/kaizen-report-ui -n "$NS" --timeout=180s || true
    kubectl get deploy,svc,pods,pvc -n "$NS" -l app=kaizen-report-ui
    echo ""
    echo "✔ Report UI: http://192.168.100.172:31090  (Lens → compute → kaizen-report-ui)"
    ;;
  logs)
    kubectl logs -n "$NS" deploy/kaizen-report-ui --tail=80 -f
    ;;
  status)
    kubectl get deploy,svc,pods,pvc,job -n "$NS" -l app=kaizen-report-ui -o wide
    ;;
  delete)
    kubectl delete job/kaizen-report-ui-build deploy/kaizen-report-ui svc/kaizen-report-ui \
      pvc/kaizen-report-ui-data -n "$NS" --ignore-not-found
    ;;
  *)
    echo "Usage: $0 [apply|logs|delete|status]" >&2
    exit 1
    ;;
esac
