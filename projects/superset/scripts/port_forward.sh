#!/bin/bash
# Port-forward Superset + ClickHouse for local bootstrap
# Forward Superset + ClickHouse để chạy bootstrap local
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -n "${K3S_CONF:-}" ]; then
  export KUBECONFIG="$K3S_CONF"
elif [ -f "$ROOT/../../dags/config.k3s-new" ]; then
  export KUBECONFIG="$ROOT/../../dags/config.k3s-new"
fi

port_open() { nc -z localhost "$1" &>/dev/null; }

forward() {
  local label=$1 ns=$2 svc=$3 local=$4 remote=$5
  if port_open "$local"; then
    echo "✔ $label already on :$local"
    return
  fi
  echo "→ Forward $label :$local → $ns/$svc:$remote"
  kubectl port-forward -n "$ns" "svc/$svc" "$local:$remote" >/dev/null 2>&1 &
  sleep 1
  port_open "$local" || { echo "❌ Failed: $label"; exit 1; }
  echo "✔ $label ready on :$local"
}

echo "=== Port-forward for Superset bootstrap ==="
forward "Superset" "apps" "apache-superset" 32288 8888
forward "ClickHouse HTTP" "databases" "clickhouse-service" 8123 8123
forward "MinIO" "storage" "minio" 31900 9000

echo ""
echo "Superset : http://localhost:32288  (svc apache-superset:8888 → NodePort 32288)"
echo "Tailscale: http://100.104.250.97:32288"
echo "ClickHouse HTTP : http://localhost:8123"
echo "MinIO : http://localhost:31900"
echo "Press Ctrl+C to stop forwards."

wait
