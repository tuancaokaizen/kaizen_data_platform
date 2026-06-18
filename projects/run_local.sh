#!/bin/bash
# Local report dashboard — data from K3s ClickHouse via port-forward
# Dashboard local — data lấy từ ClickHouse trên K3s qua port-forward
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT/backend"
FRONTEND_DIR="$ROOT/frontend"
BIN_FILE="$BACKEND_DIR/build/kaizen-report-api"
PF_PIDS=()
BACKEND_PID=""
BACKEND_STARTED_BY_US=false

if [ -n "${K3S_CONF:-}" ]; then
    export KUBECONFIG="$K3S_CONF"
elif [ -f "$ROOT/../dags/config.k3s-new" ]; then
    export KUBECONFIG="$ROOT/../dags/config.k3s-new"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
    export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

cleanup() {
    echo ""
    echo "=== Stopping local stack ==="
    if [ "$BACKEND_STARTED_BY_US" = true ] && [ -n "$BACKEND_PID" ] && kill -0 "$BACKEND_PID" 2>/dev/null; then
        kill "$BACKEND_PID" 2>/dev/null || true
    fi
    for pid in ${PF_PIDS[@]+"${PF_PIDS[@]}"}; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

port_open() { nc -z localhost "$1" &>/dev/null; }

free_port() {
    local pids
    pids="$(lsof -ti tcp:"$1" -sTCP:LISTEN 2>/dev/null || true)"
    if [ -n "$pids" ]; then
        echo "→ Free port :$1 (PID $pids)"
        # shellcheck disable=SC2086
        kill $pids 2>/dev/null || true
        sleep 1
    fi
}

port_forward() {
    local label=$1 ns=$2 svc=$3 port=$4
    if port_open "$port"; then
        echo "✔ $label on :$port (reuse)"
        return
    fi
    echo "→ Forward $label :$port → $ns/$svc"
    kubectl port-forward -n "$ns" "svc/$svc" "$port:$port" >/dev/null 2>&1 &
    PF_PIDS+=("$!")
    sleep 1
    port_open "$port" || { echo "❌ Forward failed: $label"; exit 1; }
    echo "✔ $label ready"
}

ensure_env() {
    [ -f "$BACKEND_DIR/.env" ] || cp "$BACKEND_DIR/.env.example" "$BACKEND_DIR/.env"
    [ -f "$FRONTEND_DIR/.env" ] || cp "$FRONTEND_DIR/.env.example" "$FRONTEND_DIR/.env"
}

# Connect ClickHouse: auto port-forward OR direct host from .env / Kết nối CH: tự forward hoặc host trực tiếp
setup_clickhouse() {
    set -a
    # shellcheck disable=SC1091
    source "$BACKEND_DIR/.env"
    set +a

    local ch_host="${CLICKHOUSE_HOST:-localhost}"
    local ch_port="${CLICKHOUSE_PORT:-9000}"

    # Direct mode: colleague uses NodePort / VPN IP — set in backend/.env
    # Chế độ trực tiếp: đồng nghiệp dùng NodePort hoặc IP cluster, không cần kubectl forward
    if [ "${SKIP_K8S_FORWARD:-0}" = "1" ] || [ "$ch_host" != "localhost" ]; then
        echo "✔ ClickHouse direct: $ch_host:$ch_port (Tailscale / NodePort — no kubectl)"
        if ! nc -z -w 3 "$ch_host" "$ch_port" 2>/dev/null; then
            echo "❌ Cannot reach $ch_host:$ch_port"
            echo "   → Vào Tailscale cùng tailnet với máy K3s?"
            echo "   → Kiểm tra backend/.env (xem .env.tailscale.example)"
            exit 1
        fi
        echo "✔ ClickHouse reachable"
        export CLICKHOUSE_HOST="$ch_host"
        export CLICKHOUSE_PORT="$ch_port"
        return
    fi

    if ! command -v kubectl >/dev/null; then
        echo "❌ kubectl not found."
        echo "   Tailscale only: cp backend/.env.tailscale.example backend/.env"
        echo "   Then edit CLICKHOUSE_HOST and run ./run_local.sh again"
        exit 1
    fi

    port_forward "ClickHouse native" "databases" "clickhouse-service" 9000
    port_forward "ClickHouse HTTP"  "databases" "clickhouse-service" 8123
    export CLICKHOUSE_HOST=localhost
    export CLICKHOUSE_PORT=9000
}

start_backend() {
    echo "=== Backend :8090 ==="
    free_port 8090
    (cd "$BACKEND_DIR" && ./deploy_backend.sh build >/dev/null)
    (
        # Keep CH host/port from setup_clickhouse — .env must not override forward/Tailscale
        # Giữ host/port từ setup_clickhouse — không để .env ghi đè sau port-forward
        local saved_ch_host="${CLICKHOUSE_HOST:-}"
        local saved_ch_port="${CLICKHOUSE_PORT:-}"
        set -a
        # shellcheck disable=SC1091
        source "$BACKEND_DIR/.env"
        set +a
        if [ -n "$saved_ch_host" ]; then
            export CLICKHOUSE_HOST="$saved_ch_host"
            export CLICKHOUSE_PORT="$saved_ch_port"
        else
            export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
            export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
        fi
        exec "$BIN_FILE"
    ) &
    BACKEND_PID=$!
    BACKEND_STARTED_BY_US=true

    for _ in $(seq 1 20); do
        curl -sf "http://localhost:8090/healthz" >/dev/null && { echo "✔ http://localhost:8090"; return; }
        kill -0 "$BACKEND_PID" 2>/dev/null || { echo "❌ Backend crashed"; exit 1; }
        sleep 1
    done
    echo "❌ Backend timeout"; exit 1
}

start_frontend() {
    echo "=== Frontend :8092 ==="
    free_port 8092
    echo "   UI: http://localhost:8092  (Ctrl+C to stop all)"
    cd "$FRONTEND_DIR" && ./deploy_frontend.sh run
}

main() {
    command -v npm >/dev/null || { echo "❌ npm required (brew install node)"; exit 1; }

    echo "=== Kaizen Report (local UI, K3s data) ==="
    ensure_env
    setup_clickhouse
    start_backend
    start_frontend
}

main "$@"
