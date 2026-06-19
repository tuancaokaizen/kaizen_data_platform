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

# Kubeconfig — same path as superset/batch (dags/config.k3s-new)
# Kubeconfig — cùng đường dẫn với superset/batch
setup_kubeconfig() {
    if [ -n "${K3S_CONF:-}" ]; then
        export KUBECONFIG="$K3S_CONF"
    elif [ -f "$ROOT/../dags/config.k3s-new" ]; then
        export KUBECONFIG="$ROOT/../dags/config.k3s-new"
    elif [ -f "$HOME/.kube/config.k3s-new" ]; then
        export KUBECONFIG="$HOME/.kube/config.k3s-new"
    elif [ -f "$HOME/.kube/config" ]; then
        export KUBECONFIG="$HOME/.kube/config"
    fi
}

setup_kubeconfig

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

# Python for superset_deploy CLI — prefer project venv / Python cho CLI superset_deploy
superset_python() {
    local venv_py="$ROOT/superset/.venv/bin/python3"
    if [ -x "$venv_py" ]; then
        echo "$venv_py"
    else
        echo "python3"
    fi
}

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

port_open() {
    nc -z 127.0.0.1 "$1" &>/dev/null || nc -z localhost "$1" &>/dev/null
}

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
    local label=$1 ns=$2 svc=$3 local_port=$4 remote_port=${5:-$4}
    if port_open "$local_port"; then
        echo "✔ $label on :$local_port (reuse)"
        return
    fi

    free_port "$local_port"

    local log_file attempt pid
    log_file="$(mktemp /tmp/kaizen-pf-XXXXXX.log)"
    for attempt in 1 2 3; do
        echo "→ Forward $label localhost:$local_port → $ns/$svc:$remote_port (try $attempt/3)"
        kubectl port-forward --address 127.0.0.1 -n "$ns" "svc/$svc" "$local_port:$remote_port" >"$log_file" 2>&1 &
        pid=$!
        sleep 2
        if port_open "$local_port" && kill -0 "$pid" 2>/dev/null; then
            PF_PIDS+=("$pid")
            echo "✔ $label ready"
            rm -f "$log_file"
            return
        fi
        kill "$pid" 2>/dev/null || true
        sleep 1
    done

    echo "❌ Forward failed: $label"
    if [ -s "$log_file" ]; then
        sed -n '1,5p' "$log_file" | sed 's/^/   /'
    fi
    rm -f "$log_file"
    exit 1
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

# Superset: forward apache-superset:8888 → localhost:32288 when using local URL
# Superset: forward svc apache-superset về localhost khi dùng URL local
setup_superset() {
    set -a
    # shellcheck disable=SC1091
    source "$BACKEND_DIR/.env"
    set +a

    local ss_host="${SUPERSET_HOST:-http://localhost:32288}"

    if [[ "$ss_host" == *"localhost"* ]] || [[ "$ss_host" == *"127.0.0.1"* ]]; then
        if ! command -v kubectl >/dev/null; then
            echo "❌ kubectl not found (SUPERSET_HOST=$ss_host needs port-forward)."
            exit 1
        fi
        port_forward "Superset" "apps" "apache-superset" 32288 8888
        export SUPERSET_HOST="http://localhost:32288"
        echo "✔ Superset embed API: $SUPERSET_HOST"
        return
    fi

    echo "✔ Superset direct: $ss_host (no kubectl forward)"
    export SUPERSET_HOST="$ss_host"
}

# Grant Gamma role for embedded guest tokens / Cấp quyền Gamma cho guest token nhúng
setup_superset_guest_permissions() {
    local script="$ROOT/superset/scripts/grant_guest_embed_permissions.sh"
    if [ ! -x "$script" ] || ! command -v kubectl >/dev/null; then
        return
    fi
    echo "=== Superset guest permissions ==="
    if bash "$script"; then
        echo "✔ Superset guest permissions ready"
    else
        echo "⚠️  Guest permissions failed — tab BI có thể báo 403"
    fi
}

# Register parent-frame origins for embedded SDK / Đăng ký origin trang cha cho embed
setup_superset_pvc() {
    local script="$ROOT/superset/scripts/apply_superset_pvc_k8s.sh"
    if [ ! -x "$script" ] || ! command -v kubectl >/dev/null; then
        return
    fi
    echo "=== Superset persistent volume ==="
    if bash "$script"; then
        echo "✔ Superset PVC ready"
    else
        echo "⚠️  PVC setup failed — restart pod có thể reset dashboard IDs"
    fi
}

# Register parent-frame origins for embedded SDK / Đăng ký origin trang cha cho embed
setup_superset_embed_domains() {
    local script_dir="$ROOT/superset"
    if ! command -v python3 >/dev/null || ! port_open 32288; then
        return
    fi
    echo "=== Superset embed domains ==="
    local -a domain_args=()
    local ip
    if command -v ipconfig >/dev/null; then
        for iface in en0 en1 bridge0; do
            ip="$(ipconfig getifaddr "$iface" 2>/dev/null || true)"
            [ -n "$ip" ] && domain_args+=(--domain "http://${ip}:8092")
        done
    fi
    if command -v hostname >/dev/null; then
        while read -r ip; do
            [ -n "$ip" ] && domain_args+=(--domain "http://${ip}:8092")
        done < <(hostname -I 2>/dev/null | tr ' ' '\n' | grep -E '^[0-9]+\.' || true)
    fi
    # Tailscale IPs (utun) — Vite often exposes 100.x.x.x / IP Tailscale cho embed
    if command -v tailscale >/dev/null 2>&1; then
        while read -r ip; do
            [ -n "$ip" ] && domain_args+=(--domain "http://${ip}:8092")
        done < <(tailscale ip -4 2>/dev/null | tr ' ' '\n' | grep -E '^[0-9]+\.' || true)
    fi
    if command -v ifconfig >/dev/null 2>&1; then
        while read -r ip; do
            case "$ip" in
                127.*|169.254.*) continue ;;
            esac
            domain_args+=(--domain "http://${ip}:8092")
        done < <(ifconfig 2>/dev/null | awk '/inet / {print $2}' | sed 's/addr://')
    fi
    if (
        cd "$script_dir"
        SUPERSET_EMBED_DOMAIN="${SUPERSET_EMBED_DOMAIN:-http://localhost:8092}" \
            "$(superset_python)" -m superset_deploy enable-embed "${domain_args[@]}"
    ); then
        echo "✔ Superset embed domains ready"
        # Sync frontend registry + backend embed JSON paths / Đồng bộ registry frontend
        "$(superset_python)" -m superset_deploy export-registry 2>/dev/null || true
    else
        echo "⚠️  enable-embed failed — mở UI qua IP có thể báo 403"
    fi
}

# Populate chart query_context for guest filter apply / Lưu query_context để guest apply filter
setup_superset_query_context() {
    local script_dir="$ROOT/superset"
    if ! command -v python3 >/dev/null || ! port_open 32288; then
        return
    fi
    echo "=== Superset chart query_context ==="
    if (cd "$script_dir" && "$(superset_python)" -m superset_deploy save-query-context); then
        echo "✔ Chart query_context ready"
    else
        echo "⚠️  save-query-context failed — apply filter có thể báo lỗi guest payload"
    fi
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
        local saved_ss_host="${SUPERSET_HOST:-}"
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
        if [ -n "$saved_ss_host" ]; then
            export SUPERSET_HOST="$saved_ss_host"
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
    if [ -n "${KUBECONFIG:-}" ]; then
        echo "   KUBECONFIG: $KUBECONFIG"
    fi
    ensure_env
    setup_clickhouse
    setup_superset
    setup_superset_pvc
    setup_superset_guest_permissions
    setup_superset_embed_domains
    setup_superset_query_context
    start_backend
    start_frontend
}

main "$@"
