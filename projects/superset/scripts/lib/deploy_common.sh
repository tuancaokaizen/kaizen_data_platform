# Shared helpers for Superset shell scripts
# Hàm dùng chung cho các script deploy Superset
#
# Caller must set SUPERSET_ROOT before sourcing:
#   SUPERSET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
#   source "$SUPERSET_ROOT/scripts/lib/deploy_common.sh"

: "${SUPERSET_ROOT:?SUPERSET_ROOT must be set before sourcing deploy_common.sh}"

VENV="$SUPERSET_ROOT/.venv"
PYTHON="${VENV}/bin/python"
PIP="${VENV}/bin/pip"
SUPERSET_PF_PIDS=()

superset_log() { echo "=== $* ==="; }

# Kubeconfig — same pattern as deploy_batch.sh / deploy_airflow.sh
# Cấu hình kubeconfig giống batch-processing và airflow
superset_setup_kubeconfig() {
  if [ -n "${K3S_CONF:-}" ]; then
    export KUBECONFIG="$K3S_CONF"
  elif [ -f "$SUPERSET_ROOT/../../dags/config.k3s-new" ]; then
    export KUBECONFIG="$SUPERSET_ROOT/../../dags/config.k3s-new"
  elif [ -f "$HOME/.kube/config.k3s-new" ]; then
    export KUBECONFIG="$HOME/.kube/config.k3s-new"
  fi
}

superset_port_open() {
  nc -z localhost "$1" &>/dev/null
}

# Auto port-forward K8s service → localhost (batch/stream pattern)
# Tự forward service K8s về localhost nếu port chưa mở
superset_setup_port_forward() {
  local port=$1
  local keyword=$2
  local target_port=${3:-9000}

  if superset_port_open "$port"; then
    echo "✔ Port $port already open ($keyword)"
    return 0
  fi

  echo "Port $port is closed. Auto port-forward service matching '$keyword'..."
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "⚠️ kubectl not found — cannot auto port-forward"
    return 1
  fi

  superset_setup_kubeconfig
  local svc_line=""
  svc_line=$(kubectl get svc --all-namespaces 2>/dev/null | grep -iE "$keyword" | head -n 1) || true
  if [ -z "$svc_line" ]; then
    echo "⚠️ Service matching '$keyword' not found in Kubernetes"
    return 1
  fi

  local ns svc
  ns=$(echo "$svc_line" | awk '{print $1}')
  svc=$(echo "$svc_line" | awk '{print $2}')
  echo "→ kubectl port-forward -n $ns svc/$svc $port:$target_port"
  kubectl port-forward -n "$ns" "svc/$svc" "$port:$target_port" >/dev/null 2>&1 &
  local pid=$!
  SUPERSET_PF_PIDS+=("$pid")
  sleep 3
  if superset_port_open "$port"; then
    echo "✔ Port-forward $svc on localhost:$port (PID: $pid)"
    return 0
  fi
  echo "❌ Port-forward failed for $svc on port $port"
  return 1
}

superset_teardown_port_forwards() {
  for pid in ${SUPERSET_PF_PIDS[@]+"${SUPERSET_PF_PIDS[@]}"}; do
    if [ -n "$pid" ]; then
      echo "Closing port-forward (PID: $pid)..."
      kill "$pid" 2>/dev/null || true
    fi
  done
  SUPERSET_PF_PIDS=()
}

# MinIO: localhost endpoint → auto forward (port 31900 like batch/stream)
# MinIO: endpoint localhost → tự forward (cổng 31900 giống batch/stream)
superset_ensure_minio_port_forward() {
  local endpoint="${MINIO_ENDPOINT:-http://localhost:31900}"
  local port
  port=$(echo "$endpoint" | grep -oE '[0-9]+$' || echo "31900")

  if [[ "$endpoint" != *"localhost"* ]] && [[ "$endpoint" != *"127.0.0.1"* ]]; then
    # Remote MinIO (e.g. Tailscale :9000) is usually not exposed — use port-forward / Dùng forward thay vì remote
    echo "⚠️ MinIO remote endpoint ($endpoint) — switching to localhost:${port} + port-forward"
    export MINIO_ENDPOINT="http://localhost:${port}"
  else
    export MINIO_ENDPOINT="http://localhost:${port}"
  fi
  superset_setup_port_forward "$port" "minio" 9000
}

superset_ensure_env() {
  local use_tailscale=${1:-false}
  if [ "$use_tailscale" = true ]; then
    cp -f "$SUPERSET_ROOT/config/.env.tailscale.example" "$SUPERSET_ROOT/config/.env"
    echo "✔ Using Tailscale env: config/.env"
  elif [ ! -f "$SUPERSET_ROOT/config/.env" ]; then
    if [ -f "$SUPERSET_ROOT/config/.env.tailscale.example" ]; then
      cp "$SUPERSET_ROOT/config/.env.tailscale.example" "$SUPERSET_ROOT/config/.env"
      echo "✔ Created config/.env from .env.tailscale.example (edit if needed)"
    else
      cp "$SUPERSET_ROOT/config/.env.example" "$SUPERSET_ROOT/config/.env"
      echo "✔ Created config/.env from .env.example"
    fi
  else
    echo "✔ Using existing config/.env"
  fi

  set -a
  # shellcheck disable=SC1091
  source "$SUPERSET_ROOT/config/.env"
  set +a
}

superset_ensure_venv() {
  local skip_venv=${1:-false}
  if [ "$skip_venv" = true ]; then
    PYTHON="$(command -v python3)"
    echo "✔ Using system python: $PYTHON"
    return
  fi

  if ! command -v python3 >/dev/null 2>&1; then
    echo "❌ python3 not found (brew install python)"
    exit 1
  fi

  if [ ! -d "$VENV" ]; then
    superset_log "Creating Python venv"
    python3 -m venv "$VENV"
  fi

  superset_log "Installing dependencies"
  "$PIP" install -q -r "$SUPERSET_ROOT/requirements.txt"
  echo "✔ venv ready: $VENV"
}

superset_preflight() {
  local skip=${1:-false}
  if [ "$skip" = true ]; then
    echo "⏭ Preflight skipped (dry-run)"
    return
  fi

  local host="${SUPERSET_HOST:-http://localhost:32288}"
  host="${host%/}"
  superset_log "Preflight Superset: $host"

  if ! curl -sf --connect-timeout 5 "${host}/health" >/dev/null 2>&1 \
     && ! curl -sf --connect-timeout 5 -o /dev/null -w "%{http_code}" "${host}/login/" | grep -qE '200|302'; then
    echo "❌ Cannot reach Superset at $host"
    echo "   Tailscale: SUPERSET_HOST=http://100.104.250.97:32288"
    echo "   Or run: ./scripts/port_forward.sh (other terminal)"
    exit 1
  fi
  echo "✔ Superset reachable"
}

superset_run_python() {
  local script=$1
  shift
  superset_log "Running $(basename "$script")"
  PYTHONPATH="$SUPERSET_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
    "$PYTHON" "$script" "$@"
}

superset_run_module() {
  local subcmd=$1
  shift
  superset_log "Running superset_deploy $subcmd"
  PYTHONPATH="$SUPERSET_ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
    "$PYTHON" -m superset_deploy "$subcmd" "$@"
}

superset_print_dashboard_links() {
  local host="${SUPERSET_HOST:-http://localhost:32288}"
  host="${host%/}"
  echo "Superset : $host"
  echo "Dashboards:"
  echo "  $host/superset/dashboard/tc-tong-hop-cua-hang/"
  echo "  $host/superset/dashboard/quy-pttt-cua-hang/"
}

# Port-forward apache-superset when SUPERSET_HOST points at localhost
# Forward Superset K3s khi dùng SUPERSET_HOST localhost
superset_ensure_superset_port_forward() {
  local host="${SUPERSET_HOST:-http://localhost:32288}"
  if [[ "$host" != *"localhost"* ]] && [[ "$host" != *"127.0.0.1"* ]]; then
    echo "✔ Superset direct: $host (no port-forward)"
    return 0
  fi
  export SUPERSET_HOST="http://localhost:32288"
  if ! command -v kubectl >/dev/null 2>&1; then
    echo "⚠️ kubectl not found — cannot port-forward Superset"
    return 1
  fi
  superset_setup_kubeconfig
  if superset_port_open 32288; then
    echo "✔ Superset on :32288 (reuse)"
    return 0
  fi
  echo "→ Port-forward Superset apps/apache-superset :32288"
  kubectl port-forward -n apps svc/apache-superset 32288:8888 >/dev/null 2>&1 &
  local pid=$!
  SUPERSET_PF_PIDS+=("$pid")
  sleep 3
  if superset_port_open 32288; then
    echo "✔ Superset ready on localhost:32288 (PID: $pid)"
    return 0
  fi
  echo "❌ Superset port-forward failed"
  return 1
}

# Print --domain args for enable-embed (one per line) / In domain cho enable-embed
superset_collect_embed_domain_args() {
  local seen=""
  _add_domain() {
    local d="${1%/}"
    [ -z "$d" ] && return
    case " $seen " in
      *" $d "*) return ;;
    esac
    seen="$seen $d"
    echo "$d"
  }

  _add_domain "${SUPERSET_EMBED_DOMAIN:-http://localhost:8092}"
  _add_domain "http://localhost:8092"
  _add_domain "http://127.0.0.1:8092"

  local ip iface
  if command -v ipconfig >/dev/null; then
    for iface in en0 en1 bridge0; do
      ip="$(ipconfig getifaddr "$iface" 2>/dev/null || true)"
      [ -n "$ip" ] && _add_domain "http://${ip}:8092"
    done
  fi
  if command -v tailscale >/dev/null 2>&1; then
    while read -r ip; do
      [ -n "$ip" ] && _add_domain "http://${ip}:8092"
    done < <(tailscale ip -4 2>/dev/null | tr ' ' '\n' | grep -E '^[0-9]+\.' || true)
  fi
  if command -v ifconfig >/dev/null 2>&1; then
    while read -r ip; do
      case "$ip" in
        127.*|169.254.*) continue ;;
      esac
      _add_domain "http://${ip}:8092"
    done < <(ifconfig 2>/dev/null | awk '/inet / {print $2}' | sed 's/addr://')
  fi
}
