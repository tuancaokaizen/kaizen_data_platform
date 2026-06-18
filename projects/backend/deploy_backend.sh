#!/bin/bash
# Local backend helpers — use ../run_local.sh for full stack / Helper backend local
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$DIR/build"
BIN_FILE="$BIN_DIR/kaizen-report-api"
MAIN_FILE="$DIR/cmd/api/main.go"

build_api() {
    mkdir -p "$BIN_DIR"
    echo "=== Building backend API ==="
    (cd "$DIR" && go mod tidy && go build -o "$BIN_FILE" "$MAIN_FILE")
    echo "✔ $BIN_FILE"
}

run_api() {
    if [ ! -f "$BIN_FILE" ]; then
        build_api
    fi
    if [ -f "$DIR/.env" ]; then
        set -a
        # shellcheck disable=SC1091
        source "$DIR/.env"
        set +a
    fi
    export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-localhost}"
    export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
    echo "=== Running backend API on :${API_PORT:-8090} ==="
    exec "$BIN_FILE"
}

clean_api() {
    rm -rf "$BIN_DIR"
    echo "✔ Cleaned backend build/"
}

ACTION=${1:-}
case "$ACTION" in
    build) build_api ;;
    run) run_api ;;
    clean) clean_api ;;
    *)
        echo "Usage: ./deploy_backend.sh {build|run|clean}"
        echo "  Full stack (ClickHouse port-forward + UI): cd .. && ./run_local.sh"
        exit 1
        ;;
esac
