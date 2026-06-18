#!/bin/bash
# Local frontend helpers — use ../run_local.sh for full stack / Helper frontend local
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

run_frontend() {
    echo "=== Starting frontend dev server (:8092) ==="
    cd "$DIR"
    npm install
    npm run dev -- --host 0.0.0.0 --port 8092
}

build_frontend() {
    cd "$DIR"
    npm install
    npm run build
}

clean_frontend() {
    rm -rf "$DIR/dist" "$DIR/node_modules" "$DIR/.vite"
    echo "✔ Cleaned dist/, node_modules/, .vite/"
}

ACTION=${1:-}
case "$ACTION" in
    run) run_frontend ;;
    build) build_frontend ;;
    clean) clean_frontend ;;
    *)
        echo "Usage: ./deploy_frontend.sh {run|build|clean}"
        echo "  Full stack: cd .. && ./run_local.sh"
        exit 1
        ;;
esac
