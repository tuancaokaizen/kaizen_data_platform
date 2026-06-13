#!/bin/bash
set -e

# Get directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Prioritize K3S_CONF or config.k3s-new over standard KUBECONFIG
if [ -n "$K3S_CONF" ]; then
    export KUBECONFIG="$K3S_CONF"
elif [ -f "$DIR/config.k3s-new" ]; then
    export KUBECONFIG="$DIR/config.k3s-new"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
    export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

# Helper function to read config.ini
get_config_val() {
    local section=$1
    local key=$2
    awk -F " *= *" -v sec="[$section]" -v k="$key" '
        $0 == sec { in_sec = 1; next }
        /^\[/ { in_sec = 0 }
        in_sec && $1 == k { print $2; exit }
    ' "$DIR/../projects/stream-processing/config.ini"
}

# Load MinIO config
MINIO_ENDPOINT=$(get_config_val "minio" "endpoint")
MINIO_ACCESS_KEY=$(get_config_val "minio" "access_key")
MINIO_SECRET_KEY=$(get_config_val "minio" "secret_key")
MINIO_BUCKET="airflow"

# Parse port from MINIO_ENDPOINT (default to 9001 if parsing fails)
MINIO_PORT=$(echo "$MINIO_ENDPOINT" | grep -oE '[0-9]+$' || echo "9001")

upload_dags() {
    echo "=== 1. Uploading Airflow DAGs and Jobs to MinIO (bucket: $MINIO_BUCKET) ==="
    
    if [ ! -d "$DIR" ]; then
        echo "❌ Error: DAG directory not found at $DIR."
        exit 1
    fi

    # Set local MC config directory to bypass home directory permission limits
    mkdir -p "$DIR/../.mc"
    export MC_CONFIG_DIR="$DIR/../.mc"

    local PF_PID=""

    # Tự động Port Forward nếu cổng MinIO chưa mở trên localhost
    if ! nc -z localhost "$MINIO_PORT" &>/dev/null; then
        echo "Port $MINIO_PORT is closed. Setting up port-forward to MinIO service..."
        
        # Tự tìm namespace và service của MinIO trong K3s/Kubernetes
        if command -v kubectl &> /dev/null; then
            local SVC_LINE=""
            SVC_LINE=$(kubectl get svc --all-namespaces 2>/dev/null | grep -i "minio" | head -n 1) || true
            if [ -n "$SVC_LINE" ]; then
                local MINIO_NAMESPACE
                local MINIO_SVC
                MINIO_NAMESPACE=$(echo "$SVC_LINE" | awk '{print $1}')
                MINIO_SVC=$(echo "$SVC_LINE" | awk '{print $2}')
                echo "Detected MinIO service: '$MINIO_SVC' in namespace '$MINIO_NAMESPACE'"
                
                kubectl port-forward -n "$MINIO_NAMESPACE" svc/"$MINIO_SVC" "$MINIO_PORT":9000 >/dev/null 2>&1 &
                PF_PID=$!
                echo "Port-forwarding started (PID: $PF_PID). Waiting for connection..."
                sleep 3
            else
                echo "⚠️ Warning: MinIO service not found in Kubernetes. Upload might fail if port is closed."
            fi
        else
            echo "⚠️ Warning: kubectl not found. Cannot set up port-forward automatically."
        fi
    else
        echo "Port $MINIO_PORT is already open."
    fi

    # Tiến hành tải dữ liệu lên MinIO
    local EXIT_CODE=0
    if command -v mc &> /dev/null; then
        echo "Using mc (MinIO Client) for upload..."
        mc alias set local-minio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api s3v4
        mc mb local-minio/"$MINIO_BUCKET" 2>/dev/null || true
        
        # Sync thư mục dags (loại trừ các file script shell và kubernetes config) lên MinIO local-minio/airflow/dags/
        mc mirror --overwrite --exclude "*.sh" --exclude ".mc*" --exclude "config.k3s-new" "$DIR" local-minio/"$MINIO_BUCKET"/dags/
        echo "✔ DAGs successfully synchronized to local-minio/$MINIO_BUCKET/dags/ via mc"
    elif command -v aws &> /dev/null; then
        echo "Using aws CLI for upload..."
        AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
        aws s3 sync "$DIR" "s3://$MINIO_BUCKET/dags" \
        --exclude "*.sh" --exclude ".mc*" --exclude "config.k3s-new" \
        --endpoint-url "$MINIO_ENDPOINT" --no-verify-ssl
        echo "✔ DAGs successfully synchronized to s3://$MINIO_BUCKET/dags via aws CLI"
    else
        echo "❌ Error: Neither 'mc' (MinIO Client) nor 'aws' CLI is installed."
        EXIT_CODE=1
    fi

    # Dọn dẹp tiến trình Port Forward sau khi hoàn thành
    if [ -n "$PF_PID" ]; then
        echo "Closing port-forward (PID: $PF_PID)..."
        kill "$PF_PID" 2>/dev/null || true
    fi

    if [ "$EXIT_CODE" -ne 0 ]; then
        exit "$EXIT_CODE"
    fi
}

ACTION=$1
case "$ACTION" in
    upload)
        upload_dags
        ;;
    *)
        echo "Usage: ./deploy_airflow.sh upload"
        exit 1
        ;;
esac
