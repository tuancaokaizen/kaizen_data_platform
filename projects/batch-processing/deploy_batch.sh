#!/bin/bash
# deploy_batch.sh
# Script build & deploy Spark Batch jar lên MinIO
set -e

# Get directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Prioritize K3S_CONF or config.k3s-new over standard KUBECONFIG
if [ -n "${K3S_CONF:-}" ]; then
    export KUBECONFIG="$K3S_CONF"
elif [ -f "$DIR/../../dags/config.k3s-new" ]; then
    export KUBECONFIG="$DIR/../../dags/config.k3s-new"
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
    ' "$DIR/config.ini"
}

# Load MinIO config
MINIO_ENDPOINT=$(get_config_val "minio" "endpoint")
MINIO_ACCESS_KEY=$(get_config_val "minio" "access_key")
MINIO_SECRET_KEY=$(get_config_val "minio" "secret_key")

# Cấu hình bucket và đường dẫn đích theo đặc tả SparkSubmit
MINIO_BUCKET="spark"
REMOTE_JAR_PATH="jobs/kaizen-batch-processing.jar"
JAR_FILE="build/libs/kaizen-batch-processing-1.0.0-batch.jar"

# Parse port from MINIO_ENDPOINT
MINIO_PORT=$(echo "$MINIO_ENDPOINT" | grep -oE '[0-9]+$' || echo "9001")

setup_port_forward() {
    local port=$1
    local keyword=$2
    local pf_var_name=$3
    
    if ! nc -z localhost "$port" &>/dev/null; then
        echo "Port $port is closed. Attempting to auto port-forward service containing '$keyword'..."
        if command -v kubectl &> /dev/null; then
            local SVC_LINE=""
            SVC_LINE=$(kubectl get svc --all-namespaces 2>/dev/null | grep -iE "$keyword" | head -n 1) || true
            if [ -n "$SVC_LINE" ]; then
                local NS
                local SVC
                NS=$(echo "$SVC_LINE" | awk '{print $1}')
                SVC=$(echo "$SVC_LINE" | awk '{print $2}')
                echo "Detected service '$SVC' in namespace '$NS'"
                
                local target_port="$port"
                if [[ "$keyword" == "minio" ]]; then
                    target_port="9000"
                fi
                kubectl port-forward -n "$NS" svc/"$SVC" "$port":"$target_port" >/dev/null 2>&1 &
                local pid=$!
                eval "$pf_var_name=$pid"
                echo "Port-forwarding started for $SVC on port $port (PID: $pid). Waiting..."
                sleep 3
            else
                echo "⚠️ Warning: Service matching '$keyword' not found in Kubernetes."
            fi
        else
            echo "⚠️ Warning: kubectl not found. Cannot auto port-forward."
        fi
    else
        echo "Port $port is already open."
    fi
}

teardown_port_forward() {
    local pid=$1
    if [ -n "$pid" ]; then
        echo "Closing port-forward (PID: $pid)..."
        kill "$pid" 2>/dev/null || true
    fi
}

clean_and_build() {
    echo "=== 1. Cleaning and Building Spark Batch Jar ==="
    # Sử dụng no-daemon và gradle-user-home nội bộ để tránh xung đột quyền ghi file
    gradle --gradle-user-home "$DIR/.gradle-home" --no-daemon clean buildBatchJar
    echo "✔ Build successful: $JAR_FILE"
}

upload_jar() {
    echo "=== 2. Uploading Spark Batch Jar to MinIO (bucket: $MINIO_BUCKET, path: $REMOTE_JAR_PATH) ==="
    if [ ! -f "$JAR_FILE" ]; then
        echo "❌ Error: Jar file not found at $JAR_FILE. Please run './deploy_batch.sh build' first."
        exit 1
    fi

    # Cấu hình mc nội bộ tránh vi phạm ghi thư mục home
    mkdir -p "$DIR/.mc"
    export MC_CONFIG_DIR="$DIR/.mc"

    local PF_PID=""
    setup_port_forward "$MINIO_PORT" "minio" PF_PID

    local EXIT_CODE=0
    if command -v mc &> /dev/null; then
        echo "Using mc (MinIO Client) for upload..."
        mc alias set local-minio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api s3v4 --quiet
        mc mb local-minio/"$MINIO_BUCKET" 2>/dev/null || true
        mc anonymous set download local-minio/"$MINIO_BUCKET" || true
        if mc cp "$JAR_FILE" local-minio/"$MINIO_BUCKET"/"$REMOTE_JAR_PATH"; then
            echo "✔ Upload successful."
        else
            echo "❌ Error: MinIO upload failed!"
            EXIT_CODE=1
        fi
    elif command -v aws &> /dev/null; then
        echo "Using aws CLI for upload..."
        AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
        aws s3 cp "$JAR_FILE" "s3://$MINIO_BUCKET/$REMOTE_JAR_PATH" \
        --endpoint-url "$MINIO_ENDPOINT" --no-verify-ssl
        echo "✔ Uploaded to s3://$MINIO_BUCKET/$REMOTE_JAR_PATH via aws CLI"
    else
        echo "❌ Error: Neither 'mc' (MinIO Client) nor 'aws' CLI is installed."
        EXIT_CODE=1
    fi

    teardown_port_forward "$PF_PID"

    if [ "$EXIT_CODE" -ne 0 ]; then
        exit "$EXIT_CODE"
    fi
}

clean_minio() {
    echo "=== Cleaning up Spark JAR on MinIO ==="
    local PF_PID=""
    setup_port_forward "$MINIO_PORT" "minio" PF_PID

    mkdir -p "$DIR/.mc"
    export MC_CONFIG_DIR="$DIR/.mc"

    if command -v mc &> /dev/null; then
        mc alias set local-minio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api s3v4 --quiet
        mc rm --force local-minio/"$MINIO_BUCKET"/"$REMOTE_JAR_PATH" >/dev/null 2>&1 || true
        echo "✔ JAR cleared on MinIO."
    elif command -v aws &> /dev/null; then
        AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
        aws s3 rm "s3://$MINIO_BUCKET/$REMOTE_JAR_PATH" --endpoint-url "$MINIO_ENDPOINT" --no-verify-ssl >/dev/null 2>&1 || true
        echo "✔ JAR cleared on MinIO."
    fi

    teardown_port_forward "$PF_PID"
    echo "✔ Clean completed successfully!"
}

# Routing
ACTION=$1
case "$ACTION" in
    build)
        clean_and_build
        ;;
    upload)
        upload_jar
        ;;
    clean)
        clean_minio
        ;;
    deploy)
        clean_and_build
        upload_jar
        ;;
    *)
        echo "Usage: ./deploy_batch.sh {build|upload|clean|deploy}"
        echo "  build  : Build Spark Batch shadow JAR locally"
        echo "  upload : Upload built JAR to MinIO under spark bucket"
        echo "  clean  : Clear Spark JAR artifact on MinIO"
        echo "  deploy : Run build and upload sequentially"
        exit 1
        ;;
esac
