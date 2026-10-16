#!/bin/bash
# Build the Kaizen batch jar and publish it to MinIO, where Airflow tasks download it.
# Build jar batch Kaizen và đưa lên MinIO, nơi các task Airflow tải về.
#
# One object per job under com.kaizen.batch, run by dags/jobs/spark_task.py with args: env fromDate toDate [job args]
# Mỗi job là một object dưới com.kaizen.batch, chạy bởi dags/jobs/spark_task.py với args: env fromDate toDate [tham số job]
# No credentials are read from config.ini: MinIO keys come from Secret compute/kaizen-flink-s3
# and stay in env only (MC_HOST_*), never in argv or output.
# Không đọc credential từ config.ini: khoá MinIO lấy từ Secret compute/kaizen-flink-s3
# và chỉ nằm trong env (MC_HOST_*), không nằm trong argv hay output.
set -euo pipefail

# Prefer K3S_CONF, then the lab cluster config / Ưu tiên K3S_CONF, rồi config của cluster lab
if [ -n "${K3S_CONF:-}" ]; then
    export KUBECONFIG="$K3S_CONF"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
    export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

# Project root is the parent of deploy/ / Gốc project là thư mục cha của deploy/
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)"
JAR_FILE="$DIR/build/libs/kaizen-batch-processing-1.0.0-batch.jar"

MINIO_NAMESPACE="storage"
MINIO_SERVICE="minio"
MINIO_LOCAL_PORT=19000
SECRET_NAMESPACE="compute"
SECRET_NAME="kaizen-flink-s3"
BUCKET="spark"
# Must match JAR_URL in dags/jobs/spark_task.py / Phải khớp JAR_URL trong dags/jobs/spark_task.py
REMOTE_JAR="jobs/kaizen-batch.jar"

gradle_run() {
    (cd "$DIR" && gradle --gradle-user-home "$DIR/.gradle-home" --no-daemon "$@")
}

# Port-forward MinIO (if needed) and export MC_HOST_kzbatch; sets PORT_FORWARD_PID
# Port-forward MinIO (nếu cần) và export MC_HOST_kzbatch; gán PORT_FORWARD_PID
PORT_FORWARD_PID=""
connect_minio() {
    command -v mc >/dev/null || { echo "❌ 'mc' (MinIO client) is required"; exit 1; }
    if ! nc -z 127.0.0.1 "$MINIO_LOCAL_PORT" 2>/dev/null; then
        kubectl port-forward -n "$MINIO_NAMESPACE" "svc/$MINIO_SERVICE" "$MINIO_LOCAL_PORT:9000" >/dev/null 2>&1 &
        PORT_FORWARD_PID=$!
        trap disconnect_minio EXIT
        sleep 3
    fi
    local access_key secret_key
    access_key="$(kubectl get secret -n "$SECRET_NAMESPACE" "$SECRET_NAME" -o jsonpath='{.data.access-key}' | base64 -d)"
    secret_key="$(kubectl get secret -n "$SECRET_NAMESPACE" "$SECRET_NAME" -o jsonpath='{.data.secret-key}' | base64 -d)"
    # Credentials only in env, never in argv / Credential chỉ nằm trong env, không nằm trong argv
    export MC_CONFIG_DIR="$DIR/.mc"
    export MC_HOST_kzbatch="http://$access_key:$secret_key@127.0.0.1:$MINIO_LOCAL_PORT"
}

disconnect_minio() {
    unset MC_HOST_kzbatch
    if [ -n "$PORT_FORWARD_PID" ]; then
        kill "$PORT_FORWARD_PID" 2>/dev/null || true
        PORT_FORWARD_PID=""
    fi
}

# ---------------------------------------------------------------------------
# Actions / Thao tác
# ---------------------------------------------------------------------------

build() {
    echo "=== Build: tests + shadow jar ==="
    gradle_run clean test shadowJar
    echo "✔ $JAR_FILE"
}

# Bucket policy is left unchanged: no anonymous access is granted here
# Giữ nguyên policy của bucket: script này không mở quyền truy cập ẩn danh
publish() {
    [ -f "$JAR_FILE" ] || { echo "❌ $JAR_FILE missing, run '$0 build' first"; exit 1; }
    connect_minio
    echo "=== Publish to $BUCKET/$REMOTE_JAR ==="
    mc cp --quiet "$JAR_FILE" "kzbatch/$BUCKET/$REMOTE_JAR"
    mc stat "kzbatch/$BUCKET/$REMOTE_JAR" | grep -E '^(Name|Date|Size)'
    disconnect_minio
    echo "✔ Published"
}

status() {
    echo "=== Local jar ==="
    if [ -f "$JAR_FILE" ]; then ls -la "$JAR_FILE"; else echo "  (not built)"; fi
    connect_minio
    echo "=== MinIO $BUCKET/$REMOTE_JAR ==="
    mc stat "kzbatch/$BUCKET/$REMOTE_JAR" | grep -E '^(Name|Date|Size)' || echo "  (missing)"
    disconnect_minio
}

usage() {
    cat <<EOF
Usage: $0 <action>
  build                    gradle clean test shadowJar
  publish                  copy the built jar to MinIO $BUCKET/$REMOTE_JAR
  deploy                   build → publish
  status                   local jar and MinIO object (name, date, size)
EOF
}

case "${1:-}" in
    build) build ;;
    publish) publish ;;
    deploy) build; publish ;;
    status) status ;;
    *) usage; exit 1 ;;
esac
