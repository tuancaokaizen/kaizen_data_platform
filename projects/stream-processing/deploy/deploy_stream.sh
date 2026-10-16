#!/bin/bash
set -e

# Prioritize K3S_CONF or config.k3s-new over standard KUBECONFIG
if [ -n "$K3S_CONF" ]; then
    export KUBECONFIG="$K3S_CONF"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
    export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

# Get directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

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

MINIO_BUCKET="flink"
REMOTE_JAR_PATH="artifact/kaizen-stream-processing-1.0-SNAPSHOT.jar"

JAR_FILE="build/libs/kaizen-stream-processing-1.0-SNAPSHOT.jar"

# Tự động quét các file *Job.java trong package com.kaizen.flink.entrypoint
JOBS=()
ENTRYPOINT_DIR="$DIR/src/main/java/com/kaizen/flink/entrypoint"
if [ -d "$ENTRYPOINT_DIR" ]; then
    for f in "$ENTRYPOINT_DIR"/*Job.java; do
        if [ -f "$f" ]; then
            class_name=$(basename "$f" .java)
            JOBS+=("com.kaizen.flink.entrypoint.$class_name")
        fi
    done
fi

if [ ${#JOBS[@]} -eq 0 ]; then
    echo "❌ Error: No Flink *Job.java entrypoints found at $ENTRYPOINT_DIR"
    exit 1
fi

# Parse port from MINIO_ENDPOINT
MINIO_PORT=$(echo "$MINIO_ENDPOINT" | grep -oE '[0-9]+$' || echo "9001")

get_flink_jobmanager_pod() {
    if ! command -v kubectl &> /dev/null; then
        echo "❌ Error: 'kubectl' command not found." >&2
        return 1
    fi
    local JM_LINE
    JM_LINE=$(kubectl get pods --all-namespaces 2>/dev/null | grep -iE "flink-jobmanager|jobmanager" | head -n 1) || true
    if [ -z "$JM_LINE" ]; then
        echo "❌ Error: Flink JobManager pod not found in Kubernetes." >&2
        return 1
    fi
    local JM_NS
    local JM_POD
    JM_NS=$(echo "$JM_LINE" | awk '{print $1}')
    JM_POD=$(echo "$JM_LINE" | awk '{print $2}')
    echo "$JM_NS $JM_POD"
}

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
    echo "=== 1. Cleaning and Building Flink Shadow Jar ==="
    gradle --gradle-user-home "$DIR/.gradle-home" --no-daemon clean shadowJar
    echo "✔ Build successful: $JAR_FILE"
}

upload_jar() {
    echo "=== 2. Uploading Flink Stream Jar to MinIO (bucket: $MINIO_BUCKET, path: $REMOTE_JAR_PATH) ==="
    if [ ! -f "$JAR_FILE" ]; then
        echo "❌ Error: Jar file not found at $JAR_FILE. Please run './deploy_stream.sh build' first."
        exit 1
    fi

    # Set local MC config directory to bypass home directory permission limits
    mkdir -p "$DIR/.mc"
    export MC_CONFIG_DIR="$DIR/.mc"

    local PF_PID=""
    setup_port_forward "$MINIO_PORT" "minio" PF_PID

    local EXIT_CODE=0
    if command -v mc &> /dev/null; then
        echo "Using mc (MinIO Client) for upload..."
        mc alias set local-minio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api s3v4 --quiet
        mc mb local-minio/"$MINIO_BUCKET" 2>/dev/null || true
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

start_stream() {
    echo "=== 3. Starting all ${#JOBS[@]} Kaizen Flink Jobs (via kubectl cp + local path) ==="
    
    local JM_INFO
    JM_INFO=$(get_flink_jobmanager_pod)
    local JM_NS
    local JM_POD
    JM_NS=$(echo "$JM_INFO" | awk '{print $1}')
    JM_POD=$(echo "$JM_INFO" | awk '{print $2}')

    if [ -z "$JM_POD" ]; then
        echo "❌ Error: Flink JobManager pod is not available."
        exit 1
    fi
    echo "Using Flink JobManager: $JM_POD in namespace $JM_NS"

    # Copy JAR into the JobManager container (avoids s3a:// URI issues)
    local CONTAINER_JAR="/tmp/kaizen-stream-processing.jar"
    echo "Copying JAR to container: $CONTAINER_JAR..."
    kubectl cp "$DIR/$JAR_FILE" "$JM_NS/$JM_POD:$CONTAINER_JAR"
    echo "✔ JAR copied to container."

    for job in "${JOBS[@]}"; do
        local class_name="${job##*.}"
        local job_name="Kaizen-${class_name}"
        local savepoint_file="$DIR/.savepoints/$job_name"
        
        local restore_flag=""
        if [ -f "$savepoint_file" ]; then
            local sp_path
            sp_path=$(cat "$savepoint_file")
            if [ -n "$sp_path" ]; then
                echo "Restoring job $job_name from savepoint: $sp_path"
                restore_flag="-s $sp_path"
            fi
            rm -f "$savepoint_file"
        fi

        echo "Submitting job: $job_name (class: $job)..."
        if [ -n "$restore_flag" ]; then
            kubectl exec -n "$JM_NS" "$JM_POD" -- flink run -d $restore_flag -c "$job" "$CONTAINER_JAR"
        else
            kubectl exec -n "$JM_NS" "$JM_POD" -- flink run -d -c "$job" "$CONTAINER_JAR"
        fi
    done
    
    echo "✔ All jobs submitted successfully!"

    # Verify health after 5 seconds
    echo "Waiting 5 seconds for jobs to initialize and check their health..."
    sleep 5

    local FLINK_LIST_OUT
    FLINK_LIST_OUT=$(kubectl exec -n "$JM_NS" "$JM_POD" -- flink list 2>/dev/null) || true

    local all_running=true
    for job in "${JOBS[@]}"; do
        local class_name="${job##*.}"
        local job_name="Kaizen-${class_name}"
        if ! echo "$FLINK_LIST_OUT" | grep -E "$job_name" | grep -q "RUNNING"; then
            echo "❌ Error: Job '$job_name' is NOT running!"
            all_running=false
        else
            echo "✔ Job '$job_name' is RUNNING."
        fi
    done

    if [ "$all_running" = false ]; then
        echo "⚠️ Warning: One or more Flink jobs failed to start correctly. Please run './deploy_stream.sh status' or check Flink UI."
        exit 1
    else
        echo "✔ All Flink jobs successfully verified!"
    fi
}

stop_stream() {
    echo "=== 4. Stopping all running Kaizen Flink Jobs ==="
    local JM_INFO
    JM_INFO=$(get_flink_jobmanager_pod)
    local JM_NS
    local JM_POD
    JM_NS=$(echo "$JM_INFO" | awk '{print $1}')
    JM_POD=$(echo "$JM_INFO" | awk '{print $2}')

    if [ -z "$JM_POD" ]; then
        echo "❌ Error: Flink JobManager pod is not available."
        exit 1
    fi

    # Lấy thông tin chi tiết các job đang chạy bên trong pod
    local running_jobs_info=""
    running_jobs_info=$(kubectl exec -n "$JM_NS" "$JM_POD" -- flink list 2>/dev/null | grep -E "RUNNING|CREATED|RESTARTING" | grep "Kaizen") || true
    if [ -z "$running_jobs_info" ]; then
        echo "No running Kaizen jobs found."
        return
    fi

    mkdir -p "$DIR/.savepoints"

    # Đọc danh sách dòng kết quả
    while read -r line; do
        if [ -n "$line" ]; then
            local job_id
            local job_name
            job_id=$(echo "$line" | grep -oE '[a-f0-9]{32}' | head -n 1) || true
            job_name=$(echo "$line" | grep -oE 'Kaizen-[a-zA-Z0-9_-]+' | head -n 1) || true
            
            if [ -n "$job_id" ] && [ -n "$job_name" ]; then
                echo "Stopping job: $job_name (ID: $job_id) with savepoint in s3a://$MINIO_BUCKET/savepoint..."
                local STOP_OUTPUT
                STOP_OUTPUT=$(kubectl exec -n "$JM_NS" "$JM_POD" -- sh -c "flink stop --savepointPath \"s3a://$MINIO_BUCKET/savepoint\" \"$job_id\"" 2>&1) || true
                echo "$STOP_OUTPUT"
                
                local SAVEPOINT_PATH
                SAVEPOINT_PATH=$(echo "$STOP_OUTPUT" | grep -oE 's3a?://[a-zA-Z0-9./_-]+' | head -n 1) || true
                if [ -n "$SAVEPOINT_PATH" ]; then
                    echo "✔ Saved savepoint path for $job_name: $SAVEPOINT_PATH"
                    echo "$SAVEPOINT_PATH" > "$DIR/.savepoints/$job_name"
                else
                    echo "⚠️ Failed to stop with savepoint. Cancelling job ID: $job_id..."
                    kubectl exec -n "$JM_NS" "$JM_POD" -- flink cancel "$job_id"
                    rm -f "$DIR/.savepoints/$job_name"
                fi
            fi
        fi
    done <<< "$running_jobs_info"

    echo "✔ Stopped all jobs."
}

show_status() {
    echo "=== Flink Jobs Status ==="
    local JM_INFO
    JM_INFO=$(get_flink_jobmanager_pod)
    local JM_NS
    local JM_POD
    JM_NS=$(echo "$JM_INFO" | awk '{print $1}')
    JM_POD=$(echo "$JM_INFO" | awk '{print $2}')

    if [ -z "$JM_POD" ]; then
        echo "❌ Error: Flink JobManager pod is not available."
        exit 1
    fi

    kubectl exec -n "$JM_NS" "$JM_POD" -- flink list || true
}

clean_stream() {
    echo "=== Cleaning up Flink jobs and MinIO checkpoints ==="
    
    local JM_INFO
    JM_INFO=$(get_flink_jobmanager_pod)
    local JM_NS
    local JM_POD
    JM_NS=$(echo "$JM_INFO" | awk '{print $1}')
    JM_POD=$(echo "$JM_INFO" | awk '{print $2}')

    # 1. Cancel all running Flink jobs
    if [ -n "$JM_POD" ]; then
        local running_jobs=""
        running_jobs=$(kubectl exec -n "$JM_NS" "$JM_POD" -- flink list 2>/dev/null | grep -E "RUNNING|CREATED|RESTARTING" | grep "Kaizen" | awk '{print $4}') || true
        if [ -n "$running_jobs" ]; then
            for job_id in $running_jobs; do
                echo "Cancelling active job ID: $job_id..."
                kubectl exec -n "$JM_NS" "$JM_POD" -- flink cancel "$job_id" || true
            done
        else
            echo "No running Kaizen jobs to cancel."
        fi
    else
        echo "⚠️ Warning: Flink JobManager pod not found. Skipping cancelling jobs."
    fi

    # 2. Delete local savepoint tracking files
    rm -rf "$DIR/.savepoints"
    echo "✔ Cleared local savepoint tracking files."

    # 3. Clean savepoints, checkpoints, and JAR on MinIO
    echo "Cleaning savepoints and checkpoints on MinIO..."
    local PF_PID=""
    setup_port_forward "$MINIO_PORT" "minio" PF_PID

    mkdir -p "$DIR/.mc"
    export MC_CONFIG_DIR="$DIR/.mc"

    if command -v mc &> /dev/null; then
        mc alias set local-minio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api s3v4 --quiet
        mc rm --recursive --force local-minio/"$MINIO_BUCKET"/savepoint/ >/dev/null 2>&1 || true
        mc rm --recursive --force local-minio/"$MINIO_BUCKET"/checkpoint/ >/dev/null 2>&1 || true
        mc rm --force local-minio/"$MINIO_BUCKET"/"$REMOTE_JAR_PATH" >/dev/null 2>&1 || true
        echo "✔ Savepoints, checkpoints, and JAR cleared on MinIO."
    elif command -v aws &> /dev/null; then
        AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
        aws s3 rm "s3://$MINIO_BUCKET/savepoint/" --recursive --endpoint-url "$MINIO_ENDPOINT" --no-verify-ssl >/dev/null 2>&1 || true
        AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
        aws s3 rm "s3://$MINIO_BUCKET/checkpoint/" --recursive --endpoint-url "$MINIO_ENDPOINT" --no-verify-ssl >/dev/null 2>&1 || true
        AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
        aws s3 rm "s3://$MINIO_BUCKET/$REMOTE_JAR_PATH" --endpoint-url "$MINIO_ENDPOINT" --no-verify-ssl >/dev/null 2>&1 || true
        echo "✔ Savepoints, checkpoints, and JAR cleared on MinIO."
    fi

    teardown_port_forward "$PF_PID"

    echo "✔ Clean completed successfully!"
}

# Main routing based on argument
ACTION=$1
case "$ACTION" in
    build)
        clean_and_build
        ;;
    upload)
        upload_jar
        ;;
    start)
        start_stream
        ;;
    stop)
        stop_stream
        ;;
    status)
        show_status
        ;;
    clean)
        clean_stream
        ;;
    restart)
        stop_stream
        clean_and_build
        upload_jar
        start_stream
        ;;
    stream)
        clean_and_build
        upload_jar
        start_stream
        ;;
    *)
        echo "Usage: ./deploy_stream.sh {build|upload|start|stop|status|clean|restart|stream}"
        echo "  build   : Clean and build the shadow JAR"
        echo "  upload  : Upload the built JAR to MinIO"
        echo "  start   : Deploy all ${#JOBS[@]} jobs as FlinkSessionJobs"
        echo "  stop    : Stop all active FlinkSessionJobs"
        echo "  status  : View FlinkSessionJobs status"
        echo "  clean   : Clean up FlinkSessionJobs, flush Redis, and clear checkpoints/artifacts"
        echo "  restart : Stop active jobs -> clean & build -> upload -> start jobs"
        echo "  stream  : Clean & build -> upload -> start jobs in one go"
        exit 1
        ;;
esac
