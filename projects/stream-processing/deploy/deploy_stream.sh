#!/bin/bash
# Build and run the Kaizen stream jobs on the Flink session cluster (compute/kaizen-flink-*).
# Build và chạy các stream job Kaizen trên Flink session cluster (compute/kaizen-flink-*).
#
# Jobs = every src/main/java/com/kaizen/stream/{processing,simulation}/<pipeline>/*Job.java.
# Group = the folder (processing | simulation); Flink name = "Kaizen-<group>-<name()>".
# Job = mọi src/main/java/com/kaizen/stream/{processing,simulation}/<pipeline>/*Job.java.
# Nhóm = thư mục (processing | simulation); tên Flink = "Kaizen-<nhóm>-<name()>".
# Selection / Chọn job: a group (processing | simulation), a full name (simulation-order) or a short name (order).
# No credentials are read from config.ini: jobs get them from Secret kaizen-stream-secrets,
# `clean` reads MinIO keys from Secret kaizen-flink-s3 into env only.
# Không đọc credential từ config.ini: job lấy từ Secret kaizen-stream-secrets,
# `clean` đọc khoá MinIO từ Secret kaizen-flink-s3 và chỉ giữ trong env.
set -euo pipefail

# Prefer K3S_CONF, then the lab cluster config / Ưu tiên K3S_CONF, rồi config của cluster lab
if [ -n "${K3S_CONF:-}" ]; then
    export KUBECONFIG="$K3S_CONF"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
    export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

# Project root is the parent of deploy/ / Gốc project là thư mục cha của deploy/
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)"
SOURCE_ROOT="$DIR/src/main/java"
JOB_GROUPS=(processing simulation)
JAR_FILE="$DIR/build/libs/kaizen-stream-processing-1.0-SNAPSHOT.jar"
CONTAINER_JAR="/tmp/kaizen-stream-processing.jar"
SAVEPOINT_DIR="$DIR/.savepoints"

FLINK_NS="compute"
FLINK_JM_LABEL="component=jobmanager"
STATE_BUCKET="flink"
SAVEPOINT_URI="s3a://$STATE_BUCKET/savepoint"

# ---------------------------------------------------------------------------
# Job discovery / Tìm job
# ---------------------------------------------------------------------------

# Short name returned by name(), e.g. "order" (ArchitectureTest keeps it a literal)
# Tên ngắn trả về từ name(), vd. "order" (ArchitectureTest bắt buộc là chuỗi hằng)
job_short_name() {
    grep -A2 'String name()' "$1" | grep -oE 'return "[a-z0-9-]+";' | head -1 | sed -E 's/return "(.*)";/\1/'
}

# Lines "<class> <Kaizen-name> <group> <short name>", filtered by group, full name or short name
# Các dòng "<class> <Kaizen-name> <nhóm> <tên ngắn>", lọc theo nhóm, tên đầy đủ hoặc tên ngắn
list_jobs() {
    local only="${1:-}" group f name class
    for group in "${JOB_GROUPS[@]}"; do
        # Only <group>/<pipeline>/*Job.java; processing/job/ is the job framework
        # Chỉ <nhóm>/<pipeline>/*Job.java; processing/job/ là khung chạy job
        for f in "$SOURCE_ROOT/com/kaizen/stream/$group"/*/*Job.java; do
            [ -f "$f" ] || continue
            [ "$(basename "$(dirname "$f")")" = "job" ] && continue
            name="$(job_short_name "$f")"
            if [ -z "$name" ]; then
                echo "❌ Cannot read name() literal in $f" >&2
                exit 1
            fi
            if [ -z "$only" ] || [ "$only" = "$group" ] || [ "$only" = "$group-$name" ] || [ "$only" = "$name" ]; then
                class="${f#"$SOURCE_ROOT/"}"
                class="${class%.java}"
                echo "${class//\//.} Kaizen-$group-$name $group $name"
            fi
        done
    done
}

selected_jobs() {
    local jobs
    jobs="$(list_jobs "${1:-}")"
    if [ -z "$jobs" ]; then
        echo "❌ No job matches '${1:-}'. Available: ${JOB_GROUPS[*]} $(list_jobs | awk '{print $3 "-" $4}' | tr '\n' ' ')" >&2
        exit 1
    fi
    # A short name used in both groups is ambiguous / Tên ngắn trùng ở 2 nhóm thì không rõ job nào
    if [ "$(echo "$jobs" | awk '{print $3}' | sort -u | wc -l)" -gt 1 ] && [ "${1:-}" != "" ] \
        && ! printf '%s\n' "${JOB_GROUPS[@]}" | grep -qx "$1"; then
        echo "❌ '$1' matches several jobs, use the full name <group>-<name>" >&2
        exit 1
    fi
    echo "$jobs"
}

# ---------------------------------------------------------------------------
# Flink helpers / Hàm hỗ trợ Flink
# ---------------------------------------------------------------------------

jm_pod() {
    local pod
    pod="$(kubectl get pods -n "$FLINK_NS" -l "$FLINK_JM_LABEL" --field-selector=status.phase=Running \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [ -z "$pod" ]; then
        echo "❌ No running JobManager in namespace $FLINK_NS" >&2
        exit 1
    fi
    echo "$pod"
}

flink_cli() {
    kubectl exec -n "$FLINK_NS" "$JM_POD" -- flink "$@"
}

# "<jobId> <Kaizen-name>" of active jobs / "<jobId> <Kaizen-name>" của job đang chạy
active_jobs() {
    flink_cli list 2>/dev/null | grep -E '\((RUNNING|CREATED|RESTARTING)\)' \
        | sed -nE 's/.* : ([a-f0-9]{32}) : (Kaizen-[A-Za-z0-9_-]+) .*/\1 \2/p' || true
}

# ---------------------------------------------------------------------------
# Actions / Thao tác
# ---------------------------------------------------------------------------

build() {
    echo "=== Build: tests + shadow jar ==="
    (cd "$DIR" && gradle --gradle-user-home "$DIR/.gradle-home" --no-daemon clean test shadowJar)
    echo "✔ $JAR_FILE"
}

start() {
    local only="${1:-}" jobs active class name group short savepoint_file sp
    jobs="$(selected_jobs "$only")"
    [ -f "$JAR_FILE" ] || { echo "❌ $JAR_FILE missing, run '$0 build' first"; exit 1; }
    JM_POD="$(jm_pod)"
    active="$(active_jobs)"

    echo "=== Start on $FLINK_NS/$JM_POD ==="
    kubectl cp "$JAR_FILE" "$FLINK_NS/$JM_POD:$CONTAINER_JAR"

    while read -r class name group short; do
        # Never run two copies of one job: they would share consumer groups
        # Không chạy 2 bản của cùng job: chúng sẽ dùng chung consumer group
        if echo "$active" | grep -q " $name\$"; then
            echo "⏭  $name is already running, skipped (use '$0 stop $group-$short' first)"
            continue
        fi
        local args=(run -d)
        savepoint_file="$SAVEPOINT_DIR/$group-$short"
        if [ -s "$savepoint_file" ]; then
            sp="$(cat "$savepoint_file")"
            echo "↺  $name restores from $sp"
            args+=(-s "$sp")
        fi
        echo "▶  $name ($class)"
        flink_cli "${args[@]}" -c "$class" "$CONTAINER_JAR"
        rm -f "$savepoint_file"
    done <<< "$jobs"

    echo "Waiting 10s, then checking state..."
    sleep 10
    local failed=0
    while read -r class name _; do
        if flink_cli list 2>/dev/null | grep " : $name " | grep -q '(RUNNING)'; then
            echo "✔ $name RUNNING"
        else
            echo "❌ $name is not RUNNING (see '$0 status' or the Flink UI)"
            failed=1
        fi
    done <<< "$jobs"
    return "$failed"
}

stop() {
    local only="${1:-}" jobs active id name group short out sp failed=0
    jobs="$(selected_jobs "$only")"
    JM_POD="$(jm_pod)"
    active="$(active_jobs)"
    mkdir -p "$SAVEPOINT_DIR"

    echo "=== Stop with savepoint ($SAVEPOINT_URI) ==="
    while read -r _ name group short; do
        id="$(echo "$active" | awk -v n="$name" '$2 == n {print $1}' | head -1)"
        if [ -z "$id" ]; then
            echo "·  $name is not running"
            continue
        fi
        out="$(flink_cli stop --savepointPath "$SAVEPOINT_URI" "$id" 2>&1)" || true
        sp="$(echo "$out" | grep -oE 's3a?://[A-Za-z0-9./_-]+' | grep savepoint- | head -1 || true)"
        if [ -n "$sp" ]; then
            echo "$sp" > "$SAVEPOINT_DIR/$group-$short"
            echo "■  $name stopped, savepoint $sp"
        else
            # No cancel fallback: cancelling would lose state; the job keeps running
            # Không tự cancel: cancel làm mất state; job vẫn tiếp tục chạy
            echo "$out" | tail -20
            echo "❌ $name: stop-with-savepoint failed, job left RUNNING. Fix the cause, or cancel by hand: flink cancel $id"
            failed=1
        fi
    done <<< "$jobs"
    return "$failed"
}

status() {
    JM_POD="$(jm_pod)"
    echo "=== Jobs in source code ==="
    list_jobs | awk '{printf "  %-12s %-40s %s\n", $3, $2, $1}'
    echo "=== Flink ($FLINK_NS/$JM_POD) ==="
    flink_cli list -a || true
    if compgen -G "$SAVEPOINT_DIR/*" >/dev/null; then
        echo "=== Pending savepoints (used by next start) ==="
        for f in "$SAVEPOINT_DIR"/*; do echo "  $(basename "$f"): $(cat "$f")"; done
    fi
}

# Cancel every Kaizen job and wipe its state; consumer groups keep their committed offsets
# Huỷ mọi job Kaizen và xoá state; consumer group vẫn giữ offset đã commit
clean() {
    JM_POD="$(jm_pod)"
    echo "This cancels ALL Kaizen jobs WITHOUT savepoint and deletes:"
    echo "  - local $SAVEPOINT_DIR"
    echo "  - s3://$STATE_BUCKET/checkpoint/ and s3://$STATE_BUCKET/savepoint/ on MinIO"
    read -r -p "Type 'yes' to continue: " answer
    [ "$answer" = "yes" ] || { echo "Aborted."; exit 1; }

    local id name
    while read -r id name; do
        [ -n "$id" ] || continue
        echo "✖  cancel $name ($id)"
        flink_cli cancel "$id" || true
    done <<< "$(active_jobs)"
    rm -rf "$SAVEPOINT_DIR"

    command -v mc >/dev/null || { echo "❌ 'mc' (MinIO client) is required for clean"; exit 1; }
    local pf_pid="" ak sk
    if ! nc -z 127.0.0.1 19000 2>/dev/null; then
        kubectl port-forward -n storage svc/minio 19000:9000 >/dev/null 2>&1 &
        pf_pid=$!
        sleep 3
    fi
    ak="$(kubectl get secret -n "$FLINK_NS" kaizen-flink-s3 -o jsonpath='{.data.access-key}' | base64 -d)"
    sk="$(kubectl get secret -n "$FLINK_NS" kaizen-flink-s3 -o jsonpath='{.data.secret-key}' | base64 -d)"
    # Credentials only in env, never in argv / Credential chỉ nằm trong env, không nằm trong argv
    export MC_CONFIG_DIR="$DIR/.mc"
    export MC_HOST_kzflink="http://$ak:$sk@127.0.0.1:19000"
    mc rm --recursive --force "kzflink/$STATE_BUCKET/checkpoint/" >/dev/null 2>&1 || true
    mc rm --recursive --force "kzflink/$STATE_BUCKET/savepoint/" >/dev/null 2>&1 || true
    unset MC_HOST_kzflink ak sk
    [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true
    echo "✔ Clean done"
}

usage() {
    cat <<EOF
Usage: $0 <action> [selection]
  build                  gradle clean test shadowJar
  start   [selection]    submit jobs (restores pending savepoints); skips jobs already running
  stop    [selection]    stop with savepoint; on failure the job keeps running
  restart [selection]    stop → build → start
  stream  [selection]    build → start
  status                 jobs in source code, Flink job list, pending savepoints
  clean                  cancel all Kaizen jobs and delete checkpoints/savepoints (asks for 'yes')
Selection: empty (all) | ${JOB_GROUPS[*]} | <group>-<name> | <name>
Jobs: $(list_jobs | awk '{print $3 "-" $4}' | tr '\n' ' ')
EOF
}

case "${1:-}" in
    build) build ;;
    start) start "${2:-}" ;;
    stop) stop "${2:-}" ;;
    restart) stop "${2:-}"; build; start "${2:-}" ;;
    stream) build; start "${2:-}" ;;
    status) status ;;
    clean) clean ;;
    *) usage; exit 1 ;;
esac
