#!/bin/bash
# Create / reconcile Kaizen Kafka topics (v2) on the KRaft broker.
# Tạo / đồng bộ topic Kafka Kaizen (v2) trên broker KRaft.
# Design / Thiết kế: docs/design/kafka_topic_model.md mục 5.
#
# Usage / Cách dùng:
#   deploy/create_kafka_topics.sh              # create missing, align configs / tạo topic thiếu, chỉnh cấu hình
#   deploy/create_kafka_topics.sh --recreate   # DELETE then create (data loss) / XOÁ rồi tạo lại (mất dữ liệu)
# Topic names must match config/Topic.java (TopicTest) / Tên topic phải khớp config/Topic.java (TopicTest)
set -euo pipefail

if [ -n "${K3S_CONF:-}" ]; then
  export KUBECONFIG="$K3S_CONF"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
  export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

NS="${KAFKA_NAMESPACE:-streaming}"
POD="${KAFKA_POD:-kaizen-kafka-kraft-0}"
RECREATE=false
[ "${1:-}" = "--recreate" ] && RECREATE=true

MIB=1048576
HOUR_MS=3600000
DAY_MS=86400000

# name | partitions | retention.ms | retention.bytes | segment.ms
# tên | số partition | retention.ms | retention.bytes/partition | segment.ms
TOPICS=(
  "order-bundles|6|$((3 * DAY_MS))|$((128 * MIB))|$((6 * HOUR_MS))"
  "payment-bundles|6|$((3 * DAY_MS))|$((128 * MIB))|$((6 * HOUR_MS))"
  "inventory-movements|6|$((1 * DAY_MS))|$((128 * MIB))|$((3 * HOUR_MS))"
  "logistics-events|3|$((1 * DAY_MS))|$((64 * MIB))|$((3 * HOUR_MS))"
  "order-bundles.dlq|1|$((7 * DAY_MS))|$((64 * MIB))|$((1 * DAY_MS))"
  "payment-bundles.dlq|1|$((7 * DAY_MS))|$((64 * MIB))|$((1 * DAY_MS))"
  "inventory-movements.dlq|1|$((7 * DAY_MS))|$((64 * MIB))|$((1 * DAY_MS))"
  "logistics-events.dlq|1|$((7 * DAY_MS))|$((64 * MIB))|$((1 * DAY_MS))"
)

# Shared topic configs / Cấu hình chung cho mọi topic
COMMON="cleanup.policy=delete,segment.bytes=$((32 * MIB)),segment.index.bytes=$MIB,compression.type=producer,min.insync.replicas=1,max.message.bytes=$MIB,message.timestamp.type=CreateTime"

kexec() {
  kubectl exec -n "$NS" "$POD" -- "$@"
}

BOOT=localhost:9092
echo "Broker pod: $NS/$POD | recreate=$RECREATE"

# Delete first when explicitly requested / Chỉ xoá khi gọi --recreate
if $RECREATE; then
  for spec in "${TOPICS[@]}"; do
    name="${spec%%|*}"
    kexec kafka-topics --bootstrap-server "$BOOT" --delete --if-exists --topic "$name"
  done
  sleep 3
fi

EXISTING="$(kexec kafka-topics --bootstrap-server "$BOOT" --list)"

for spec in "${TOPICS[@]}"; do
  IFS='|' read -r name parts ret_ms ret_bytes seg_ms <<< "$spec"
  configs="$COMMON,retention.ms=$ret_ms,retention.bytes=$ret_bytes,segment.ms=$seg_ms"

  if grep -qx "$name" <<< "$EXISTING"; then
    # Partition count must never change on a live topic / Không được đổi số partition khi topic đang dùng
    current=$(kexec kafka-topics --bootstrap-server "$BOOT" --describe --topic "$name" \
      | awk 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "PartitionCount:") print $(i + 1) }')
    if [ "$current" != "$parts" ]; then
      echo "ERROR: $name has $current partitions, expected $parts. Re-run with --recreate." >&2
      exit 1
    fi
    echo "Aligning configs: $name"
    kexec kafka-configs --bootstrap-server "$BOOT" --alter --entity-type topics \
      --entity-name "$name" --add-config "$configs" > /dev/null
  else
    echo "Creating: $name ($parts partitions)"
    # Pass each config as its own --config flag / Mỗi cấu hình là một cờ --config
    args=()
    IFS=',' read -ra kvs <<< "$configs"
    for kv in "${kvs[@]}"; do args+=(--config "$kv"); done
    kexec kafka-topics --bootstrap-server "$BOOT" --create --topic "$name" \
      --partitions "$parts" --replication-factor 1 "${args[@]}"
  fi
done

echo "--- Topics ---"
for spec in "${TOPICS[@]}"; do
  name="${spec%%|*}"
  # awk reads to EOF so pipefail never sees SIGPIPE / awk đọc hết input nên pipefail không gặp SIGPIPE
  kexec kafka-topics --bootstrap-server "$BOOT" --describe --topic "$name" | awk 'NR == 1'
done
echo "Done."
