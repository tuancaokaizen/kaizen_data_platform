#!/bin/bash
# Create Kaizen Kafka topics with 2-day retention (OOM-safe defaults).
# Tạo topic Kafka Kaizen — retention 2 ngày, giới hạn size để tránh OOM.
set -euo pipefail

if [ -n "${K3S_CONF:-}" ]; then
  export KUBECONFIG="$K3S_CONF"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
  export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

BOOT="${KAFKA_BOOTSTRAP:-kaizen-kafka.streaming.svc.cluster.local:9092}"
NAMESPACE="${KAFKA_ADMIN_NAMESPACE:-orchestrator}"
PARTITIONS="${PARTITIONS:-3}"
RETENTION_MS="${RETENTION_MS:-172800000}"       # 2 days / 2 ngày
# 256MB/partition — 3 partitions × 2 topics ≈ 1.5GB max (safe for 2Gi broker)
RETENTION_BYTES="${RETENTION_BYTES:-268435456}"
SEGMENT_BYTES="${SEGMENT_BYTES:-67108864}"      # 64MB — faster segment GC / GC segment nhanh hơn
SEGMENT_MS="${SEGMENT_MS:-1800000}"             # 30 min roll / Xoay segment 30 phút

TOPICS=(order-bundles payment-bundles)

echo "Bootstrap: $BOOT"
echo "Partitions: $PARTITIONS | Retention: ${RETENTION_MS}ms, max ${RETENTION_BYTES} bytes/partition"

kubectl run "kafka-create-topics-$(date +%s)" --rm -i --restart=Never -n "$NAMESPACE" \
  --image=confluentinc/cp-kafka:7.4.0 --command -- bash -c "
set -e
BOOT='$BOOT'
for TOPIC in ${TOPICS[*]}; do
  kafka-topics --bootstrap-server \$BOOT --delete --topic \$TOPIC 2>/dev/null || true
done
sleep 2
for TOPIC in ${TOPICS[*]}; do
  echo \"Creating \$TOPIC ...\"
  kafka-topics --bootstrap-server \$BOOT --create --if-not-exists \
    --topic \$TOPIC --partitions $PARTITIONS --replication-factor 1 \
    --config retention.ms=$RETENTION_MS \
    --config retention.bytes=$RETENTION_BYTES \
    --config segment.bytes=$SEGMENT_BYTES \
    --config segment.ms=$SEGMENT_MS \
    --config cleanup.policy=delete \
    --config compression.type=lz4 \
    --config min.insync.replicas=1 \
    --config message.timestamp.type=CreateTime
done
kafka-topics --bootstrap-server \$BOOT --describe --topic ${TOPICS[0]}
kafka-topics --bootstrap-server \$BOOT --describe --topic ${TOPICS[1]}
"

echo "Done."
