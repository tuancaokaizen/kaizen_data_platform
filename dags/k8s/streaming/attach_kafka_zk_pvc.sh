#!/usr/bin/env bash
# Attach PVC to kaizen-kafka + zookeeper (local-path).
# Gắn PVC cho Kafka/ZK. Strategy Recreate (RWO).
#
# Notes / Lưu ý:
# - Migrating live log dirs is fragile (clusterId mismatch). Prefer empty PVC + recreate topics.
# - Copy live data dễ lệch clusterId với ZK → ưu tiên PVC trống + tạo lại topic.
#
# Usage:
#   ./dags/k8s/streaming/attach_kafka_zk_pvc.sh
set -euo pipefail

export KUBECONFIG="${K3S_CONF:-$HOME/.kube/config.k3s-new}"
NS=streaming
NODE="${KAFKA_NODE:-tuan-cao-rog-strix-g512lv-g512lv}"
DIR="$(cd "$(dirname "$0")" && pwd)"
JM="${FLINK_JM_POD:-kaizen-flink-jobmanager-f496cdb4-rgjhh}"

echo "=== 1) Ensure PVCs ==="
kubectl apply -f "$DIR/kafka-zk-pvc.yaml"

echo "=== 2) Patch Zookeeper (Recreate + PVC + memory) ==="
# Memory: 384Mi caused OOMKilled → empty zxid → Kafka stuck / 384Mi gây OOM → Kafka kẹt ZK
kubectl patch deploy zookeeper -n "$NS" --type=json -p='[
  {"op":"replace","path":"/spec/strategy","value":{"type":"Recreate"}},
  {"op":"add","path":"/spec/template/spec/securityContext","value":{"fsGroup":1000}},
  {"op":"add","path":"/spec/template/spec/volumes","value":[{"name":"zk-data","persistentVolumeClaim":{"claimName":"kaizen-zookeeper-data"}}]},
  {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts","value":[{"name":"zk-data","mountPath":"/var/lib/zookeeper"}]},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-","value":{"name":"ZOOKEEPER_DATA_DIR","value":"/var/lib/zookeeper/data"}},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-","value":{"name":"ZOOKEEPER_DATA_LOG_DIR","value":"/var/lib/zookeeper/log"}},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-","value":{"name":"KAFKA_HEAP_OPTS","value":"-Xms256m -Xmx512m"}},
  {"op":"add","path":"/spec/template/spec/containers/0/resources","value":{"requests":{"cpu":"50m","memory":"512Mi"},"limits":{"cpu":"500m","memory":"1Gi"}}}
]' 2>/dev/null || echo "ZK already patched (or path exists) — verify manually"
# Ensure memory/heap even if PVC patch already applied / Đảm bảo mem/heap dù PVC đã gắn
kubectl patch deploy zookeeper -n "$NS" --type=json -p='[
  {"op":"replace","path":"/spec/template/spec/containers/0/resources","value":{"requests":{"cpu":"50m","memory":"512Mi"},"limits":{"cpu":"500m","memory":"1Gi"}}}
]' 2>/dev/null || true
kubectl rollout status deploy/zookeeper -n "$NS" --timeout=180s

echo "=== 3) Patch Kafka (Recreate + PVC) ==="
kubectl patch deploy kaizen-kafka -n "$NS" --type=json -p='[
  {"op":"replace","path":"/spec/strategy","value":{"type":"Recreate"}},
  {"op":"add","path":"/spec/template/spec/securityContext","value":{"fsGroup":1000}},
  {"op":"add","path":"/spec/template/spec/volumes","value":[{"name":"kafka-data","persistentVolumeClaim":{"claimName":"kaizen-kafka-data"}}]},
  {"op":"add","path":"/spec/template/spec/containers/0/volumeMounts","value":[{"name":"kafka-data","mountPath":"/var/lib/kafka/data"}]},
  {"op":"add","path":"/spec/template/spec/containers/0/env/-","value":{"name":"KAFKA_LOG_DIRS","value":"/var/lib/kafka/data"}}
]' 2>/dev/null || echo "Kafka already patched — verify manually"
kubectl rollout status deploy/kaizen-kafka -n "$NS" --timeout=240s

echo "=== 4) Verify ==="
kubectl get pvc -n "$NS" | grep -E 'kaizen-kafka-data|kaizen-zookeeper-data'
KAFKA_POD=$(kubectl get pod -n "$NS" -l app=kaizen-kafka -o jsonpath='{.items[0].metadata.name}')
kubectl get pod -n "$NS" -l 'app in (kaizen-kafka,zookeeper)' -o wide
kubectl exec -n "$NS" "$KAFKA_POD" -- df -h /var/lib/kafka/data
kubectl exec -n "$NS" "$KAFKA_POD" -- kafka-topics --bootstrap-server localhost:9092 --list || true
echo "DONE"
