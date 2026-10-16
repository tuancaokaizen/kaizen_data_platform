#!/bin/bash
# patch_airflow_optimize.sh — PYTHONPATH, logs PVC, log cleanup sidecar
# Tối ưu Airflow: import jobs.*, mount log PVC, tự dọn log >7 ngày
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

if [ -n "${K3S_CONF:-}" ]; then
  export KUBECONFIG="$K3S_CONF"
elif [ -f "$DIR/config.k3s-new" ]; then
  export KUBECONFIG="$DIR/config.k3s-new"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
  export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

NAMESPACE="orchestrator"
PATCH_FILE="$DIR/k8s/airflow-lab-optimize.yaml"

echo "=== 1. Upload DAGs (incl. jobs/__init__.py) to MinIO ==="
"$DIR/deploy_airflow.sh" upload

echo ""
echo "=== 2. Patch scheduler + webserver (PYTHONPATH, logs PVC, cleanup sidecar) ==="
# Strategic merge patches — avoid partial apply deleting existing containers
# Patch merge chiến lược — tránh apply partial xóa container hiện có
kubectl set env statefulset/airflow-lab-scheduler -n "$NAMESPACE" -c scheduler PYTHONPATH=/opt/airflow/dags
kubectl set env deployment/airflow-lab-webserver -n "$NAMESPACE" -c webserver PYTHONPATH=/opt/airflow/dags

kubectl patch statefulset airflow-lab-scheduler -n "$NAMESPACE" --type='strategic' -p '{
  "spec": {"template": {"spec": {"volumes": [{"name": "logs", "persistentVolumeClaim": {"claimName": "logs-airflow-lab-scheduler-0"}}]}}}
}'

kubectl patch statefulset airflow-lab-scheduler -n "$NAMESPACE" --type='strategic' -p '{
  "spec": {"template": {"spec": {"containers": [{"name": "scheduler", "volumeMounts": [{"name": "logs", "mountPath": "/opt/airflow/logs"}]}]}}}
}'

kubectl patch statefulset airflow-lab-scheduler -n "$NAMESPACE" --type='strategic' -p '{
  "spec": {"template": {"spec": {"containers": [{"name": "log-cleanup", "image": "busybox:1.36", "command": ["/bin/sh", "-c"], "args": ["while true; do\n  BEFORE=$(du -sh /opt/airflow/logs 2>/dev/null | cut -f1 || echo ?)\n  DELETED=$(find /opt/airflow/logs -type f -mtime +7 2>/dev/null | wc -l)\n  find /opt/airflow/logs -type f -mtime +7 -delete 2>/dev/null || true\n  find /opt/airflow/logs -type d -empty -delete 2>/dev/null || true\n  echo \"[$(date -Iseconds)] log-cleanup before=${BEFORE} deleted=${DELETED}\"\n  sleep 86400\ndone\n"], "volumeMounts": [{"name": "logs", "mountPath": "/opt/airflow/logs"}]}]}}}
}'

echo ""
echo "=== 3. Wait for rollout ==="
kubectl rollout status statefulset/airflow-lab-scheduler -n "$NAMESPACE" --timeout=180s
kubectl rollout status deployment/airflow-lab-webserver -n "$NAMESPACE" --timeout=180s

echo ""
echo "=== 4. Verify DAG imports ==="
sleep 15
kubectl exec -n "$NAMESPACE" airflow-lab-scheduler-0 -c scheduler -- \
  airflow dags list-import-errors 2>&1 | head -20

echo ""
kubectl exec -n "$NAMESPACE" airflow-lab-scheduler-0 -c scheduler -- \
  airflow dags list 2>&1 | grep -E "postgres_vacuum|kafka_retention|spark_dw_transform" || true

echo ""
echo "=== 5. Verify log mount + PYTHONPATH ==="
kubectl exec -n "$NAMESPACE" airflow-lab-scheduler-0 -c scheduler -- sh -c '
  echo "PYTHONPATH=$PYTHONPATH"
  df -h /opt/airflow/logs | tail -1
  du -sh /opt/airflow/logs
'

echo ""
echo "✔ Airflow optimization applied."
