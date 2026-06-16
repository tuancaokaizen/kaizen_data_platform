#!/bin/bash
# patch_airflow_dag_sync.sh
# Patch Airflow scheduler + webserver để sync DAGs từ MinIO
set -euo pipefail

# Get directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# Prioritize K3S_CONF or config.k3s-new over standard KUBECONFIG
if [ -n "${K3S_CONF:-}" ]; then
    export KUBECONFIG="$K3S_CONF"
elif [ -f "$DIR/config.k3s-new" ]; then
    export KUBECONFIG="$DIR/config.k3s-new"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
    export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

NAMESPACE="orchestrator"
MINIO_ENDPOINT="http://minio.storage.svc.cluster.local:9000"
MINIO_ACCESS_KEY="admin"
MINIO_SECRET_KEY="kaizen123"
MINIO_BUCKET="airflow"
SYNC_INTERVAL=30  # giây

echo "=== Patching Airflow to sync DAGs from MinIO ==="

# ─── 1. Tạo Secret cho MinIO credentials ────────────────────────────────────
echo "Creating MinIO credentials secret..."
kubectl create secret generic airflow-minio-secret \
  -n "$NAMESPACE" \
  --from-literal=MINIO_ACCESS_KEY="$MINIO_ACCESS_KEY" \
  --from-literal=MINIO_SECRET_KEY="$MINIO_SECRET_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -

# ─── 2. Patch StatefulSet (scheduler) ───────────────────────────────────────
SCHEDULER_CONTAINER="scheduler"
echo "Target scheduler container: '$SCHEDULER_CONTAINER'"

echo "Patching scheduler StatefulSet..."
kubectl patch statefulset airflow-lab-scheduler \
  -n "$NAMESPACE" \
  --patch "
spec:
  template:
    spec:
      volumes:
      - name: dags
        emptyDir: {}
      initContainers:
      - name: init-dag-sync
        image: minio/mc:latest
        command: [\"/bin/sh\", \"-c\"]
        args:
        - |
          mc alias set minio \$MINIO_ENDPOINT \$MINIO_ACCESS_KEY \$MINIO_SECRET_KEY --api s3v4 &&
          mc mirror --overwrite minio/airflow/dags/ /opt/airflow/dags/ &&
          echo 'Initial DAG sync done'
        env:
        - name: MINIO_ENDPOINT
          value: \"$MINIO_ENDPOINT\"
        - name: MINIO_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-minio-secret
              key: MINIO_ACCESS_KEY
        - name: MINIO_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-minio-secret
              key: MINIO_SECRET_KEY
        volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
      containers:
      - name: dag-sync
        image: minio/mc:latest
        command: [\"/bin/sh\", \"-c\"]
        args:
        - |
          mc alias set minio \$MINIO_ENDPOINT \$MINIO_ACCESS_KEY \$MINIO_SECRET_KEY --api s3v4;
          while true; do
            mc mirror --overwrite --remove minio/airflow/dags/ /opt/airflow/dags/ &&
            echo \"[\$(date)] DAG sync completed\" ||
            echo \"[\$(date)] DAG sync failed, retrying...\";
            sleep 30;
          done
        env:
        - name: MINIO_ENDPOINT
          value: \"$MINIO_ENDPOINT\"
        - name: MINIO_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-minio-secret
              key: MINIO_ACCESS_KEY
        - name: MINIO_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-minio-secret
              key: MINIO_SECRET_KEY
        volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
      - name: $SCHEDULER_CONTAINER
        command:
        - \"/bin/bash\"
        - \"-c\"
        - \"if [ -f /opt/airflow/dags/requirements.txt ]; then pip install -r /opt/airflow/dags/requirements.txt; fi && pip install apache-airflow-providers-postgres==6.0.0 apache-airflow-providers-neo4j==3.7.0 apache-airflow-providers-apache-spark==4.11.0 && airflow scheduler\"
        volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
"

# ─── 3. Patch Deployment (webserver) ────────────────────────────────────────
WEBSERVER_CONTAINER="webserver"
echo "Target webserver container: '$WEBSERVER_CONTAINER'"

echo "Patching webserver Deployment..."
kubectl patch deployment airflow-lab-webserver \
  -n "$NAMESPACE" \
  --patch "
spec:
  template:
    spec:
      volumes:
      - name: dags
        emptyDir: {}
      initContainers:
      - name: init-dag-sync
        image: minio/mc:latest
        command: [\"/bin/sh\", \"-c\"]
        args:
        - |
          mc alias set minio \$MINIO_ENDPOINT \$MINIO_ACCESS_KEY \$MINIO_SECRET_KEY --api s3v4 &&
          mc mirror --overwrite minio/airflow/dags/ /opt/airflow/dags/ &&
          echo 'Initial DAG sync done'
        env:
        - name: MINIO_ENDPOINT
          value: \"$MINIO_ENDPOINT\"
        - name: MINIO_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-minio-secret
              key: MINIO_ACCESS_KEY
        - name: MINIO_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-minio-secret
              key: MINIO_SECRET_KEY
        volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
      containers:
      - name: dag-sync
        image: minio/mc:latest
        command: [\"/bin/sh\", \"-c\"]
        args:
        - |
          mc alias set minio \$MINIO_ENDPOINT \$MINIO_ACCESS_KEY \$MINIO_SECRET_KEY --api s3v4;
          while true; do
            mc mirror --overwrite --remove minio/airflow/dags/ /opt/airflow/dags/ &&
            echo \"[\$(date)] DAG sync completed\" ||
            echo \"[\$(date)] DAG sync failed, retrying...\";
            sleep 30;
          done
        env:
        - name: MINIO_ENDPOINT
          value: \"$MINIO_ENDPOINT\"
        - name: MINIO_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-minio-secret
              key: MINIO_ACCESS_KEY
        - name: MINIO_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: airflow-minio-secret
              key: MINIO_SECRET_KEY
        volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
      - name: $WEBSERVER_CONTAINER
        command:
        - \"/bin/bash\"
        - \"-c\"
        - \"if [ -f /opt/airflow/dags/requirements.txt ]; then pip install -r /opt/airflow/dags/requirements.txt; fi && pip install apache-airflow-providers-postgres==6.0.0 apache-airflow-providers-neo4j==3.7.0 apache-airflow-providers-apache-spark==4.11.0 && airflow webserver\"
        volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
"

echo ""
echo "✔ Patch applied! Waiting for pods to restart..."
kubectl rollout status statefulset/airflow-lab-scheduler -n "$NAMESPACE" --timeout=120s || true
kubectl rollout status deployment/airflow-lab-webserver -n "$NAMESPACE" --timeout=120s || true

echo ""
echo "✔ Done! DAG sync sidecar is now running."
echo "  - Syncs from: MinIO airflow/dags/ every ${SYNC_INTERVAL}s"
echo "  - DAGs available at: /opt/airflow/dags/ inside pods"
echo ""
echo "Verify with:"
echo "  kubectl exec -n $NAMESPACE airflow-lab-scheduler-0 -c scheduler -- ls /opt/airflow/dags/"
