#!/bin/bash
# Mount PVC for /app/superset_home — persist SQLite + embed UUIDs across pod restarts
# Gắn PVC cho superset_home — giữ SQLite và UUID nhúng khi restart pod
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS="${SUPERSET_NAMESPACE:-apps}"
DEPLOY="${SUPERSET_DEPLOY:-apache-superset}"
PVC_NAME="${SUPERSET_PVC_NAME:-superset-data}"
MOUNT_PATH="/app/superset_home"

if [ -n "${K3S_CONF:-}" ]; then
  export KUBECONFIG="$K3S_CONF"
elif [ -f "$ROOT/../../dags/config.k3s-new" ]; then
  export KUBECONFIG="$ROOT/../../dags/config.k3s-new"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
  export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

echo "=== Superset PVC ($PVC_NAME) ==="
echo "KUBECONFIG: ${KUBECONFIG:-default}"
echo "Namespace : $NS / $DEPLOY"

if ! command -v kubectl >/dev/null; then
  echo "❌ kubectl not found"
  exit 1
fi

if ! kubectl get deployment "$DEPLOY" -n "$NS" >/dev/null 2>&1; then
  echo "⚠️  Deployment $DEPLOY not found in $NS — skip PVC"
  exit 0
fi

has_pvc_mount() {
  kubectl get deployment "$DEPLOY" -n "$NS" -o json \
    | python3 -c "import json,sys; d=json.load(sys.stdin); mounts=d['spec']['template']['spec']['containers'][0].get('volumeMounts',[]); print('yes' if any(m.get('name')=='superset-data' for m in mounts) else 'no')"
}

if [ "$(has_pvc_mount)" = "yes" ]; then
  echo "✔ PVC volume already mounted on $DEPLOY"
  kubectl get pvc "$PVC_NAME" -n "$NS" 2>/dev/null || kubectl apply -f "$ROOT/k8s/superset-data-pvc.yaml"
  exit 0
fi

# Backup metadata from running pod before first PVC mount / Sao lưu trước khi gắn PVC lần đầu
BACKUP_DIR="${TMPDIR:-/tmp}/superset-home-backup-$$"
POD=""
if POD=$(kubectl get pods -n "$NS" -l app=apache-superset -o jsonpath='{.items[0].metadata.name}' 2>/dev/null); then
  if [ -n "$POD" ]; then
    echo "→ Backup $MOUNT_PATH from pod $POD ..."
    rm -rf "$BACKUP_DIR"
    mkdir -p "$BACKUP_DIR"
    if kubectl cp "$NS/$POD:$MOUNT_PATH/." "$BACKUP_DIR/" 2>/dev/null; then
      if [ -f "$BACKUP_DIR/superset.db" ]; then
        echo "✔ Backed up superset.db ($(du -h "$BACKUP_DIR/superset.db" | awk '{print $1}'))"
      else
        echo "⚠️  Backup folder empty — fresh Superset DB will be created on PVC"
        rm -rf "$BACKUP_DIR"
        BACKUP_DIR=""
      fi
    else
      echo "⚠️  kubectl cp failed — continuing without backup"
      rm -rf "$BACKUP_DIR"
      BACKUP_DIR=""
    fi
  fi
fi

kubectl apply -f "$ROOT/k8s/superset-data-pvc.yaml"
echo "→ PVC created (local-path binds when pod mounts volume) ..."

echo "→ Patch deployment to mount PVC at $MOUNT_PATH ..."
kubectl patch deployment "$DEPLOY" -n "$NS" --type=json -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/volumes/-",
    "value": {
      "name": "superset-data",
      "persistentVolumeClaim": { "claimName": "'"$PVC_NAME"'" }
    }
  },
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/volumeMounts/-",
    "value": {
      "name": "superset-data",
      "mountPath": "'"$MOUNT_PATH"'"
    }
  }
]' 2>/dev/null || kubectl patch deployment "$DEPLOY" -n "$NS" --type=strategic -p "
spec:
  template:
    spec:
      volumes:
        - name: superset-data
          persistentVolumeClaim:
            claimName: $PVC_NAME
      containers:
        - name: superset
          volumeMounts:
            - name: superset-data
              mountPath: $MOUNT_PATH
"

echo "→ Rolling restart ..."
kubectl rollout restart deployment/"$DEPLOY" -n "$NS"
kubectl rollout status deployment/"$DEPLOY" -n "$NS" --timeout=300s

kubectl wait --for=condition=Bound "pvc/$PVC_NAME" -n "$NS" --timeout=180s 2>/dev/null || true

NEW_POD=$(kubectl get pods -n "$NS" -l app=apache-superset -o jsonpath='{.items[0].metadata.name}')
echo "→ New pod: $NEW_POD"

# Restore backup if PVC DB is smaller than backup (fresh init overwrote empty PVC)
# Khôi phục nếu DB trên PVC nhỏ hơn bản backup
if [ -n "${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ] && [ -f "$BACKUP_DIR/superset.db" ]; then
  BACKUP_SIZE=$(stat -f%z "$BACKUP_DIR/superset.db" 2>/dev/null || stat -c%s "$BACKUP_DIR/superset.db")
  CURRENT_SIZE=0
  if kubectl exec -n "$NS" "$NEW_POD" -- test -f "$MOUNT_PATH/superset.db" 2>/dev/null; then
    CURRENT_SIZE=$(kubectl exec -n "$NS" "$NEW_POD" -- stat -c%s "$MOUNT_PATH/superset.db" 2>/dev/null || echo 0)
  fi
  NEED_RESTORE="no"
  if [ "${CURRENT_SIZE:-0}" -lt "$((BACKUP_SIZE * 9 / 10))" ]; then
    NEED_RESTORE="yes"
  fi
  if [ "$NEED_RESTORE" = "yes" ]; then
    echo "→ Restoring superset_home (backup ${BACKUP_SIZE}B > current ${CURRENT_SIZE}B) ..."
    kubectl exec -n "$NS" "$NEW_POD" -- sh -c "rm -rf ${MOUNT_PATH}/*" 2>/dev/null || true
    kubectl cp "$BACKUP_DIR/." "$NS/$NEW_POD:$MOUNT_PATH/"
    kubectl rollout restart deployment/"$DEPLOY" -n "$NS"
    kubectl rollout status deployment/"$DEPLOY" -n "$NS" --timeout=300s
    echo "✔ Restored metadata from backup"
  else
    echo "✔ PVC superset.db OK (${CURRENT_SIZE}B) — skip restore"
  fi
  rm -rf "$BACKUP_DIR"
fi

echo ""
echo "✔ Superset PVC ready — dashboard IDs / embed UUIDs survive pod restarts"
echo "  PVC: $PVC_NAME ($NS)"
echo "  Mount: $MOUNT_PATH"
