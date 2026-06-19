#!/bin/bash
# Enable EMBEDDED_SUPERSET on K3s Superset deployment
# Bật EMBEDDED_SUPERSET trên deployment Superset K3s
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS="${SUPERSET_NAMESPACE:-apps}"
DEPLOY="${SUPERSET_DEPLOY:-apache-superset}"

if [ -n "${K3S_CONF:-}" ]; then
  export KUBECONFIG="$K3S_CONF"
elif [ -f "$ROOT/../../dags/config.k3s-new" ]; then
  export KUBECONFIG="$ROOT/../../dags/config.k3s-new"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
  export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

echo "=== Enable Superset embedded (K8s) ==="
echo "KUBECONFIG: ${KUBECONFIG:-default}"
echo "Namespace : $NS / $DEPLOY"

kubectl apply -f "$ROOT/k8s/superset-embed-configmap.yaml"

# Mount superset_config.py into pythonpath / Gắn superset_config.py vào pythonpath
kubectl patch deployment "$DEPLOY" -n "$NS" --type=json -p='[
  {
    "op": "add",
    "path": "/spec/template/spec/volumes/-",
    "value": {
      "name": "superset-embed-config",
      "configMap": { "name": "superset-embed-config" }
    }
  },
  {
    "op": "add",
    "path": "/spec/template/spec/containers/0/volumeMounts/-",
    "value": {
      "name": "superset-embed-config",
      "mountPath": "/app/pythonpath/superset_config.py",
      "subPath": "superset_config.py",
      "readOnly": true
    }
  }
]' 2>/dev/null || kubectl patch deployment "$DEPLOY" -n "$NS" --type=strategic -p '
spec:
  template:
    spec:
      volumes:
        - name: superset-embed-config
          configMap:
            name: superset-embed-config
      containers:
        - name: superset
          volumeMounts:
            - name: superset-embed-config
              mountPath: /app/pythonpath/superset_config.py
              subPath: superset_config.py
              readOnly: true
'

echo "→ Rolling restart Superset..."
kubectl rollout restart deployment/"$DEPLOY" -n "$NS"
kubectl rollout status deployment/"$DEPLOY" -n "$NS" --timeout=300s

echo ""
echo "✔ Done. Verify embed route (needs port-forward or NodePort):"
echo "  curl -s -o /dev/null -w '%{http_code}' http://localhost:32288/embedded/<uuid>"
echo "  python -m superset_deploy enable-embed"
