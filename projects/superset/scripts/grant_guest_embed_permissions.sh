#!/bin/bash
# Grant Gamma/Public role permissions for embedded dashboards (one-shot)
# Cấp quyền role guest cho nhúng dashboard (chạy một lần sau deploy)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS="${SUPERSET_NAMESPACE:-apps}"

if [ -n "${K3S_CONF:-}" ]; then
  export KUBECONFIG="$K3S_CONF"
elif [ -f "$ROOT/../../dags/config.k3s-new" ]; then
  export KUBECONFIG="$ROOT/../../dags/config.k3s-new"
elif [ -f "$HOME/.kube/config.k3s-new" ]; then
  export KUBECONFIG="$HOME/.kube/config.k3s-new"
fi

POD=$(kubectl get pods -n "$NS" -l app=apache-superset --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
if [ -z "$POD" ]; then
  echo "❌ No running Superset pod in $NS"
  exit 1
fi
echo "=== Grant guest embed permissions on pod $POD ==="

kubectl exec -i -n "$NS" "$POD" -- python3 - <<'PY'
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import security_manager
    from superset.connectors.sqla.models import SqlaTable
    from superset.extensions import db
    from superset.models.core import Database

    role_name = app.config.get("GUEST_ROLE_NAME", "Gamma")
    role = security_manager.find_role(role_name)
    if role is None:
        raise SystemExit(f"role not found: {role_name}")

    # Sync base role templates first, then add guest grants / Sync role trước, grant sau
    security_manager.sync_role_definitions()

    for perm_name in (
        "all_database_access",
        "all_datasource_access",
        "all_query_access",
        "all_dashboard_access",
    ):
        pvm = security_manager.find_permission_view_menu(perm_name, perm_name)
        if pvm:
            security_manager.add_permission_role(role, pvm)

    for database in db.session.query(Database).all():
        pvm = security_manager.find_permission_view_menu("database_access", database.perm)
        if pvm:
            security_manager.add_permission_role(role, pvm)

    for datasource in db.session.query(SqlaTable).all():
        pvm = security_manager.find_permission_view_menu("datasource_access", datasource.perm)
        if pvm:
            security_manager.add_permission_role(role, pvm)

    security_manager.get_session.commit()
    ds_count = len([p for p in role.permissions if p.permission.name == "datasource_access"])
    print(
        f"OK role={role_name} databases={db.session.query(Database).count()} "
        f"datasets={db.session.query(SqlaTable).count()} datasource_access={ds_count}"
    )
PY

echo "✔ Guest permissions granted"
