#!/bin/bash
# Populate slice.query_context for embedded guest filters / Lưu query_context cho filter guest
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

POD=$(kubectl get pods -n "$NS" -l app=apache-superset -o jsonpath='{.items[0].metadata.name}')
echo "=== Save chart query_context on pod $POD ==="

kubectl exec -i -n "$NS" "$POD" -- python3 - <<'PY'
import json

from flask import g
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import security_manager
    from superset.charts.schemas import ChartDataQueryContextSchema
    from superset.commands.chart.data.get_data_command import ChartDataCommand
    from superset.extensions import db
    from superset.models.slice import Slice

    admin = security_manager.find_user(username="admin")
    if admin is None:
        raise SystemExit("admin user not found")
    g.user = admin

    schema = ChartDataQueryContextSchema()
    saved = 0
    failed = 0

    for chart in db.session.query(Slice).order_by(Slice.id).all():
        if chart.query_context:
            continue
        try:
            form_data = chart.form_data
            datasource = chart.datasource
            if datasource is None:
                failed += 1
                print(f"  skip {chart.id}: no datasource")
                continue

            payload = {
                "datasource": {"id": datasource.id, "type": chart.datasource_type or "table"},
                "force": True,
                "queries": [{"columns": [], "metrics": [], "row_limit": 1000}],
                "form_data": form_data,
                "result_format": "json",
                "result_type": "full",
            }
            query_context = schema.load(payload)
            command = ChartDataCommand(query_context)
            command.validate()
            command.run()

            chart.query_context = json.dumps(query_context.to_dict(), default=str)
            saved += 1
            print(f"  ✔ chart {chart.id}: {chart.slice_name[:60]}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"  ❌ chart {chart.id}: {exc}")

    db.session.commit()
    print(f"OK saved={saved} failed={failed}")
PY

echo "✔ Chart query_context saved"
