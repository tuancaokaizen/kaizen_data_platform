#!/usr/bin/env bash
# Stabilize KD realtime data consistency across PG / ES / fnd / CH hot+cold.
# Đảm bảo dữ liệu KD realtime khớp giữa PG / ES / fnd / CH hot+cold.
#
# Steps:
#   1) Reconcile ES order-bundles from PG for mismatch days (deterministic _id)
#   2) Purge orphan fnd.order rows for those days, re-ingest PG → Iceberg/CH fnd
#   3) Full BackfillRealtimeBusinessColdJob (overwrite cold from fnd) + seedHot + refresh views
#
# Usage:
#   ./dags/scripts/stabilize_realtime_business_consistency.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export KUBECONFIG="${K3S_CONF:-$HOME/.kube/config.k3s-new}"
NS_AF="${AIRFLOW_NAMESPACE:-orchestrator}"
SCHED="${AIRFLOW_SCHEDULER_POD:-airflow-lab-scheduler-0}"
SPARK_NS=compute
SPARK_POD=$(kubectl get pods -n "$SPARK_NS" -l app=kaizen-spark-master -o jsonpath='{.items[0].metadata.name}')

FROM_DATE="${FROM_DATE:-2026-07-10}"
TO_DATE="${TO_DATE:-2026-08-08}"
# Days with known PG↔ES mismatch near outage / Các ngày lệch PG↔ES quanh outage
ES_DAYS_FROM="${ES_DAYS_FROM:-2026-07-10}"
ES_DAYS_TO="${ES_DAYS_TO:-2026-07-12}"
ES_AUG_FROM="${ES_AUG_FROM:-2026-08-07}"
ES_AUG_TO="${ES_AUG_TO:-2026-08-08}"

echo "=== 0) Cluster targets ==="
echo "scheduler=$SCHED spark=$SPARK_NS/$SPARK_POD"
echo "ingest=[$FROM_DATE,$TO_DATE) es_reconcile=[$ES_DAYS_FROM,$ES_DAYS_TO)+[$ES_AUG_FROM,$ES_AUG_TO)"

echo "=== 1/4 Reconcile ES order-bundles from PG ==="
kubectl cp "$ROOT/dags/scripts/reconcile_pg_es_order_bundles.py" \
  "$NS_AF/$SCHED:/tmp/reconcile_pg_es_order_bundles.py" -c scheduler
kubectl exec -n "$NS_AF" "$SCHED" -c scheduler -- \
  python3 /tmp/reconcile_pg_es_order_bundles.py \
    --config /opt/airflow/dags/config.ini \
    --from-date "$ES_DAYS_FROM" --to-date "$ES_DAYS_TO"
kubectl exec -n "$NS_AF" "$SCHED" -c scheduler -- \
  python3 /tmp/reconcile_pg_es_order_bundles.py \
    --config /opt/airflow/dags/config.ini \
    --from-date "$ES_AUG_FROM" --to-date "$ES_AUG_TO"

echo "=== 2/4 Purge orphan fnd.order for Jul10 (+ re-ingest range via Spark) ==="
# Delete Iceberg fnd.order rows in windows that diverge from PG, then merge-ingest.
# Xóa dòng fnd.order lệch PG rồi ingest lại (merge).
kubectl exec -n "$SPARK_NS" "$SPARK_POD" -- bash -lc '
set -e
/opt/spark/bin/spark-sql --master local[2] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://nessie-catalog-svc.compute.svc.cluster.local:19120/api/v1 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.warehouse=s3a://iceberg-data \
  --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.hadoop.HadoopFileIO \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio.storage.svc.cluster.local:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=kaizen123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  -e "
SELECT '\''before_jul10'\'', count(*) FROM nessie.fnd.order
 WHERE TransactionDate >= timestamp '\''2026-07-10 00:00:00+07'\''
   AND TransactionDate <  timestamp '\''2026-07-11 00:00:00+07'\'';
DELETE FROM nessie.fnd.order
 WHERE TransactionDate >= timestamp '\''2026-07-10 00:00:00+07'\''
   AND TransactionDate <  timestamp '\''2026-07-11 00:00:00+07'\'';
DELETE FROM nessie.fnd.order
 WHERE TransactionDate >= timestamp '\''2026-08-07 00:00:00+07'\''
   AND TransactionDate <  timestamp '\''2026-08-08 00:00:00+07'\'';
SELECT '\''after_jul10'\'', count(*) FROM nessie.fnd.order
 WHERE TransactionDate >= timestamp '\''2026-07-10 00:00:00+07'\''
   AND TransactionDate <  timestamp '\''2026-07-11 00:00:00+07'\'';
"
'

echo "Trigger transaction ingest DAG for [$FROM_DATE, $TO_DATE)..."
kubectl exec -n "$NS_AF" "$SCHED" -c scheduler -- \
  airflow dags trigger spark_transaction_data_ingest_pipeline \
    --conf "{\"fromDate\":\"$FROM_DATE\",\"toDate\":\"$TO_DATE\",\"useWatermark\":false}"

echo "Waiting for ingest DAG to finish (poll 30s)..."
for i in $(seq 1 80); do
  STATE=$(kubectl exec -n "$NS_AF" "$SCHED" -c scheduler -- \
    airflow dags list-runs -d spark_transaction_data_ingest_pipeline -o plain 2>/dev/null \
    | awk 'NR==2 {print $3; exit}')
  echo "  ingest state=$STATE (poll $i)"
  case "$STATE" in
    success) break ;;
    failed|upstream_failed) echo "Ingest DAG failed"; exit 1 ;;
  esac
  sleep 30
done

echo "=== 3/4 BackfillRealtimeBusinessCold (full history → cold, seedHot=true) ==="
kubectl exec -n "$SPARK_NS" "$SPARK_POD" -- bash -lc '
set -e
curl -s -f -o /tmp/kaizen-batch.jar \
  http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch.jar
/opt/spark/bin/spark-submit \
  --master spark://kaizen-spark-master.compute.svc.cluster.local:7077 \
  --class com.kaizen.batch.publish.clickhouse.BackfillRealtimeBusinessCold \
  --conf spark.driver.host=$(hostname -i) \
  --conf spark.executor.memory=2g --conf spark.executor.cores=1 \
  --conf spark.cores.max=2 --conf spark.driver.memory=2g \
  --conf spark.driver.extraJavaOptions=-Duser.timezone=UTC \
  --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
  /tmp/kaizen-batch.jar \
  PROD 2026-01-01 2026-08-08 14 true
'

echo "=== 4/4 Refresh CH serving pointer + views ==="
kubectl exec -n "$NS_AF" "$SCHED" -c scheduler -- \
  airflow tasks test spark_realtime_business_backfill_pipeline refresh_realtime_business_serving 2026-08-07T00:00:00+00:00
kubectl exec -n "$NS_AF" "$SCHED" -c scheduler -- \
  airflow tasks test spark_realtime_business_backfill_pipeline refresh_realtime_business_views 2026-08-07T00:00:00+00:00

echo "DONE stabilize_realtime_business_consistency"
