#!/usr/bin/env bash
# Reset biz_rt KD realtime report (hot + cold Iceberg) for redeployed Flink logic.
# Xóa dữ liệu báo cáo KD realtime hot/cold sau khi deploy logic CoProcess mới.
#
# Usage:
#   ./dags/scripts/clean_biz_rt_report.sh              # hot truncate + drop cold + reset kafka + refresh views
#   ./dags/scripts/clean_biz_rt_report.sh --no-kafka   # skip consumer offset reset
#   ./dags/scripts/clean_biz_rt_report.sh --rebackfill  # also trigger backfill cold (seedHot=false)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export KUBECONFIG="${K3S_CONF:-$ROOT/dags/config.k3s-new}"
SCHED="${AIRFLOW_SCHEDULER_POD:-airflow-lab-scheduler-0}"
NS="${AIRFLOW_NAMESPACE:-orchestrator}"
RESET_KAFKA=1
REBACKFILL=0

for arg in "$@"; do
  case "$arg" in
    --no-kafka) RESET_KAFKA=0 ;;
    --rebackfill) REBACKFILL=1 ;;
  esac
done

echo "=== 1/5 Truncate ClickHouse hot report.biz_rt_order ==="
kubectl exec -n "$NS" "$SCHED" -c scheduler -- python3 -c "
import requests
host='clickhouse-service.databases.svc.cluster.local'
base=f'http://{host}:8123/'
auth=('admin','kaizen123')
before=requests.post(base, params={'query':'SELECT count() FROM report.biz_rt_order FORMAT TabSeparated'}, auth=auth, timeout=30).text.strip()
r=requests.post(base, params={'query':'TRUNCATE TABLE IF EXISTS report.biz_rt_order'}, auth=auth, timeout=60)
after=requests.post(base, params={'query':'SELECT count() FROM report.biz_rt_order FORMAT TabSeparated'}, auth=auth, timeout=30).text.strip()
print(f'rows before: {before} -> after: {after}')
assert r.status_code < 400, r.text
"

echo "=== 2/5 Drop Iceberg cold stg/serving biz_rt_order ==="
SPARK_POD=$(kubectl get pods -n compute -l app=kaizen-spark-master -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [ -z "$SPARK_POD" ]; then
  SPARK_POD=$(kubectl get pods -A 2>/dev/null | grep spark-master | head -1 | awk '{print $2}')
  SPARK_NS=$(kubectl get pods -A 2>/dev/null | grep spark-master | head -1 | awk '{print $1}')
else
  SPARK_NS=compute
fi
kubectl exec -n "$SPARK_NS" "$SPARK_POD" -- bash -lc '
/opt/spark/bin/spark-sql --master local[1] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://nessie.nessie.svc.cluster.local:19120/api/v1 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.warehouse=s3a://iceberg-data/ \
  -e "DROP TABLE IF EXISTS nessie.stg.biz_rt_order PURGE; DROP TABLE IF EXISTS nessie.serving.biz_rt_order PURGE;" 2>/dev/null || \
/opt/spark/bin/spark-sql --master local[1] \
  -e "DROP TABLE IF EXISTS nessie.stg.biz_rt_order; DROP TABLE IF EXISTS nessie.serving.biz_rt_order;" 2>/dev/null || true
echo "Iceberg biz_rt tables dropped (best-effort)"
'

echo "=== 3/5 Refresh serving.biz_rt_order Iceberg pointer ==="
kubectl exec -n "$NS" "$SCHED" -c scheduler -- \
  airflow tasks test spark_biz_rt_backfill_pipeline refresh_biz_rt_serving 2026-07-05T02:10:05+00:00 2>&1 | tail -5

if [ "$RESET_KAFKA" -eq 1 ]; then
  echo "=== 4/5 Reset Flink Kafka consumer groups (to earliest, ~2d retention) ==="
  KAFKA_POD=$(kubectl get pods -A 2>/dev/null | grep -E 'kafka.*[0-9]|kaizen-kafka' | grep Running | head -1 | awk '{print $2}')
  KAFKA_NS=$(kubectl get pods -A 2>/dev/null | grep -E 'kafka.*[0-9]|kaizen-kafka' | grep Running | head -1 | awk '{print $1}')
  if [ -n "$KAFKA_POD" ]; then
    for G in kaizen-biz-rt-order-group kaizen-biz-rt-payment-group; do
      for T in order-bundles payment-bundles; do
        kubectl exec -n "$KAFKA_NS" "$KAFKA_POD" -- bash -lc \
          "kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group $G --topic $T --reset-offsets --to-earliest --execute" 2>/dev/null || true
      done
    done
    echo "Consumer groups reset (restart BizRealtimeReportJob to replay)"
  else
    echo "WARN: Kafka pod not found — restart BizRealtimeReportJob manually after clean"
  fi
else
  echo "=== 4/5 Skipped Kafka reset (--no-kafka) ==="
fi

echo "=== 5/5 Redeploy views + optional backfill ==="
kubectl exec -n "$NS" "$SCHED" -c scheduler -- \
  airflow tasks test spark_biz_rt_backfill_pipeline refresh_biz_rt_views 2026-07-05T02:10:05+00:00 2>&1 | tail -3

if [ "$REBACKFILL" -eq 1 ]; then
  echo "Running cold backfill (seedHot=false via spark-submit)..."
  kubectl exec -n "$SPARK_NS" "$SPARK_POD" -- bash -lc '
set -e
curl -s -f -o /tmp/kaizen-batch-processing.jar \
  http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar
/opt/spark/bin/spark-submit \
  --master spark://kaizen-spark-master.compute.svc.cluster.local:7077 \
  --class com.kaizen.spark.utility.BackfillBizRtColdJob \
  --conf spark.driver.host=$(hostname -i) \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio.storage.svc.cluster.local:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=kaizen123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://nessie-catalog-svc.compute.svc.cluster.local:19120/api/v1 \
  --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.hadoop.HadoopFileIO \
  --conf spark.sql.catalog.nessie.warehouse=s3a://iceberg-data \
  --conf spark.executor.memory=1g --conf spark.executor.cores=1 --conf spark.cores.max=2 --conf spark.driver.memory=1g \
  /tmp/kaizen-batch-processing.jar \
  --fromDate 2026-01-01 --hotDays 14 --seedHot false
' 2>&1 | tail -30
  kubectl exec -n "$NS" "$SCHED" -c scheduler -- \
    airflow tasks test spark_biz_rt_backfill_pipeline refresh_biz_rt_serving 2026-07-05T02:10:05+00:00 2>&1 | tail -3
  kubectl exec -n "$NS" "$SCHED" -c scheduler -- \
    airflow tasks test spark_biz_rt_backfill_pipeline refresh_biz_rt_views 2026-07-05T02:10:05+00:00 2>&1 | tail -3
fi

echo "Done. Restart Kaizen-BizRealtimeReportJob on Flink to ingest with new offsets."
