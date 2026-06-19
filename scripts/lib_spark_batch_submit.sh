#!/bin/bash
# lib_spark_batch_submit.sh
# Run a batch-processing Spark job on the Spark master pod via spark-submit + fat JAR.
# Chạy Spark job batch trên master pod bằng spark-submit và fat JAR từ MinIO.
#
# Usage: source scripts/lib_spark_batch_submit.sh
#        run_spark_batch_job <namespace> <pod> <main_class>

run_spark_batch_job() {
    local namespace="$1"
    local pod="$2"
    local main_class="$3"

    if [ -z "$namespace" ] || [ -z "$pod" ] || [ -z "$main_class" ]; then
        echo "Usage: run_spark_batch_job <namespace> <pod> <main_class>"
        return 1
    fi

    kubectl exec -n "$namespace" "$pod" -- /bin/sh -c "
set -e
JAR=/tmp/kaizen-batch-processing.jar
echo 'Downloading latest batch JAR from MinIO...'
curl -sSf -o \"\$JAR\" http://minio.storage.svc.cluster.local:9000/spark/jobs/kaizen-batch-processing.jar
/opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --class '$main_class' \
  --conf 'spark.hadoop.fs.s3a.endpoint=http://minio.storage.svc.cluster.local:9000' \
  --conf 'spark.hadoop.fs.s3a.access.key=admin' \
  --conf 'spark.hadoop.fs.s3a.secret.key=kaizen123' \
  --conf 'spark.hadoop.fs.s3a.path.style.access=true' \
  --conf 'spark.hadoop.fs.s3a.connection.ssl.enabled=false' \
  --conf 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem' \
  --conf 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' \
  --conf 'spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog' \
  --conf 'spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog' \
  --conf 'spark.sql.catalog.nessie.uri=http://nessie-catalog-svc.compute.svc.cluster.local:19120/api/v1' \
  --conf 'spark.sql.catalog.nessie.io-impl=org.apache.iceberg.hadoop.HadoopFileIO' \
  --conf 'spark.sql.catalog.nessie.warehouse=s3a://iceberg-data' \
  --conf 'spark.sql.catalog.nessie.gc-enabled=true' \
  --conf 'spark.sql.session.timeZone=Asia/Ho_Chi_Minh' \
  --conf 'spark.driver.extraJavaOptions=-Duser.timezone=Asia/Ho_Chi_Minh' \
  \"\$JAR\"
"
}
