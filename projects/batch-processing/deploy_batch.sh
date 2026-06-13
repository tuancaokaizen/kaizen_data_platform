#!/bin/bash
set -e

# Helper function to read config.ini
get_config_val() {
    local section=$1
    local key=$2
    awk -F " *= *" -v sec="[$section]" -v k="$key" '
        $0 == sec { in_sec = 1; next }
        /^\[/ { in_sec = 0 }
        in_sec && $1 == k { print $2; exit }
    ' config.ini
}

# Load MinIO config
MINIO_ENDPOINT=$(get_config_val "minio" "endpoint")
MINIO_ACCESS_KEY=$(get_config_val "minio" "access_key")
MINIO_SECRET_KEY=$(get_config_val "minio" "secret_key")
MINIO_BUCKET=$(get_config_val "minio" "bucket_name")

echo "=== Building Spark Batch Jar ==="
gradle buildBatchJar

echo "=== Uploading Spark Batch Jar to MinIO ==="
if command -v mc &> /dev/null; then
    echo "Using mc (MinIO Client) for upload..."
    mc alias set local-minio "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" --api s3v4
    mc mb local-minio/"$MINIO_BUCKET" 2>/dev/null || true
    mc cp build/libs/kaizen-batch-processing-1.0.0-batch.jar local-minio/"$MINIO_BUCKET"/jars/
elif command -v aws &> /dev/null; then
    echo "Using aws CLI for upload..."
    AWS_ACCESS_KEY_ID="$MINIO_ACCESS_KEY" AWS_SECRET_ACCESS_KEY="$MINIO_SECRET_KEY" \
    aws s3 cp build/libs/kaizen-batch-processing-1.0.0-batch.jar "s3://$MINIO_BUCKET/jars/kaizen-data-platform-1.0.0-batch.jar" \
    --endpoint-url "$MINIO_ENDPOINT" --no-verify-ssl
else
    echo "❌ Error: Neither 'mc' (MinIO Client) nor 'aws' CLI is installed."
    echo "Please install mc (e.g. 'brew install minio-mc') or aws-cli to deploy."
    exit 1
fi

echo "Deploy Batch Spark completed successfully!"
