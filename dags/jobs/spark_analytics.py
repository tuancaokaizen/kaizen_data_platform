def get_pod_operator_config():
    return {
        "task_id": "spark_analytics_job",
        "name": "spark-analytics-job",
        "namespace": "spark-jobs",
        "image": "apache/spark:3.4.1",
        "cmds": ["/opt/spark/bin/spark-submit"],
        "arguments": [
            "--class", "com.kaizen.spark.processing.BatchAggregator",
            "s3a://data-platform/jars/kaizen-data-platform-1.0.0-batch.jar"
        ]
    }
