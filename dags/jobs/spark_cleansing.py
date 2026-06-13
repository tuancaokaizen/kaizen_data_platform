def get_pod_operator_config():
    return {
        "task_id": "spark_cleansing_job",
        "name": "spark-cleansing-job",
        "namespace": "spark-jobs",
        "image": "apache/spark:3.4.1",
        "cmds": ["/opt/spark/bin/spark-submit"],
        "arguments": [
            "--class", "com.kaizen.spark.processing.DataCleansing",
            "s3a://data-platform/jars/kaizen-data-platform-1.0.0-batch.jar"
        ]
    }
