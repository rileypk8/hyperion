"""Configuration for Spark jobs."""

# HDFS paths
HDFS_RAW_PATH = "hdfs://namenode:9000/raw"
HDFS_PROCESSED_PATH = "hdfs://namenode:9000/processed"
HDFS_EXPORTS_PATH = "hdfs://namenode:9000/analytics/exports"

# Local paths (for testing without HDFS)
LOCAL_RAW_PATH = "/opt/data/raw"
LOCAL_PROCESSED_PATH = "/opt/data/processed"
LOCAL_EXPORTS_PATH = "/opt/data/exports"

# Spark configuration
SPARK_APP_NAME = "hyperion-etl"
SPARK_MASTER = "spark://spark-master:7077"

# For local development
SPARK_LOCAL_MASTER = "local[*]"
