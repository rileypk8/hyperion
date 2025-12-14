#!/bin/bash
# Run all Spark transformation jobs in order

set -e

SPARK_MASTER="spark://spark-master:7077"
JOBS_DIR="/opt/spark-jobs"
SPARK_SUBMIT="/spark/bin/spark-submit"

echo "=========================================="
echo "Running Hyperion Spark Jobs"
echo "=========================================="

echo ""
echo "[1/4] Loading films..."
docker exec hyperion-spark-master $SPARK_SUBMIT \
  --master $SPARK_MASTER \
  $JOBS_DIR/01_load_films.py

echo ""
echo "[2/4] Loading characters..."
docker exec hyperion-spark-master $SPARK_SUBMIT \
  --master $SPARK_MASTER \
  $JOBS_DIR/02_load_characters.py

echo ""
echo "[3/4] Loading box office data..."
docker exec hyperion-spark-master $SPARK_SUBMIT \
  --master $SPARK_MASTER \
  $JOBS_DIR/03_load_boxoffice.py

echo ""
echo "[4/4] Exporting analytics..."
docker exec hyperion-spark-master $SPARK_SUBMIT \
  --master $SPARK_MASTER \
  $JOBS_DIR/04_export_analytics.py

echo ""
echo "=========================================="
echo "All jobs completed successfully!"
echo "=========================================="
echo ""
echo "Verify processed data:"
echo "  docker exec hyperion-namenode hdfs dfs -ls /processed"
echo ""
echo "Verify exports:"
echo "  docker exec hyperion-namenode hdfs dfs -ls /analytics/exports"
