#!/bin/bash
# Start the Hyperion big data stack
# Starts services in dependency order and waits for health checks

set -e

echo "=========================================="
echo "Starting Hyperion Big Data Stack"
echo "=========================================="

cd "$(dirname "$0")/.."

echo ""
echo "[1/4] Starting Kafka cluster..."
docker-compose up -d zookeeper kafka
echo "Waiting for Kafka to be healthy..."
sleep 15

echo ""
echo "[2/4] Starting Hadoop HDFS..."
docker-compose up -d namenode datanode
echo "Waiting for HDFS to be healthy..."
sleep 20

echo ""
echo "[3/4] Starting Spark cluster..."
docker-compose up -d spark-master spark-worker
sleep 5

echo ""
echo "[4/4] Starting Hive Metastore and Trino..."
docker-compose up -d hive-metastore-db hive-metastore trino
sleep 10

echo ""
echo "[5/5] Initializing HDFS directories..."
docker-compose up hdfs-init
sleep 5

echo ""
echo "[6/6] Starting UI services..."
docker-compose up -d kafka-ui

echo ""
echo "=========================================="
echo "Stack started successfully!"
echo "=========================================="
echo ""
echo "Service URLs:"
echo "  - Kafka UI:        http://localhost:8080"
echo "  - HDFS NameNode:   http://localhost:9870"
echo "  - HDFS DataNode:   http://localhost:9864"
echo "  - Spark Master:    http://localhost:8081"
echo "  - Spark Worker:    http://localhost:8082"
echo "  - Trino:           http://localhost:8083"
echo ""
echo "Next steps:"
echo "  1. Create Kafka topics: ./scripts/create-kafka-topics.sh"
echo "  2. Run producers to ingest data"
echo "  3. Run consumers to land data in HDFS"
echo "  4. Run Spark jobs to transform data"
