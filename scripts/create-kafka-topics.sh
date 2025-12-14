#!/bin/bash
# Create Kafka topics for Hyperion data pipeline
# Run this after Kafka is healthy

set -e

KAFKA_CONTAINER="hyperion-kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "Creating Kafka topics..."

# Raw data topics (from producers)
docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-films \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-characters \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-boxoffice \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-soundtracks \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-games \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo ""
echo "Topics created successfully!"
echo ""
echo "Available topics:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER
