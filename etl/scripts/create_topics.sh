#!/bin/bash
# Create Kafka topics for Hyperion ETL pipeline

set -e

KAFKA_CONTAINER="hyperion-kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "Creating Kafka topics..."

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until docker exec $KAFKA_CONTAINER kafka-broker-api-versions --bootstrap-server $BOOTSTRAP_SERVER > /dev/null 2>&1; do
  echo "Kafka not ready yet, waiting..."
  sleep 2
done

echo "Kafka is ready. Creating topics..."

# Create topics
docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-films \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-characters \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-boxoffice \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-games \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic raw-soundtracks \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

# Create dead letter queue topics for failed messages
docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic dlq-films \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic dlq-characters \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic dlq-boxoffice \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic dlq-games \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic dlq-soundtracks \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "Listing all topics:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER

echo "Topics created successfully!"
