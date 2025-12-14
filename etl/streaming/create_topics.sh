#!/bin/bash
# Create Kafka topics with appropriate partition counts

KAFKA_CONTAINER="hyperion-kafka"
BOOTSTRAP_SERVER="localhost:9092"

echo "Creating Kafka topics..."

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

# Dead letter queues
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

echo "Topics created. Listing all topics:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $BOOTSTRAP_SERVER
