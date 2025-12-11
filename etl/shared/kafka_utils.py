"""Kafka producer and consumer utilities for Hyperion ETL."""

import json
import logging
from typing import Callable, Any
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import (
    StringSerializer,
    StringDeserializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer

from .config import settings

logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def create_producer(use_avro: bool = False, schema_str: str = None) -> Producer:
    """
    Create a Kafka producer.

    Args:
        use_avro: Whether to use Avro serialization
        schema_str: Avro schema string (required if use_avro=True)

    Returns:
        Configured Kafka Producer
    """
    config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": "hyperion-producer",
        "acks": "all",
        "retries": 3,
        "retry.backoff.ms": 1000,
        "linger.ms": 5,
        "batch.size": 16384,
    }

    return Producer(config)


def create_consumer(
    group_id: str,
    topics: list[str],
    auto_offset_reset: str = "earliest",
) -> Consumer:
    """
    Create a Kafka consumer.

    Args:
        group_id: Consumer group ID
        topics: List of topics to subscribe to
        auto_offset_reset: Where to start reading if no offset exists

    Returns:
        Configured Kafka Consumer
    """
    config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": f"{settings.consumer_group_prefix}-{group_id}",
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 45000,
    }

    consumer = Consumer(config)
    consumer.subscribe(topics)
    return consumer


class JSONProducer:
    """Producer that serializes messages as JSON."""

    def __init__(self, topic: str):
        self.topic = topic
        self.producer = create_producer()
        self.serializer = StringSerializer("utf-8")

    def produce(self, key: str, value: dict):
        """Produce a message to the topic."""
        self.producer.produce(
            topic=self.topic,
            key=self.serializer(key),
            value=json.dumps(value).encode("utf-8"),
            callback=delivery_report,
        )

    def flush(self, timeout: float = 30.0):
        """Flush all buffered messages."""
        self.producer.flush(timeout)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()


class JSONConsumer:
    """Consumer that deserializes JSON messages and processes them in batches."""

    def __init__(
        self,
        group_id: str,
        topics: list[str],
        process_batch: Callable[[list[dict]], None],
        batch_size: int = 100,
        poll_timeout: float = 1.0,
    ):
        self.consumer = create_consumer(group_id, topics)
        self.process_batch = process_batch
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout
        self.running = True
        self.deserializer = StringDeserializer("utf-8")

    def consume(self, max_messages: int = None):
        """
        Consume messages and process in batches.

        Args:
            max_messages: Maximum number of messages to consume (None = unlimited)
        """
        batch = []
        messages_processed = 0

        try:
            while self.running:
                if max_messages and messages_processed >= max_messages:
                    break

                msg = self.consumer.poll(self.poll_timeout)

                if msg is None:
                    # No message, process any pending batch
                    if batch:
                        self._process_and_commit(batch)
                        messages_processed += len(batch)
                        batch = []
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition {msg.partition()}")
                        # Process remaining batch when we hit EOF
                        if batch:
                            self._process_and_commit(batch)
                            messages_processed += len(batch)
                            batch = []
                    else:
                        raise KafkaException(msg.error())
                    continue

                # Deserialize message
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    key = msg.key().decode("utf-8") if msg.key() else None
                    batch.append({"key": key, "value": value, "offset": msg.offset()})
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                    continue

                # Process batch if full
                if len(batch) >= self.batch_size:
                    self._process_and_commit(batch)
                    messages_processed += len(batch)
                    batch = []

            # Process any remaining messages
            if batch:
                self._process_and_commit(batch)
                messages_processed += len(batch)

        finally:
            self.consumer.close()

        logger.info(f"Consumed {messages_processed} messages total")
        return messages_processed

    def _process_and_commit(self, batch: list[dict]):
        """Process a batch and commit offsets."""
        try:
            self.process_batch(batch)
            self.consumer.commit()
            logger.info(f"Processed and committed batch of {len(batch)} messages")
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            raise

    def stop(self):
        """Signal the consumer to stop."""
        self.running = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
