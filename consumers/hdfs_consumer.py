#!/usr/bin/env python3
"""
Kafka consumer that writes messages to HDFS.

Consumes from Kafka topics and lands data as newline-delimited JSON
files in HDFS for downstream Spark processing.
"""

import json
import logging
import signal
import sys
from datetime import datetime
from typing import Callable

from config import settings
from hdfs_client import HDFSClient, get_hdfs_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
except ImportError:
    logger.error("confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)


class HDFSConsumer:
    """
    Consumes Kafka messages and writes batches to HDFS.

    Features:
    - Configurable batch size and timeout
    - Automatic directory creation
    - Graceful shutdown handling
    - Manual offset commits after successful HDFS writes
    """

    def __init__(
        self,
        topics: list[str],
        hdfs_base_path: str,
        batch_size: int = None,
        batch_timeout_ms: int = None,
        group_id: str = None,
    ):
        self.topics = topics if isinstance(topics, list) else [topics]
        self.hdfs_base_path = hdfs_base_path
        self.batch_size = batch_size or settings.batch_size
        self.batch_timeout_ms = batch_timeout_ms or settings.batch_timeout_ms

        # Override group_id if provided
        kafka_config = settings.kafka_config.copy()
        if group_id:
            kafka_config["group.id"] = group_id

        self.consumer = Consumer(kafka_config)
        self.hdfs = get_hdfs_client()
        self.running = False
        self.batch = []
        self.batch_start_time = None

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def _flush_batch(self) -> bool:
        """Write current batch to HDFS and commit offsets."""
        if not self.batch:
            return True

        # Partition by date for better organization
        date_partition = datetime.utcnow().strftime("%Y-%m-%d")
        path = self.hdfs.write_json_batch(
            self.hdfs_base_path,
            self.batch,
            partition_key=date_partition,
        )

        if path:
            logger.info(f"Flushed {len(self.batch)} messages to {path}")
            self.consumer.commit()
            self.batch = []
            self.batch_start_time = None
            return True
        else:
            logger.error("Failed to write batch to HDFS")
            return False

    def _should_flush(self) -> bool:
        """Check if batch should be flushed based on size or time."""
        if len(self.batch) >= self.batch_size:
            return True

        if self.batch_start_time:
            elapsed = (datetime.utcnow() - self.batch_start_time).total_seconds() * 1000
            if elapsed >= self.batch_timeout_ms:
                return True

        return False

    def consume(
        self,
        transform: Callable[[dict], dict] = None,
        max_messages: int = None,
    ) -> int:
        """
        Start consuming messages and writing to HDFS.

        Args:
            transform: Optional function to transform messages before writing
            max_messages: Optional limit on total messages to process

        Returns:
            Total number of messages processed
        """
        self.consumer.subscribe(self.topics)
        self.running = True
        total_processed = 0

        logger.info(f"Starting consumer for topics: {self.topics}")
        logger.info(f"Writing to HDFS path: {self.hdfs_base_path}")
        logger.info(f"Batch size: {self.batch_size}, timeout: {self.batch_timeout_ms}ms")

        # Ensure base directory exists
        self.hdfs.mkdir(self.hdfs_base_path)

        try:
            while self.running:
                msg = self.consumer.poll(timeout=settings.poll_timeout_seconds)

                if msg is None:
                    # Check if we should flush on timeout
                    if self._should_flush():
                        self._flush_batch()
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        raise KafkaException(msg.error())
                    continue

                # Parse message
                try:
                    value = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse message: {e}")
                    continue

                # Apply transform if provided
                if transform:
                    value = transform(value)

                # Add metadata
                value["_kafka_topic"] = msg.topic()
                value["_kafka_partition"] = msg.partition()
                value["_kafka_offset"] = msg.offset()
                value["_consumed_at"] = datetime.utcnow().isoformat()

                # Add to batch
                self.batch.append(value)
                if self.batch_start_time is None:
                    self.batch_start_time = datetime.utcnow()

                total_processed += 1

                # Check flush conditions
                if self._should_flush():
                    self._flush_batch()

                # Check message limit
                if max_messages and total_processed >= max_messages:
                    logger.info(f"Reached message limit: {max_messages}")
                    break

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            # Flush remaining messages
            if self.batch:
                logger.info(f"Flushing final batch of {len(self.batch)} messages")
                self._flush_batch()

            self.consumer.close()
            logger.info(f"Consumer closed. Total processed: {total_processed}")

        return total_processed


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Consume Kafka topics and write to HDFS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Consume films topic
  python hdfs_consumer.py --topic raw-films --hdfs-path /raw/films

  # Consume multiple topics
  python hdfs_consumer.py --topic raw-films raw-characters --hdfs-path /raw/all

  # With custom batch settings
  python hdfs_consumer.py --topic raw-boxoffice --hdfs-path /raw/boxoffice \\
      --batch-size 500 --batch-timeout 10000

  # Process limited messages (for testing)
  python hdfs_consumer.py --topic raw-films --hdfs-path /raw/films --max-messages 100
        """
    )
    parser.add_argument(
        "--topic", "-t",
        nargs="+",
        required=True,
        help="Kafka topic(s) to consume"
    )
    parser.add_argument(
        "--hdfs-path", "-p",
        required=True,
        help="HDFS base path for output"
    )
    parser.add_argument(
        "--group-id", "-g",
        help="Consumer group ID (default: hyperion-hdfs-consumer)"
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=100,
        help="Messages per batch"
    )
    parser.add_argument(
        "--batch-timeout",
        type=int,
        default=5000,
        help="Batch timeout in milliseconds"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        help="Maximum messages to process (for testing)"
    )

    args = parser.parse_args()

    consumer = HDFSConsumer(
        topics=args.topic,
        hdfs_base_path=args.hdfs_path,
        batch_size=args.batch_size,
        batch_timeout_ms=args.batch_timeout,
        group_id=args.group_id,
    )

    consumer.consume(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
