#!/usr/bin/env python3
"""
Streaming consumer for box office / revenue data.

Consumes revenue events from Kafka and writes to PostgreSQL box_office_daily table.
"""

import json
import logging
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.config import settings
from shared.db import get_db_connection, get_cursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Try to import kafka - provide helpful error if missing
try:
    from confluent_kafka import Consumer, KafkaError, KafkaException
except ImportError:
    logger.error("confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)


class RevenueConsumer:
    """
    Consumes revenue events from Kafka and writes to box_office_daily table.

    Supports:
    - Batch processing for efficiency
    - Upsert semantics (ON CONFLICT UPDATE)
    - Graceful shutdown
    """

    def __init__(
        self,
        topic: str = "revenue-events",
        group_id: str = "hyperion-revenue-consumer",
        batch_size: int = 500,
    ):
        self.topic = topic
        self.batch_size = batch_size
        self.running = False

        self.consumer = Consumer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

    def process_batch(self, messages: list[dict]):
        """Process a batch of revenue events."""
        if not messages:
            return

        with get_db_connection() as conn:
            records = []
            for msg in messages:
                try:
                    record_date = datetime.fromisoformat(msg["report_date"]).date()
                    records.append({
                        "film_id": msg["film_id"],
                        "date": record_date,
                        "state_code": msg["state_code"],
                        "revenue": Decimal(str(msg["revenue"])),
                        "week_num": 1,  # Could calculate from release date
                    })
                except (KeyError, ValueError) as e:
                    logger.warning(f"Invalid message format: {e}")
                    continue

            if records:
                with get_cursor(conn) as cursor:
                    from psycopg2.extras import execute_values

                    execute_values(
                        cursor,
                        """
                        INSERT INTO box_office_daily (film_id, date, state_code, revenue, week_num)
                        VALUES %s
                        ON CONFLICT (film_id, date, state_code) DO UPDATE SET
                            revenue = EXCLUDED.revenue,
                            week_num = EXCLUDED.week_num
                        """,
                        [
                            (r["film_id"], r["date"], r["state_code"], r["revenue"], r["week_num"])
                            for r in records
                        ],
                        page_size=500,
                    )
                    conn.commit()

                logger.info(f"Inserted/updated {len(records)} revenue records")

    def consume(self, max_messages: int = None):
        """
        Consume and process revenue events.

        Args:
            max_messages: Stop after processing this many messages (None = run forever)
        """
        self.consumer.subscribe([self.topic])
        self.running = True

        logger.info(f"Starting revenue consumer on topic '{self.topic}'")
        logger.info("Press Ctrl+C to stop")

        total_processed = 0
        batch = []

        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    # No message, process any pending batch
                    if batch:
                        self.process_batch(batch)
                        self.consumer.commit()
                        total_processed += len(batch)
                        batch = []
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
                    batch.append(value)
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON in message: {e}")
                    continue

                # Process batch when full
                if len(batch) >= self.batch_size:
                    self.process_batch(batch)
                    self.consumer.commit()
                    total_processed += len(batch)
                    batch = []

                    if max_messages and total_processed >= max_messages:
                        logger.info(f"Reached max_messages limit ({max_messages})")
                        break

        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")

        finally:
            # Process any remaining messages
            if batch:
                self.process_batch(batch)
                self.consumer.commit()
                total_processed += len(batch)

            self.consumer.close()
            logger.info(f"Consumer stopped. Total processed: {total_processed}")

        return total_processed

    def stop(self):
        """Signal the consumer to stop."""
        self.running = False


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Consume revenue events from Kafka and write to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run continuously
  python revenue_consumer.py

  # Process limited number of messages
  python revenue_consumer.py --max-messages 1000

  # Custom batch size
  python revenue_consumer.py --batch-size 1000
        """
    )
    parser.add_argument("--topic", default="revenue-events", help="Kafka topic name")
    parser.add_argument("--group-id", default="hyperion-revenue-consumer", help="Consumer group ID")
    parser.add_argument("--batch-size", type=int, default=500, help="Batch size for DB writes")
    parser.add_argument("--max-messages", type=int, help="Stop after processing N messages")

    args = parser.parse_args()

    consumer = RevenueConsumer(
        topic=args.topic,
        group_id=args.group_id,
        batch_size=args.batch_size,
    )
    consumer.consume(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
