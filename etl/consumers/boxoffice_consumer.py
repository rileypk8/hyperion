"""Consumer for box office records - loads into PostgreSQL."""

import logging
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONConsumer
from shared.db import get_db_connection, get_cursor
from shared.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BoxOfficeConsumer:
    """
    Consumes box office records from Kafka and loads into PostgreSQL.

    Handles:
    - box_office_daily table (film_id, date, state_code, revenue, week_num)

    Requires films to be loaded first to resolve film_id.
    """

    def __init__(self):
        # Cache for film_id lookups
        self.film_cache = {}  # (title, year) -> film_id

    def get_film_id(self, conn, title: str, year: int) -> int | None:
        """Get film ID by title and year."""
        cache_key = (title.lower(), year)
        if cache_key in self.film_cache:
            return self.film_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT f.id
                FROM films f
                JOIN media m ON f.media_id = m.id
                WHERE m.title = %s AND m.year = %s
                """,
                (title, year)
            )
            result = cursor.fetchone()

            if result:
                self.film_cache[cache_key] = result[0]
                return result[0]

            return None

    def process_batch(self, batch: list[dict]):
        """Process a batch of box office records."""
        with get_db_connection() as conn:
            records_to_insert = []
            skipped = 0

            for msg in batch:
                record = msg["value"]

                try:
                    film_id = self.get_film_id(
                        conn,
                        record["film_title"],
                        record["film_year"]
                    )

                    if not film_id:
                        skipped += 1
                        logger.debug(
                            f"Skipping box office record - film not found: "
                            f"{record['film_title']} ({record['film_year']})"
                        )
                        continue

                    # Parse date
                    record_date = datetime.fromisoformat(record["date"]).date()

                    records_to_insert.append({
                        "film_id": film_id,
                        "date": record_date,
                        "state_code": record["state_code"],
                        "revenue": Decimal(str(record["revenue"])),
                        "week_num": record["week_num"],
                    })

                except Exception as e:
                    logger.error(f"Error preparing box office record: {e}")
                    continue

            # Batch insert
            if records_to_insert:
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
                            for r in records_to_insert
                        ],
                        page_size=500,
                    )
                    conn.commit()

                logger.info(
                    f"Inserted {len(records_to_insert)} box office records "
                    f"(skipped {skipped} for missing films)"
                )

    def consume(self, max_messages: int = None):
        """Consume and process box office records."""
        logger.info("Starting box office consumer...")

        consumer = JSONConsumer(
            group_id="boxoffice",
            topics=["raw-boxoffice"],
            process_batch=self.process_batch,
            batch_size=1000,  # Larger batches for high-volume data
        )

        return consumer.consume(max_messages=max_messages)


def main():
    """CLI entry point for the box office consumer."""
    import argparse

    parser = argparse.ArgumentParser(description="Consume box office records from Kafka")
    parser.add_argument("--max-messages", type=int, help="Maximum messages to consume")
    args = parser.parse_args()

    consumer = BoxOfficeConsumer()
    consumer.consume(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
