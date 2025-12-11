"""Consumer for film records - loads into PostgreSQL."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONConsumer
from shared.db import get_db_connection, get_cursor, get_or_create_lookup_ids
from shared.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilmsConsumer:
    """
    Consumes film records from Kafka and loads into PostgreSQL.

    Handles:
    - studios table (with years_active and status)
    - franchises table
    - media table (parent records)
    - films table (child records linking to media and studio)
    """

    # Studio metadata for proper initialization
    STUDIO_METADATA = {
        "Pixar Animation Studios": {"start": 1986, "end": None, "status": "active"},
        "Walt Disney Animation Studios": {"start": 1937, "end": None, "status": "active"},
        "Blue Sky Studios": {"start": 1987, "end": 2021, "status": "closed"},
        "DisneyToon Studios": {"start": 1988, "end": 2018, "status": "closed"},
        "Marvel Animation": {"start": 2008, "end": None, "status": "active"},
        "20th Century Animation": {"start": 2020, "end": None, "status": "active"},
        "Fox Animation Studios": {"start": 1994, "end": 2000, "status": "closed"},
        "Searchlight Pictures": {"start": 1994, "end": None, "status": "active"},
    }

    def __init__(self):
        self.consumer = None
        # Caches to reduce DB lookups
        self.studio_cache = {}  # name -> id
        self.franchise_cache = {}  # name -> id
        self.media_cache = {}  # (title, year, type) -> id

    def ensure_studio(self, conn, studio_name: str) -> int:
        """Get or create a studio, returning its ID."""
        if studio_name in self.studio_cache:
            return self.studio_cache[studio_name]

        with get_cursor(conn) as cursor:
            # Try to get existing
            cursor.execute(
                "SELECT id FROM studios WHERE name = %s",
                (studio_name,)
            )
            result = cursor.fetchone()

            if result:
                self.studio_cache[studio_name] = result[0]
                return result[0]

            # Create new with metadata
            meta = self.STUDIO_METADATA.get(studio_name, {
                "start": 2000, "end": None, "status": "active"
            })

            cursor.execute(
                """
                INSERT INTO studios (name, years_active_start, years_active_end, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (studio_name, meta["start"], meta.get("end"), meta["status"])
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.studio_cache[studio_name] = result[0]
                return result[0]

            # If we get here, there was a conflict - fetch the existing ID
            cursor.execute("SELECT id FROM studios WHERE name = %s", (studio_name,))
            result = cursor.fetchone()
            self.studio_cache[studio_name] = result[0]
            return result[0]

    def ensure_franchise(self, conn, franchise_name: str) -> int:
        """Get or create a franchise, returning its ID."""
        if franchise_name in self.franchise_cache:
            return self.franchise_cache[franchise_name]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM franchises WHERE name = %s",
                (franchise_name,)
            )
            result = cursor.fetchone()

            if result:
                self.franchise_cache[franchise_name] = result[0]
                return result[0]

            cursor.execute(
                """
                INSERT INTO franchises (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (franchise_name,)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.franchise_cache[franchise_name] = result[0]
                return result[0]

            # Conflict case
            cursor.execute("SELECT id FROM franchises WHERE name = %s", (franchise_name,))
            result = cursor.fetchone()
            self.franchise_cache[franchise_name] = result[0]
            return result[0]

    def ensure_media(self, conn, title: str, year: int, franchise_id: int) -> int:
        """Get or create a media record, returning its ID."""
        cache_key = (title.lower(), year, "film")
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT id FROM media
                WHERE title = %s AND year = %s AND media_type = 'film'
                """,
                (title, year)
            )
            result = cursor.fetchone()

            if result:
                self.media_cache[cache_key] = result[0]
                return result[0]

            cursor.execute(
                """
                INSERT INTO media (title, year, franchise_id, media_type)
                VALUES (%s, %s, %s, 'film')
                ON CONFLICT (title, year, media_type) DO NOTHING
                RETURNING id
                """,
                (title, year, franchise_id)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.media_cache[cache_key] = result[0]
                return result[0]

            # Conflict case
            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'film'",
                (title, year)
            )
            result = cursor.fetchone()
            self.media_cache[cache_key] = result[0]
            return result[0]

    def process_batch(self, batch: list[dict]):
        """Process a batch of film records."""
        with get_db_connection() as conn:
            for msg in batch:
                film = msg["value"]

                try:
                    # Get/create studio
                    studio_id = self.ensure_studio(conn, film["studio"])

                    # Get/create franchise
                    franchise_id = self.ensure_franchise(conn, film["franchise"])

                    # Get/create media record
                    media_id = self.ensure_media(conn, film["title"], film["year"], franchise_id)

                    # Create film record
                    animation_type = film.get("animation_type", "fully_animated")

                    with get_cursor(conn) as cursor:
                        cursor.execute(
                            """
                            INSERT INTO films (media_id, studio_id, animation_type)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (media_id) DO NOTHING
                            """,
                            (media_id, studio_id, animation_type)
                        )
                        conn.commit()

                    logger.debug(f"Loaded film: {film['title']} ({film['year']})")

                except Exception as e:
                    logger.error(f"Error processing film {film.get('title')}: {e}")
                    conn.rollback()
                    raise

    def consume(self, max_messages: int = None):
        """Consume and process film records."""
        logger.info("Starting films consumer...")

        consumer = JSONConsumer(
            group_id="films",
            topics=["raw-films"],
            process_batch=self.process_batch,
            batch_size=50,
        )

        return consumer.consume(max_messages=max_messages)


def main():
    """CLI entry point for the films consumer."""
    import argparse

    parser = argparse.ArgumentParser(description="Consume film records from Kafka")
    parser.add_argument("--max-messages", type=int, help="Maximum messages to consume")
    args = parser.parse_args()

    consumer = FilmsConsumer()
    consumer.consume(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
