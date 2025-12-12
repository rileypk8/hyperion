"""Consumer for award nomination records - loads into PostgreSQL."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONConsumer
from shared.db import get_db_connection, get_cursor
from shared.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AwardsConsumer:
    """
    Consumes award nomination records from Kafka and loads into PostgreSQL.

    Handles:
    - award_bodies lookup table
    - award_categories lookup table
    - nominations table (linking media, category, talent, song)
    """

    def __init__(self):
        # Caches to reduce DB lookups
        self.media_cache = {}  # (title, year) -> id
        self.award_body_cache = {}  # name -> id
        self.award_category_cache = {}  # (body_id, name) -> id
        self.song_cache = {}  # (film_title, song_title) -> id

    def get_media_id(self, conn, film_title: str, film_year: int) -> int | None:
        """Get media ID for the film."""
        cache_key = (film_title.lower(), film_year)
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT id FROM media
                WHERE title = %s AND year = %s AND media_type = 'film'
                """,
                (film_title, film_year)
            )
            result = cursor.fetchone()

            if result:
                self.media_cache[cache_key] = result[0]
                return result[0]

            return None

    def get_or_create_award_body_id(self, conn, body_name: str) -> int:
        """Get or create award body ID."""
        if body_name in self.award_body_cache:
            return self.award_body_cache[body_name]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM award_bodies WHERE name = %s",
                (body_name,)
            )
            result = cursor.fetchone()

            if result:
                self.award_body_cache[body_name] = result[0]
                return result[0]

            cursor.execute(
                """
                INSERT INTO award_bodies (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (body_name,)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.award_body_cache[body_name] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM award_bodies WHERE name = %s", (body_name,))
            result = cursor.fetchone()
            self.award_body_cache[body_name] = result[0]
            return result[0]

    def get_or_create_award_category_id(
        self,
        conn,
        body_id: int,
        category_name: str,
        is_media_level: bool = True,
    ) -> int:
        """Get or create award category ID."""
        cache_key = (body_id, category_name)
        if cache_key in self.award_category_cache:
            return self.award_category_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT id FROM award_categories
                WHERE award_body_id = %s AND name = %s
                """,
                (body_id, category_name)
            )
            result = cursor.fetchone()

            if result:
                self.award_category_cache[cache_key] = result[0]
                return result[0]

            cursor.execute(
                """
                INSERT INTO award_categories (award_body_id, name, is_media_level)
                VALUES (%s, %s, %s)
                ON CONFLICT (award_body_id, name) DO NOTHING
                RETURNING id
                """,
                (body_id, category_name, is_media_level)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.award_category_cache[cache_key] = result[0]
                return result[0]

            cursor.execute(
                "SELECT id FROM award_categories WHERE award_body_id = %s AND name = %s",
                (body_id, category_name)
            )
            result = cursor.fetchone()
            self.award_category_cache[cache_key] = result[0]
            return result[0]

    def get_song_id(self, conn, film_title: str, film_year: int, song_title: str) -> int | None:
        """Get song ID if it exists."""
        if not song_title:
            return None

        cache_key = (film_title.lower(), song_title.lower())
        if cache_key in self.song_cache:
            return self.song_cache[cache_key]

        with get_cursor(conn) as cursor:
            # Find the song through soundtrack -> media chain
            cursor.execute(
                """
                SELECT s.id
                FROM songs s
                JOIN soundtracks st ON s.soundtrack_id = st.id
                JOIN media m ON st.source_media_id = m.id
                WHERE m.title = %s AND m.year = %s AND s.title = %s
                """,
                (film_title, film_year, song_title)
            )
            result = cursor.fetchone()

            if result:
                self.song_cache[cache_key] = result[0]
                return result[0]

            return None

    def process_batch(self, batch: list[dict]):
        """Process a batch of award records."""
        with get_db_connection() as conn:
            for msg in batch:
                award = msg["value"]

                try:
                    # Get media ID for the film
                    media_id = self.get_media_id(
                        conn,
                        award["film_title"],
                        award["film_year"]
                    )

                    if not media_id:
                        logger.debug(
                            f"Skipping award - film not found: "
                            f"{award['film_title']} ({award['film_year']})"
                        )
                        continue

                    # Get/create award body
                    body_id = self.get_or_create_award_body_id(conn, award["award_body"])

                    # Determine if this is media-level or individual award
                    is_media_level = award.get("song_title") is None

                    # Get/create award category
                    category_id = self.get_or_create_award_category_id(
                        conn,
                        body_id,
                        award["category"],
                        is_media_level
                    )

                    # Get song ID if this is a song award
                    song_id = None
                    if award.get("song_title"):
                        song_id = self.get_song_id(
                            conn,
                            award["film_title"],
                            award["film_year"],
                            award["song_title"]
                        )

                    # Map outcome to enum
                    outcome = award.get("outcome", "nominated")
                    if outcome not in ("nominated", "won"):
                        outcome = "nominated"

                    # Insert nomination
                    with get_cursor(conn) as cursor:
                        cursor.execute(
                            """
                            INSERT INTO nominations
                                (media_id, award_category_id, year, outcome, talent_id, song_id)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (media_id, award_category_id, year, talent_id, song_id)
                            DO UPDATE SET outcome = EXCLUDED.outcome
                            """,
                            (
                                media_id,
                                category_id,
                                award.get("ceremony_year"),
                                outcome,
                                None,  # talent_id - could be populated from data
                                song_id,
                            )
                        )
                        conn.commit()

                    logger.debug(
                        f"Loaded nomination: {award['film_title']} - "
                        f"{award['category']} ({outcome})"
                    )

                except Exception as e:
                    logger.error(f"Error processing award for {award.get('film_title')}: {e}")
                    conn.rollback()
                    raise

    def consume(self, max_messages: int = None):
        """Consume and process award records."""
        logger.info("Starting awards consumer...")

        consumer = JSONConsumer(
            group_id="awards",
            topics=["raw-awards"],
            process_batch=self.process_batch,
            batch_size=50,
        )

        return consumer.consume(max_messages=max_messages)


def main():
    """CLI entry point for the awards consumer."""
    import argparse

    parser = argparse.ArgumentParser(description="Consume award records from Kafka")
    parser.add_argument("--max-messages", type=int, help="Maximum messages to consume")
    args = parser.parse_args()

    consumer = AwardsConsumer()
    consumer.consume(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
