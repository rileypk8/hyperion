"""Consumer for game records - loads into PostgreSQL."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONConsumer
from shared.db import get_db_connection, get_cursor
from shared.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GamesConsumer:
    """
    Consumes game records from Kafka and loads into PostgreSQL.

    Handles:
    - franchises table (reuse existing)
    - media table (parent records with media_type='game')
    - games table (child records with developer/publisher)
    - game_platforms table (M:N junction)
    - platforms lookup table
    """

    def __init__(self):
        # Caches to reduce DB lookups
        self.franchise_cache = {}  # name -> id
        self.media_cache = {}  # (title, year, type) -> id
        self.platform_cache = {}  # name -> id

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
        """Get or create a media record for a game, returning its ID."""
        cache_key = (title.lower(), year, "game")
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT id FROM media
                WHERE title = %s AND year = %s AND media_type = 'game'
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
                VALUES (%s, %s, %s, 'game')
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
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'game'",
                (title, year)
            )
            result = cursor.fetchone()
            self.media_cache[cache_key] = result[0]
            return result[0]

    def get_platform_id(self, conn, platform_name: str) -> int | None:
        """Get platform ID from lookup table."""
        if not platform_name:
            return None

        if platform_name in self.platform_cache:
            return self.platform_cache[platform_name]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM platforms WHERE name = %s",
                (platform_name,)
            )
            result = cursor.fetchone()

            if result:
                self.platform_cache[platform_name] = result[0]
                return result[0]

            # Insert if not exists (platforms table has seed data but may need additions)
            cursor.execute(
                """
                INSERT INTO platforms (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (platform_name,)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.platform_cache[platform_name] = result[0]
                return result[0]

            # Conflict - fetch existing
            cursor.execute("SELECT id FROM platforms WHERE name = %s", (platform_name,))
            result = cursor.fetchone()
            if result:
                self.platform_cache[platform_name] = result[0]
                return result[0]

            return None

    def process_batch(self, batch: list[dict]):
        """Process a batch of game records."""
        with get_db_connection() as conn:
            for msg in batch:
                game = msg["value"]

                try:
                    # Get/create franchise
                    franchise_id = self.ensure_franchise(conn, game["franchise"])

                    # Get/create media record
                    media_id = self.ensure_media(conn, game["title"], game["year"], franchise_id)

                    # Create/update game record
                    with get_cursor(conn) as cursor:
                        cursor.execute(
                            """
                            INSERT INTO games (media_id, developer, publisher)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (media_id) DO UPDATE SET
                                developer = COALESCE(EXCLUDED.developer, games.developer),
                                publisher = COALESCE(EXCLUDED.publisher, games.publisher)
                            RETURNING id
                            """,
                            (media_id, game.get("developer"), game.get("publisher"))
                        )
                        result = cursor.fetchone()
                        game_id = result[0] if result else None
                        conn.commit()

                    # Add platform associations
                    if game_id and game.get("platforms"):
                        for platform_name in game["platforms"]:
                            platform_id = self.get_platform_id(conn, platform_name)
                            if platform_id:
                                with get_cursor(conn) as cursor:
                                    cursor.execute(
                                        """
                                        INSERT INTO game_platforms (game_id, platform_id)
                                        VALUES (%s, %s)
                                        ON CONFLICT (game_id, platform_id) DO NOTHING
                                        """,
                                        (game_id, platform_id)
                                    )
                                    conn.commit()

                    logger.debug(f"Loaded game: {game['title']} ({game['year']})")

                except Exception as e:
                    logger.error(f"Error processing game {game.get('title')}: {e}")
                    conn.rollback()
                    raise

    def consume(self, max_messages: int = None):
        """Consume and process game records."""
        logger.info("Starting games consumer...")

        consumer = JSONConsumer(
            group_id="games",
            topics=["raw-games"],
            process_batch=self.process_batch,
            batch_size=50,
        )

        return consumer.consume(max_messages=max_messages)


def main():
    """CLI entry point for the games consumer."""
    import argparse

    parser = argparse.ArgumentParser(description="Consume game records from Kafka")
    parser.add_argument("--max-messages", type=int, help="Maximum messages to consume")
    args = parser.parse_args()

    consumer = GamesConsumer()
    consumer.consume(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
