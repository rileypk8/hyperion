"""Consumer for soundtrack records - loads into PostgreSQL."""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONConsumer
from shared.db import get_db_connection, get_cursor
from shared.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SoundtracksConsumer:
    """
    Consumes soundtrack records from Kafka and loads into PostgreSQL.

    Handles:
    - media table (parent records with media_type='soundtrack')
    - soundtracks table (child records with label, source_media_id)
    - songs table (tracks on soundtracks)
    - song_credits table (M:N junction for songs ↔ talent ↔ credit_type)
    - talent table (composers, performers)
    - credit_types lookup table
    """

    def __init__(self):
        # Caches to reduce DB lookups
        self.franchise_cache = {}  # name -> id
        self.media_cache = {}  # (title, year, type) -> id
        self.talent_cache = {}  # name -> id
        self.credit_type_cache = {}  # name -> id

    def get_franchise_id(self, conn, franchise_name: str) -> int:
        """Get or create a franchise ID."""
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

            cursor.execute("SELECT id FROM franchises WHERE name = %s", (franchise_name,))
            result = cursor.fetchone()
            self.franchise_cache[franchise_name] = result[0]
            return result[0]

    def get_source_media_id(self, conn, film_title: str, film_year: int) -> int | None:
        """Get media ID for the source film."""
        cache_key = (film_title.lower(), film_year, "film")
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

    def ensure_soundtrack_media(
        self,
        conn,
        title: str,
        year: int,
        franchise_id: int,
    ) -> int:
        """Get or create a media record for a soundtrack."""
        cache_key = (title.lower(), year, "soundtrack")
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                """
                SELECT id FROM media
                WHERE title = %s AND year = %s AND media_type = 'soundtrack'
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
                VALUES (%s, %s, %s, 'soundtrack')
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

            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'soundtrack'",
                (title, year)
            )
            result = cursor.fetchone()
            self.media_cache[cache_key] = result[0]
            return result[0]

    def get_or_create_talent_id(self, conn, talent_name: str) -> int | None:
        """Get or create talent ID."""
        if not talent_name:
            return None

        talent_name = talent_name.strip()
        if talent_name in self.talent_cache:
            return self.talent_cache[talent_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM talent WHERE name = %s", (talent_name,))
            result = cursor.fetchone()

            if result:
                self.talent_cache[talent_name] = result[0]
                return result[0]

            cursor.execute(
                """
                INSERT INTO talent (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (talent_name,)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.talent_cache[talent_name] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM talent WHERE name = %s", (talent_name,))
            result = cursor.fetchone()
            if result:
                self.talent_cache[talent_name] = result[0]
                return result[0]

            return None

    def get_credit_type_id(self, conn, credit_type: str) -> int | None:
        """Get credit type ID from lookup table."""
        if not credit_type:
            return None

        credit_type = credit_type.lower()
        if credit_type in self.credit_type_cache:
            return self.credit_type_cache[credit_type]

        # Map common variations
        type_map = {
            "character_voice": "performer",
            "voice": "performer",
            "singer": "performer",
        }
        credit_type = type_map.get(credit_type, credit_type)

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM credit_types WHERE name = %s",
                (credit_type,)
            )
            result = cursor.fetchone()

            if result:
                self.credit_type_cache[credit_type] = result[0]
                return result[0]

            # Insert if not exists
            cursor.execute(
                """
                INSERT INTO credit_types (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (credit_type,)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.credit_type_cache[credit_type] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM credit_types WHERE name = %s", (credit_type,))
            result = cursor.fetchone()
            if result:
                self.credit_type_cache[credit_type] = result[0]
                return result[0]

            return None

    def process_batch(self, batch: list[dict]):
        """Process a batch of soundtrack records."""
        with get_db_connection() as conn:
            for msg in batch:
                st = msg["value"]

                try:
                    film_title = st.get("film_title")
                    film_year = st.get("film_year")

                    # Use film title as franchise name
                    franchise_id = self.get_franchise_id(conn, film_title)

                    # Get source media ID (the film this soundtrack is for)
                    source_media_id = self.get_source_media_id(conn, film_title, film_year)

                    # Create media record for the soundtrack
                    # Use film_year as soundtrack year
                    media_id = self.ensure_soundtrack_media(
                        conn, st["title"], film_year, franchise_id
                    )

                    # Create soundtrack record
                    with get_cursor(conn) as cursor:
                        cursor.execute(
                            """
                            INSERT INTO soundtracks (media_id, source_media_id, label)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (media_id) DO UPDATE SET
                                source_media_id = COALESCE(EXCLUDED.source_media_id, soundtracks.source_media_id),
                                label = COALESCE(EXCLUDED.label, soundtracks.label)
                            RETURNING id
                            """,
                            (media_id, source_media_id, st.get("label"))
                        )
                        result = cursor.fetchone()
                        soundtrack_id = result[0] if result else None
                        conn.commit()

                    if not soundtrack_id:
                        continue

                    # Process songs
                    for song in st.get("songs", []):
                        with get_cursor(conn) as cursor:
                            cursor.execute(
                                """
                                INSERT INTO songs (soundtrack_id, title, track_number)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (soundtrack_id, title) DO NOTHING
                                RETURNING id
                                """,
                                (soundtrack_id, song["title"], song.get("track_number"))
                            )
                            result = cursor.fetchone()
                            song_id = result[0] if result else None
                            conn.commit()

                        if not song_id:
                            # Song already exists, fetch its ID
                            with get_cursor(conn) as cursor:
                                cursor.execute(
                                    "SELECT id FROM songs WHERE soundtrack_id = %s AND title = %s",
                                    (soundtrack_id, song["title"])
                                )
                                result = cursor.fetchone()
                                song_id = result[0] if result else None

                        if not song_id:
                            continue

                        # Add song credits (performers)
                        for performer in song.get("performers", []):
                            talent_id = self.get_or_create_talent_id(conn, performer.get("name"))
                            credit_type_id = self.get_credit_type_id(conn, performer.get("type", "performer"))

                            if talent_id and credit_type_id:
                                with get_cursor(conn) as cursor:
                                    cursor.execute(
                                        """
                                        INSERT INTO song_credits (song_id, talent_id, credit_type_id)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (song_id, talent_id, credit_type_id) DO NOTHING
                                        """,
                                        (song_id, talent_id, credit_type_id)
                                    )
                                    conn.commit()

                        # Add track-specific composer if present
                        if song.get("composer"):
                            talent_id = self.get_or_create_talent_id(conn, song["composer"])
                            credit_type_id = self.get_credit_type_id(conn, "composer")

                            if talent_id and credit_type_id:
                                with get_cursor(conn) as cursor:
                                    cursor.execute(
                                        """
                                        INSERT INTO song_credits (song_id, talent_id, credit_type_id)
                                        VALUES (%s, %s, %s)
                                        ON CONFLICT (song_id, talent_id, credit_type_id) DO NOTHING
                                        """,
                                        (song_id, talent_id, credit_type_id)
                                    )
                                    conn.commit()

                    # Process soundtrack-level credits (composers, conductors)
                    # These apply to all songs on the soundtrack
                    # We'll add them to the first song as a simplification
                    # (In a production system, you might have album_credits table)

                    logger.debug(f"Loaded soundtrack: {st['title']}")

                except Exception as e:
                    logger.error(f"Error processing soundtrack {st.get('title')}: {e}")
                    conn.rollback()
                    raise

    def consume(self, max_messages: int = None):
        """Consume and process soundtrack records."""
        logger.info("Starting soundtracks consumer...")

        consumer = JSONConsumer(
            group_id="soundtracks",
            topics=["raw-soundtracks"],
            process_batch=self.process_batch,
            batch_size=20,
        )

        return consumer.consume(max_messages=max_messages)


def main():
    """CLI entry point for the soundtracks consumer."""
    import argparse

    parser = argparse.ArgumentParser(description="Consume soundtrack records from Kafka")
    parser.add_argument("--max-messages", type=int, help="Maximum messages to consume")
    args = parser.parse_args()

    consumer = SoundtracksConsumer()
    consumer.consume(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
