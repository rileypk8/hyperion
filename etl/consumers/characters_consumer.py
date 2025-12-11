"""Consumer for character records - loads into PostgreSQL."""

import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONConsumer
from shared.db import get_db_connection, get_cursor, get_or_create_lookup_ids
from shared.config import settings
from shared.schemas import normalize_species

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CharactersConsumer:
    """
    Consumes character records from Kafka and loads into PostgreSQL.

    Handles:
    - characters table (with species, gender lookups)
    - talent table (voice actors)
    - character_appearances table (linking character, media, talent, role)
    """

    def __init__(self):
        # Caches to reduce DB lookups
        self.role_cache = {}  # name -> id
        self.gender_cache = {}  # name -> id
        self.species_cache = {}  # name -> id
        self.character_cache = {}  # (name, franchise) -> id
        self.talent_cache = {}  # name -> id
        self.media_cache = {}  # (title, year) -> id

    def get_role_id(self, conn, role_name: str) -> int:
        """Get role ID from lookup table."""
        if role_name in self.role_cache:
            return self.role_cache[role_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM roles WHERE name = %s", (role_name,))
            result = cursor.fetchone()

            if result:
                self.role_cache[role_name] = result[0]
                return result[0]

            # Default to 'supporting' if not found
            cursor.execute("SELECT id FROM roles WHERE name = 'supporting'")
            result = cursor.fetchone()
            self.role_cache[role_name] = result[0]
            return result[0]

    def get_gender_id(self, conn, gender_name: str) -> int | None:
        """Get gender ID from lookup table."""
        if not gender_name:
            return None

        if gender_name in self.gender_cache:
            return self.gender_cache[gender_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM genders WHERE name = %s", (gender_name,))
            result = cursor.fetchone()

            if result:
                self.gender_cache[gender_name] = result[0]
                return result[0]

            # Default to 'n/a' if not found
            cursor.execute("SELECT id FROM genders WHERE name = 'n/a'")
            result = cursor.fetchone()
            if result:
                self.gender_cache[gender_name] = result[0]
                return result[0]

            return None

    def get_or_create_species_id(self, conn, species_name: str) -> int | None:
        """Get or create species ID."""
        if not species_name:
            return None

        # Normalize the species name
        normalized = normalize_species(species_name)
        if not normalized:
            return None

        if normalized in self.species_cache:
            return self.species_cache[normalized]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM species WHERE name = %s", (normalized,))
            result = cursor.fetchone()

            if result:
                self.species_cache[normalized] = result[0]
                return result[0]

            # Insert new species
            cursor.execute(
                """
                INSERT INTO species (name)
                VALUES (%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
                """,
                (normalized,)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.species_cache[normalized] = result[0]
                return result[0]

            # Conflict - fetch existing
            cursor.execute("SELECT id FROM species WHERE name = %s", (normalized,))
            result = cursor.fetchone()
            if result:
                self.species_cache[normalized] = result[0]
                return result[0]

            return None

    def get_or_create_talent_id(self, conn, talent_name: str) -> int | None:
        """Get or create talent ID."""
        if not talent_name:
            return None

        # Clean up the name
        talent_name = talent_name.strip()
        if talent_name.lower() in ["n/a", "none", "unknown", ""]:
            return None

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

            # Conflict - fetch existing
            cursor.execute("SELECT id FROM talent WHERE name = %s", (talent_name,))
            result = cursor.fetchone()
            if result:
                self.talent_cache[talent_name] = result[0]
                return result[0]

            return None

    def get_or_create_character_id(
        self,
        conn,
        name: str,
        franchise: str,
        species_id: int | None,
        gender_id: int | None,
    ) -> int:
        """Get or create character ID."""
        cache_key = (name.lower(), franchise.lower())
        if cache_key in self.character_cache:
            return self.character_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM characters WHERE name = %s AND origin_franchise = %s",
                (name, franchise)
            )
            result = cursor.fetchone()

            if result:
                self.character_cache[cache_key] = result[0]
                return result[0]

            cursor.execute(
                """
                INSERT INTO characters (name, origin_franchise, species_id, gender_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name, origin_franchise) DO NOTHING
                RETURNING id
                """,
                (name, franchise, species_id, gender_id)
            )
            result = cursor.fetchone()

            if result:
                conn.commit()
                self.character_cache[cache_key] = result[0]
                return result[0]

            # Conflict - fetch existing
            cursor.execute(
                "SELECT id FROM characters WHERE name = %s AND origin_franchise = %s",
                (name, franchise)
            )
            result = cursor.fetchone()
            self.character_cache[cache_key] = result[0]
            return result[0]

    def get_media_id(self, conn, title: str, year: int) -> int | None:
        """Get media ID by title and year."""
        if not title or not year:
            return None

        cache_key = (title.lower(), year)
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s",
                (title, year)
            )
            result = cursor.fetchone()

            if result:
                self.media_cache[cache_key] = result[0]
                return result[0]

            return None

    def parse_voice_actor(self, voice_actor_str: str) -> str | None:
        """Extract primary voice actor from possibly complex string."""
        if not voice_actor_str:
            return None

        va = voice_actor_str.strip()
        if va.lower() in ["n/a", "none", "unknown", "null"]:
            return None

        # Handle multiple actors - take the first one
        # Pattern: "Actor1 (films), Actor2 (films)"
        if "," in va:
            parts = re.split(r",\s*(?=[A-Z])", va)
            if parts:
                va = parts[0]

        # Remove parenthetical notes
        va = re.sub(r"\s*\([^)]*\)\s*$", "", va).strip()

        if va and va.lower() not in ["n/a", "none"]:
            return va

        return None

    def process_batch(self, batch: list[dict]):
        """Process a batch of character records."""
        with get_db_connection() as conn:
            for msg in batch:
                char = msg["value"]

                try:
                    name = char.get("name")
                    franchise = char.get("franchise", "Unknown")

                    if not name:
                        continue

                    # Get lookup IDs
                    role_id = self.get_role_id(conn, char.get("role", "supporting"))
                    gender_id = self.get_gender_id(conn, char.get("gender"))
                    species_id = self.get_or_create_species_id(conn, char.get("species"))

                    # Get/create character
                    character_id = self.get_or_create_character_id(
                        conn, name, franchise, species_id, gender_id
                    )

                    # Get talent ID if voice actor provided
                    voice_actor_name = self.parse_voice_actor(char.get("voice_actor"))
                    talent_id = self.get_or_create_talent_id(conn, voice_actor_name)

                    # Get media ID if film info provided
                    media_id = self.get_media_id(
                        conn,
                        char.get("film_title"),
                        char.get("film_year")
                    )

                    # Create character appearance if we have a media link
                    if media_id:
                        with get_cursor(conn) as cursor:
                            cursor.execute(
                                """
                                INSERT INTO character_appearances
                                    (character_id, media_id, talent_id, role_id, notes)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (character_id, media_id, variant) DO NOTHING
                                """,
                                (character_id, media_id, talent_id, role_id, char.get("notes"))
                            )
                            conn.commit()

                    logger.debug(f"Loaded character: {name} ({franchise})")

                except Exception as e:
                    logger.error(f"Error processing character {char.get('name')}: {e}")
                    conn.rollback()
                    raise

    def consume(self, max_messages: int = None):
        """Consume and process character records."""
        logger.info("Starting characters consumer...")

        consumer = JSONConsumer(
            group_id="characters",
            topics=["raw-characters"],
            process_batch=self.process_batch,
            batch_size=100,
        )

        return consumer.consume(max_messages=max_messages)


def main():
    """CLI entry point for the characters consumer."""
    import argparse

    parser = argparse.ArgumentParser(description="Consume character records from Kafka")
    parser.add_argument("--max-messages", type=int, help="Maximum messages to consume")
    args = parser.parse_args()

    consumer = CharactersConsumer()
    consumer.consume(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
