#!/usr/bin/env python3
"""Direct batch loader for awards - extracts from JSON and loads to PostgreSQL."""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.config import settings
from shared.db import get_db_connection, get_cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AwardsLoader:
    """
    Extracts award nominations from wdas_soundtracks_complete.json
    and loads directly to PostgreSQL.

    Handles:
    - award_bodies lookup table
    - award_categories lookup table
    - nominations table (linking media, category, talent, song)
    """

    AWARD_BODY_MAP = {
        "oscar": "Academy Awards",
        "academy award": "Academy Awards",
        "golden globe": "Golden Globe Awards",
        "annie": "Annie Awards",
        "bafta": "BAFTA",
        "grammy": "Grammy Awards",
    }

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or settings.data_dir)
        self.soundtrack_file = self.data_dir / "wdas_soundtracks_complete.json"
        # Caches
        self.media_cache = {}
        self.award_body_cache = {}
        self.award_category_cache = {}
        self.song_cache = {}

    def load_data(self) -> list[dict]:
        if not self.soundtrack_file.exists():
            logger.warning(f"Soundtracks file not found: {self.soundtrack_file}")
            return []
        try:
            with open(self.soundtrack_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse soundtracks file: {e}")
            return []

    def normalize_award_body(self, award_name: str) -> str:
        if not award_name:
            return "Academy Awards"
        award_lower = award_name.lower()
        for key, value in self.AWARD_BODY_MAP.items():
            if key in award_lower:
                return value
        return award_name

    def extract_awards(self):
        """Extract award nomination records."""
        data = self.load_data()

        for film_entry in data:
            film_title = film_entry.get("film")
            film_year = film_entry.get("film_year")
            if not film_title or not film_year:
                continue

            for release in film_entry.get("releases", []):
                # Album-level awards
                for award in release.get("album_awards", []):
                    yield {
                        "film_title": film_title,
                        "film_year": film_year,
                        "award_body": self.normalize_award_body(award.get("award")),
                        "category": award.get("category"),
                        "ceremony_year": award.get("year"),
                        "outcome": award.get("result", "nominated"),
                        "song_title": None,
                    }

                # Track-level awards
                for track in release.get("tracks", []):
                    song_title = track.get("title")
                    for award in track.get("awards", []):
                        yield {
                            "film_title": film_title,
                            "film_year": film_year,
                            "award_body": self.normalize_award_body(award.get("award")),
                            "category": award.get("category"),
                            "ceremony_year": award.get("year"),
                            "outcome": award.get("result", "nominated"),
                            "song_title": song_title,
                        }

    def get_media_id(self, conn, film_title: str, film_year: int) -> int | None:
        cache_key = (film_title.lower(), film_year)
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'film'",
                (film_title, film_year)
            )
            result = cursor.fetchone()
            if result:
                self.media_cache[cache_key] = result[0]
                return result[0]
            return None

    def get_or_create_award_body_id(self, conn, body_name: str) -> int:
        if body_name in self.award_body_cache:
            return self.award_body_cache[body_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM award_bodies WHERE name = %s", (body_name,))
            result = cursor.fetchone()
            if result:
                self.award_body_cache[body_name] = result[0]
                return result[0]

            cursor.execute(
                "INSERT INTO award_bodies (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
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

    def get_or_create_award_category_id(self, conn, body_id: int, category_name: str, is_media_level: bool) -> int:
        cache_key = (body_id, category_name)
        if cache_key in self.award_category_cache:
            return self.award_category_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM award_categories WHERE award_body_id = %s AND name = %s",
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
        if not song_title:
            return None

        cache_key = (film_title.lower(), song_title.lower())
        if cache_key in self.song_cache:
            return self.song_cache[cache_key]

        with get_cursor(conn) as cursor:
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

    def load(self, dry_run: bool = False) -> int:
        """Load all awards to PostgreSQL."""
        awards = list(self.extract_awards())
        wins = sum(1 for a in awards if a.get("outcome") == "won")
        noms = len(awards) - wins
        logger.info(f"Found {len(awards)} award nominations ({wins} wins, {noms} nominations)")

        if dry_run:
            for award in awards:
                outcome = award["outcome"]
                song_info = f" (Song: {award['song_title']})" if award["song_title"] else ""
                print(f"{award['film_title']} ({award['film_year']}){song_info}")
                print(f"  {award['award_body']} - {award['category']} - {outcome.upper()} ({award['ceremony_year']})")
            return len(awards)

        with get_db_connection() as conn:
            count = 0
            skipped = 0
            for award in awards:
                try:
                    media_id = self.get_media_id(conn, award["film_title"], award["film_year"])
                    if not media_id:
                        skipped += 1
                        continue

                    body_id = self.get_or_create_award_body_id(conn, award["award_body"])
                    is_media_level = award.get("song_title") is None
                    category_id = self.get_or_create_award_category_id(
                        conn, body_id, award["category"], is_media_level
                    )

                    song_id = None
                    if award.get("song_title"):
                        song_id = self.get_song_id(conn, award["film_title"], award["film_year"], award["song_title"])

                    outcome = award.get("outcome", "nominated")
                    if outcome not in ("nominated", "won"):
                        outcome = "nominated"

                    with get_cursor(conn) as cursor:
                        cursor.execute(
                            """
                            INSERT INTO nominations
                                (media_id, award_category_id, year, outcome, talent_id, song_id)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (media_id, award_category_id, year, talent_id, song_id)
                            DO UPDATE SET outcome = EXCLUDED.outcome
                            """,
                            (media_id, category_id, award.get("ceremony_year"), outcome, None, song_id)
                        )
                        conn.commit()

                    count += 1
                    if count % 20 == 0:
                        logger.info(f"Loaded {count} nominations...")

                except Exception as e:
                    logger.error(f"Error loading award for {award.get('film_title')}: {e}")
                    conn.rollback()

            logger.info(f"Finished loading {count} nominations (skipped {skipped} for missing films)")
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load awards from JSON to PostgreSQL")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print awards without loading")
    args = parser.parse_args()

    loader = AwardsLoader(data_dir=args.data_dir)
    loader.load(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
