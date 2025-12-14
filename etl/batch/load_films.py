#!/usr/bin/env python3
"""Direct batch loader for films - extracts from JSON and loads to PostgreSQL."""

import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.config import settings
from shared.db import get_db_connection, get_cursor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilmsLoader:
    """
    Extracts film data from character JSON files and loads directly to PostgreSQL.

    Handles:
    - studios table (with years_active and status)
    - franchises table
    - media table (parent records)
    - films table (child records linking to media and studio)
    """

    FILM_PATTERN = re.compile(r"^(.+?)\s*\((\d{4})\)$")

    DIR_TO_STUDIO = {
        "pixar_characters": "Pixar Animation Studios",
        "wdas_characters": "Walt Disney Animation Studios",
        "blue_sky_characters": "Blue Sky Studios",
        "disneytoon_characters": "DisneyToon Studios",
        "kingdom_hearts_characters": None,
        "disney_interactive_characters": None,
        "marvel_animation_characters": "Marvel Animation",
        "20th_century_animation": "20th Century Animation",
        "fox_disney_plus_characters": "20th Century Animation",
    }

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

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or settings.data_dir)
        self.seen_films = set()
        # Caches
        self.studio_cache = {}
        self.franchise_cache = {}
        self.media_cache = {}

    def parse_film_string(self, film_str: str) -> tuple[str, int] | None:
        match = self.FILM_PATTERN.match(film_str.strip())
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None

    def determine_studio(self, json_data: dict, source_dir: str) -> str | None:
        if "studio" in json_data:
            studio_name = json_data["studio"]
            if studio_name.lower() in ["pixar", "pixar animation"]:
                return "Pixar Animation Studios"
            if studio_name.lower() in ["disney", "wdas", "walt disney"]:
                return "Walt Disney Animation Studios"
            if "blue sky" in studio_name.lower():
                return "Blue Sky Studios"
            if "disneytoon" in studio_name.lower():
                return "DisneyToon Studios"
            if "marvel" in studio_name.lower():
                return "Marvel Animation"
            if "20th century" in studio_name.lower() or "fox" in studio_name.lower():
                return "20th Century Animation"
            return studio_name
        return self.DIR_TO_STUDIO.get(source_dir)

    def determine_animation_type(self, json_data: dict, title: str) -> str:
        if "animation_type" in json_data:
            return json_data["animation_type"]

        hybrid_films = {
            "enchanted", "mary poppins", "song of the south", "bedknobs and broomsticks",
            "pete's dragon", "who framed roger rabbit", "james and the giant peach",
            "the jungle book (2016)", "christopher robin", "chip 'n dale: rescue rangers",
        }

        if title.lower() in hybrid_films or any(h in title.lower() for h in hybrid_films):
            return "hybrid"
        return "fully_animated"

    def extract_films(self):
        """Extract film records from JSON files."""
        for json_file in self.data_dir.rglob("*.json"):
            source_dir = json_file.parent.name
            source_file = json_file.name

            if source_file.startswith("00_") or "summary" in source_file.lower():
                continue
            if source_dir in ["kingdom_hearts_characters", "disney_interactive_characters"]:
                continue

            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse {json_file}: {e}")
                continue

            if not isinstance(data, dict):
                continue

            studio = self.determine_studio(data, source_dir)
            franchise = data.get("franchise", "Unknown")

            for film_str in data.get("films", []):
                parsed = self.parse_film_string(film_str)
                if not parsed:
                    continue

                title, year = parsed
                film_key = (title.lower(), year)

                if film_key in self.seen_films:
                    continue
                self.seen_films.add(film_key)

                if studio:
                    yield {
                        "title": title,
                        "year": year,
                        "studio": studio,
                        "franchise": franchise,
                        "animation_type": self.determine_animation_type(data, title),
                    }

    def ensure_studio(self, conn, studio_name: str) -> int:
        if studio_name in self.studio_cache:
            return self.studio_cache[studio_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM studios WHERE name = %s", (studio_name,))
            result = cursor.fetchone()

            if result:
                self.studio_cache[studio_name] = result[0]
                return result[0]

            meta = self.STUDIO_METADATA.get(studio_name, {"start": 2000, "end": None, "status": "active"})
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

            cursor.execute("SELECT id FROM studios WHERE name = %s", (studio_name,))
            result = cursor.fetchone()
            self.studio_cache[studio_name] = result[0]
            return result[0]

    def ensure_franchise(self, conn, franchise_name: str) -> int:
        if franchise_name in self.franchise_cache:
            return self.franchise_cache[franchise_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM franchises WHERE name = %s", (franchise_name,))
            result = cursor.fetchone()

            if result:
                self.franchise_cache[franchise_name] = result[0]
                return result[0]

            cursor.execute(
                "INSERT INTO franchises (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
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

    def ensure_media(self, conn, title: str, year: int, franchise_id: int) -> int:
        cache_key = (title.lower(), year, "film")
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'film'",
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

            cursor.execute(
                "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = 'film'",
                (title, year)
            )
            result = cursor.fetchone()
            self.media_cache[cache_key] = result[0]
            return result[0]

    def load(self, dry_run: bool = False) -> int:
        """Load all films to PostgreSQL."""
        films = list(self.extract_films())
        logger.info(f"Found {len(films)} films to load")

        if dry_run:
            for film in films:
                print(f"{film['title']} ({film['year']}) - {film['studio']} - {film['franchise']}")
            return len(films)

        with get_db_connection() as conn:
            count = 0
            for film in films:
                try:
                    studio_id = self.ensure_studio(conn, film["studio"])
                    franchise_id = self.ensure_franchise(conn, film["franchise"])
                    media_id = self.ensure_media(conn, film["title"], film["year"], franchise_id)

                    with get_cursor(conn) as cursor:
                        cursor.execute(
                            """
                            INSERT INTO films (media_id, studio_id, animation_type)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (media_id) DO NOTHING
                            """,
                            (media_id, studio_id, film["animation_type"])
                        )
                        conn.commit()

                    count += 1
                    if count % 50 == 0:
                        logger.info(f"Loaded {count} films...")

                except Exception as e:
                    logger.error(f"Error loading film {film['title']}: {e}")
                    conn.rollback()

            logger.info(f"Finished loading {count} films")
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load films from JSON to PostgreSQL")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print films without loading")
    args = parser.parse_args()

    loader = FilmsLoader(data_dir=args.data_dir)
    loader.load(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
