#!/usr/bin/env python3
"""Direct batch loader for characters - extracts from JSON and loads to PostgreSQL."""

import json
import logging
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.config import settings
from shared.db import get_db_connection, get_cursor
from shared.schemas import ROLE_MAPPING, GENDER_MAPPING, normalize_species

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CharactersLoader:
    """
    Extracts character data from JSON files and loads directly to PostgreSQL.

    Handles:
    - characters table (with species, gender lookups)
    - talent table (voice actors)
    - character_appearances table (linking character, media, talent, role)
    """

    GAME_DIRS = {"disney_interactive_characters", "kingdom_hearts_characters"}
    FILM_PATTERN = re.compile(r"^(.+?)\s*\((\d{4})\)$")

    YEAR_FROM_CATEGORY = {
        "toy_story_1": 1995, "toy_story_2": 1999, "toy_story_3": 2010, "toy_story_4": 2019,
        "ice_age_1": 2002, "ice_age_1_characters": 2002,
        "meltdown": 2006, "meltdown_characters": 2006,
        "dawn_of_dinosaurs": 2009, "dawn_of_dinosaurs_characters": 2009,
        "continental_drift": 2012, "continental_drift_characters": 2012,
        "collision_course": 2016, "collision_course_characters": 2016,
    }

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or settings.data_dir)
        # Caches
        self.role_cache = {}
        self.gender_cache = {}
        self.species_cache = {}
        self.character_cache = {}
        self.talent_cache = {}
        self.media_cache = {}

    def normalize_role(self, role: str) -> str:
        if not role:
            return "supporting"
        return ROLE_MAPPING.get(role.lower().strip(), "supporting")

    def normalize_gender(self, gender: str) -> str:
        if not gender:
            return "n/a"
        return GENDER_MAPPING.get(gender.lower().strip(), "n/a")

    def parse_film_string(self, film_str: str) -> tuple[str, int] | None:
        match = self.FILM_PATTERN.match(film_str.strip())
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None

    def extract_characters(self):
        """Extract character records from all JSON files."""
        for json_file in self.data_dir.rglob("*.json"):
            source_dir = json_file.parent.name
            source_file = f"{source_dir}/{json_file.name}"

            if json_file.name.startswith("00_") or "summary" in json_file.name.lower():
                continue

            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError:
                continue

            if not isinstance(data, dict):
                continue

            franchise = data.get("franchise", "Unknown")
            is_game_data = source_dir in self.GAME_DIRS or "games" in data
            media_type = "game" if is_game_data else "film"
            media_list = data.get("games", []) if is_game_data else data.get("films", [])

            # Handle nested characters dict
            if "characters" in data and isinstance(data["characters"], dict):
                for category, char_list in data["characters"].items():
                    if not isinstance(char_list, list):
                        continue
                    media_info = self._get_media_for_category(category, media_list)
                    yield from self._process_char_list(char_list, franchise, media_info, media_type, source_file)

            # Handle flat characters list
            elif "characters" in data and isinstance(data["characters"], list):
                default_media = None
                if len(media_list) == 1:
                    default_media = self.parse_film_string(media_list[0])
                yield from self._process_char_list(data["characters"], franchise, default_media, media_type, source_file)

            # Handle categories format
            elif "categories" in data and isinstance(data["categories"], dict):
                for category, cat_data in data["categories"].items():
                    if not isinstance(cat_data, dict):
                        continue
                    char_list = cat_data.get("characters", [])
                    if not char_list:
                        continue
                    year = cat_data.get("year")
                    media_info = None
                    if year:
                        for media_str in media_list:
                            parsed = self.parse_film_string(media_str)
                            if parsed and parsed[1] == year:
                                media_info = parsed
                                break
                    yield from self._process_char_list(char_list, franchise, media_info, media_type, source_file)

    def _get_media_for_category(self, category: str, media_list: list) -> tuple[str, int] | None:
        category_lower = category.lower()
        if category_lower in self.YEAR_FROM_CATEGORY:
            year = self.YEAR_FROM_CATEGORY[category_lower]
            for media_str in media_list:
                parsed = self.parse_film_string(media_str)
                if parsed and parsed[1] == year:
                    return parsed
        return None

    def _process_char_list(self, characters: list, franchise: str, media_info, media_type: str, source_file: str):
        for char in characters:
            if not isinstance(char, dict):
                continue
            name = char.get("name")
            if not name:
                continue

            origin_franchise = char.get("origin_franchise", franchise)
            media_title, media_year = media_info if media_info else (None, None)

            yield {
                "name": name,
                "franchise": franchise,
                "origin_franchise": origin_franchise,
                "role": self.normalize_role(char.get("role")),
                "species": normalize_species(char.get("species")),
                "gender": self.normalize_gender(char.get("gender")),
                "voice_actor": char.get("voice_actor"),
                "media_title": media_title,
                "media_year": media_year,
                "media_type": media_type,
                "notes": char.get("notes"),
            }

    def get_role_id(self, conn, role_name: str) -> int:
        if role_name in self.role_cache:
            return self.role_cache[role_name]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM roles WHERE name = %s", (role_name,))
            result = cursor.fetchone()
            if result:
                self.role_cache[role_name] = result[0]
                return result[0]
            cursor.execute("SELECT id FROM roles WHERE name = 'supporting'")
            result = cursor.fetchone()
            self.role_cache[role_name] = result[0]
            return result[0]

    def get_gender_id(self, conn, gender_name: str) -> int | None:
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
            cursor.execute("SELECT id FROM genders WHERE name = 'n/a'")
            result = cursor.fetchone()
            if result:
                self.gender_cache[gender_name] = result[0]
                return result[0]
            return None

    def get_or_create_species_id(self, conn, species_name: str) -> int | None:
        if not species_name:
            return None
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

            cursor.execute(
                "INSERT INTO species (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
                (normalized,)
            )
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.species_cache[normalized] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM species WHERE name = %s", (normalized,))
            result = cursor.fetchone()
            if result:
                self.species_cache[normalized] = result[0]
                return result[0]
            return None

    def get_or_create_talent_id(self, conn, talent_name: str) -> int | None:
        if not talent_name or talent_name.lower() in ["n/a", "none", "unknown", ""]:
            return None

        # Parse voice actor - take first one and remove parenthetical notes
        va = talent_name.strip()
        if "," in va:
            parts = re.split(r",\s*(?=[A-Z])", va)
            if parts:
                va = parts[0]
        va = re.sub(r"\s*\([^)]*\)\s*$", "", va).strip()

        if not va or va.lower() in ["n/a", "none"]:
            return None

        if va in self.talent_cache:
            return self.talent_cache[va]

        with get_cursor(conn) as cursor:
            cursor.execute("SELECT id FROM talent WHERE name = %s", (va,))
            result = cursor.fetchone()
            if result:
                self.talent_cache[va] = result[0]
                return result[0]

            cursor.execute(
                "INSERT INTO talent (name) VALUES (%s) ON CONFLICT (name) DO NOTHING RETURNING id",
                (va,)
            )
            result = cursor.fetchone()
            if result:
                conn.commit()
                self.talent_cache[va] = result[0]
                return result[0]

            cursor.execute("SELECT id FROM talent WHERE name = %s", (va,))
            result = cursor.fetchone()
            if result:
                self.talent_cache[va] = result[0]
                return result[0]
            return None

    def get_or_create_character_id(self, conn, name: str, franchise: str, species_id, gender_id) -> int:
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

            cursor.execute(
                "SELECT id FROM characters WHERE name = %s AND origin_franchise = %s",
                (name, franchise)
            )
            result = cursor.fetchone()
            self.character_cache[cache_key] = result[0]
            return result[0]

    def get_media_id(self, conn, title: str, year: int, media_type: str = None) -> int | None:
        if not title or not year:
            return None

        cache_key = (title.lower(), year, media_type)
        if cache_key in self.media_cache:
            return self.media_cache[cache_key]

        with get_cursor(conn) as cursor:
            if media_type:
                cursor.execute(
                    "SELECT id FROM media WHERE title = %s AND year = %s AND media_type = %s",
                    (title, year, media_type)
                )
            else:
                cursor.execute(
                    "SELECT id FROM media WHERE title = %s AND year = %s",
                    (title, year)
                )
            result = cursor.fetchone()
            if result:
                self.media_cache[cache_key] = result[0]
                return result[0]
            return None

    def load(self, dry_run: bool = False) -> int:
        """Load all characters to PostgreSQL."""
        characters = list(self.extract_characters())
        logger.info(f"Found {len(characters)} characters to load")

        if dry_run:
            film_count = sum(1 for c in characters if c.get("media_type") != "game")
            game_count = sum(1 for c in characters if c.get("media_type") == "game")
            for char in characters[:50]:
                type_tag = f"[{char['media_type']}]" if char.get("media_type") else ""
                print(f"{type_tag} {char['name']} ({char['franchise']}) - {char['role']}")
            if len(characters) > 50:
                print(f"... and {len(characters) - 50} more")
            print(f"\nTotal: {len(characters)} ({film_count} from films, {game_count} from games)")
            return len(characters)

        with get_db_connection() as conn:
            count = 0
            for char in characters:
                try:
                    origin_franchise = char.get("origin_franchise") or char.get("franchise", "Unknown")

                    role_id = self.get_role_id(conn, char.get("role", "supporting"))
                    gender_id = self.get_gender_id(conn, char.get("gender"))
                    species_id = self.get_or_create_species_id(conn, char.get("species"))
                    character_id = self.get_or_create_character_id(conn, char["name"], origin_franchise, species_id, gender_id)
                    talent_id = self.get_or_create_talent_id(conn, char.get("voice_actor"))
                    media_id = self.get_media_id(conn, char.get("media_title"), char.get("media_year"), char.get("media_type"))

                    if media_id:
                        with get_cursor(conn) as cursor:
                            cursor.execute(
                                """
                                INSERT INTO character_appearances (character_id, media_id, talent_id, role_id, notes)
                                VALUES (%s, %s, %s, %s, %s)
                                ON CONFLICT (character_id, media_id, variant) DO NOTHING
                                """,
                                (character_id, media_id, talent_id, role_id, char.get("notes"))
                            )
                            conn.commit()

                    count += 1
                    if count % 100 == 0:
                        logger.info(f"Loaded {count} characters...")

                except Exception as e:
                    logger.error(f"Error loading character {char.get('name')}: {e}")
                    conn.rollback()

            logger.info(f"Finished loading {count} characters")
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Load characters from JSON to PostgreSQL")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print characters without loading")
    args = parser.parse_args()

    loader = CharactersLoader(data_dir=args.data_dir)
    loader.load(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
