"""Producer for character records from JSON source files."""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONProducer
from shared.config import settings
from shared.schemas import ROLE_MAPPING, GENDER_MAPPING, normalize_species

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CharactersProducer:
    """
    Extracts character data from JSON files and publishes to Kafka.

    Handles various JSON structures including nested character categories
    and film-specific character groups.
    """

    # Pattern to extract year from category names like "toy_story_1" or "ice_age_1_characters"
    YEAR_FROM_CATEGORY = {
        "toy_story_1": 1995,
        "toy_story_2": 1999,
        "toy_story_3": 2010,
        "toy_story_4": 2019,
        "ice_age_1": 2002,
        "ice_age_1_characters": 2002,
        "meltdown": 2006,
        "meltdown_characters": 2006,
        "dawn_of_dinosaurs": 2009,
        "dawn_of_dinosaurs_characters": 2009,
        "continental_drift": 2012,
        "continental_drift_characters": 2012,
        "collision_course": 2016,
        "collision_course_characters": 2016,
    }

    # Pattern to extract title and year from film strings
    FILM_PATTERN = re.compile(r"^(.+?)\s*\((\d{4})\)$")

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or settings.data_dir)
        self.seen_characters = set()  # Track to avoid exact duplicates

    def normalize_role(self, role: str) -> str:
        """Normalize role string to match schema enum."""
        if not role:
            return "supporting"
        role_lower = role.lower().strip()
        return ROLE_MAPPING.get(role_lower, "supporting")

    def normalize_gender(self, gender: str) -> str:
        """Normalize gender string to match schema enum."""
        if not gender:
            return "n/a"
        gender_lower = gender.lower().strip()
        return GENDER_MAPPING.get(gender_lower, "n/a")

    def parse_voice_actor(self, voice_actor: str | None) -> list[dict]:
        """
        Parse voice actor string, handling multiple actors and film-specific notes.

        Returns list of dicts with actor name and optional film notes.
        Examples:
            "Tom Hanks" -> [{"name": "Tom Hanks"}]
            "Jim Varney (1-2), Blake Clark (3-4)" -> [{"name": "Jim Varney", "films": "1-2"}, ...]
            "N/A" -> []
        """
        if not voice_actor or voice_actor.lower() in ["n/a", "none", "null", "unknown"]:
            return []

        actors = []

        # Check for comma-separated actors with parenthetical notes
        # Pattern: "Name (note), Name2 (note2)"
        parts = re.split(r",\s*(?=[A-Z])", voice_actor)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Check for parenthetical film indicators like "(1-2)" or "(TS3)"
            paren_match = re.match(r"^(.+?)\s*\(([^)]+)\)$", part)
            if paren_match:
                name = paren_match.group(1).strip()
                note = paren_match.group(2).strip()
                # Skip if the note looks like a non-speaking indicator
                if "non-speaking" in note.lower() or "baby" in note.lower():
                    continue
                actors.append({"name": name, "films": note})
            else:
                actors.append({"name": part})

        return actors

    def extract_film_info_from_category(self, category: str, films_list: list) -> tuple[str, int] | None:
        """
        Try to determine which film a character category belongs to.

        Args:
            category: Category name like "toy_story_3" or "main_trio"
            films_list: List of film strings from the JSON

        Returns:
            (title, year) tuple or None if not determinable
        """
        category_lower = category.lower()

        # Check direct mapping
        if category_lower in self.YEAR_FROM_CATEGORY:
            year = self.YEAR_FROM_CATEGORY[category_lower]
            # Find matching film from films_list
            for film_str in films_list:
                parsed = self.parse_film_string(film_str)
                if parsed and parsed[1] == year:
                    return parsed
            return None

        # Check if category contains a film title pattern
        for film_str in films_list:
            parsed = self.parse_film_string(film_str)
            if parsed:
                # Simplify title for matching
                simple_title = re.sub(r"[^\w]", "_", parsed[0].lower())
                if simple_title in category_lower or category_lower in simple_title:
                    return parsed

        return None

    def parse_film_string(self, film_str: str) -> tuple[str, int] | None:
        """Parse a film string like 'Toy Story (1995)' into (title, year)."""
        match = self.FILM_PATTERN.match(film_str.strip())
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None

    def extract_characters_from_list(
        self,
        characters: list[dict],
        franchise: str,
        source_file: str,
        default_film: tuple[str, int] | None = None,
    ) -> Iterator[dict]:
        """Extract character records from a list of character dicts."""
        for char in characters:
            if not isinstance(char, dict):
                continue

            name = char.get("name")
            if not name:
                continue

            # Create unique key for deduplication
            char_key = (name.lower(), franchise.lower())

            role = self.normalize_role(char.get("role"))
            gender = self.normalize_gender(char.get("gender"))
            species = normalize_species(char.get("species"))
            voice_actor = char.get("voice_actor")
            notes = char.get("notes")

            # Determine film association
            film_title = None
            film_year = None
            if default_film:
                film_title, film_year = default_film

            yield {
                "name": name,
                "franchise": franchise,
                "role": role,
                "species": species,
                "gender": gender,
                "voice_actor": voice_actor,
                "film_title": film_title,
                "film_year": film_year,
                "notes": notes,
                "source_file": source_file,
            }

    def extract_characters_from_file(self, file_path: Path) -> Iterator[dict]:
        """Extract character records from a single JSON file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            return

        source_dir = file_path.parent.name
        source_file = f"{source_dir}/{file_path.name}"

        # Skip summary files
        if file_path.name.startswith("00_") or "summary" in file_path.name.lower():
            return

        franchise = data.get("franchise", "Unknown")
        films_list = data.get("films", [])

        # Handle different JSON structures

        # Structure 1: "characters" as nested dict with categories
        if "characters" in data and isinstance(data["characters"], dict):
            for category, char_list in data["characters"].items():
                if not isinstance(char_list, list):
                    continue

                # Try to determine film for this category
                film_info = self.extract_film_info_from_category(category, films_list)

                yield from self.extract_characters_from_list(
                    char_list, franchise, source_file, film_info
                )

        # Structure 2: "characters" as flat list
        elif "characters" in data and isinstance(data["characters"], list):
            # Use first film as default if only one
            default_film = None
            if len(films_list) == 1:
                default_film = self.parse_film_string(films_list[0])

            yield from self.extract_characters_from_list(
                data["characters"], franchise, source_file, default_film
            )

        # Structure 3: "categories" with nested character groups (Toy Story format)
        elif "categories" in data:
            for category, cat_data in data["categories"].items():
                if not isinstance(cat_data, dict):
                    continue

                char_list = cat_data.get("characters", [])
                if not char_list:
                    continue

                # Check for year in category data
                year = cat_data.get("year")
                film_info = None
                if year:
                    # Find matching film title
                    for film_str in films_list:
                        parsed = self.parse_film_string(film_str)
                        if parsed and parsed[1] == year:
                            film_info = parsed
                            break

                yield from self.extract_characters_from_list(
                    char_list, franchise, source_file, film_info
                )

    def scan_all_files(self) -> Iterator[dict]:
        """Scan all JSON files in the data directory."""
        for json_file in self.data_dir.rglob("*.json"):
            yield from self.extract_characters_from_file(json_file)

    def produce(self):
        """Produce all character records to Kafka."""
        with JSONProducer("raw-characters") as producer:
            count = 0
            for character in self.scan_all_files():
                key = f"{character['franchise']}-{character['name']}"
                producer.produce(key, character)
                count += 1

                if count % 100 == 0:
                    logger.info(f"Produced {count} characters...")
                    producer.producer.poll(0)

            producer.flush()
            logger.info(f"Finished producing {count} character records")
            return count


def main():
    """CLI entry point for the characters producer."""
    import argparse

    parser = argparse.ArgumentParser(description="Produce character records to Kafka")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print records without producing")
    parser.add_argument("--limit", type=int, help="Limit number of records to process")
    args = parser.parse_args()

    producer = CharactersProducer(data_dir=args.data_dir)

    if args.dry_run:
        print("=== DRY RUN - Characters to be produced ===\n")
        count = 0
        for char in producer.scan_all_files():
            film_info = f" in {char['film_title']} ({char['film_year']})" if char['film_title'] else ""
            print(f"{char['name']} ({char['franchise']}){film_info} - {char['role']} - {char['voice_actor']}")
            count += 1
            if args.limit and count >= args.limit:
                break
        print(f"\nTotal: {count} characters")
    else:
        producer.produce()


if __name__ == "__main__":
    main()
