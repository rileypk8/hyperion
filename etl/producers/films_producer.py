"""Producer for film/media records from JSON source files."""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONProducer
from shared.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilmsProducer:
    """
    Extracts film data from character JSON files and publishes to Kafka.

    Parses film titles in format "Title (YYYY)" and extracts metadata
    including studio and franchise information.
    """

    # Pattern to extract title and year from strings like "Toy Story (1995)"
    FILM_PATTERN = re.compile(r"^(.+?)\s*\((\d{4})\)$")

    # Map directory names to studio names
    DIR_TO_STUDIO = {
        "pixar_characters": "Pixar Animation Studios",
        "wdas_characters": "Walt Disney Animation Studios",
        "blue_sky_characters": "Blue Sky Studios",
        "disneytoon_characters": "DisneyToon Studios",
        "kingdom_hearts_characters": None,  # Games, not films
        "disney_interactive_characters": None,  # Games, not films
        "marvel_animation_characters": "Marvel Animation",
        "20th_century_animation": "20th Century Animation",
        "fox_disney_plus_characters": "20th Century Animation",
    }

    # Studio metadata
    STUDIOS = {
        "Pixar Animation Studios": {"start": 1986, "status": "active"},
        "Walt Disney Animation Studios": {"start": 1937, "status": "active"},
        "Blue Sky Studios": {"start": 1987, "end": 2021, "status": "closed"},
        "DisneyToon Studios": {"start": 1988, "end": 2018, "status": "closed"},
        "Marvel Animation": {"start": 2008, "status": "active"},
        "20th Century Animation": {"start": 2020, "status": "active"},
        "Fox Animation Studios": {"start": 1994, "end": 2000, "status": "closed"},
        "Searchlight Pictures": {"start": 1994, "status": "active"},
    }

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or settings.data_dir)
        self.seen_films = set()  # Track (title, year) to avoid duplicates

    def parse_film_string(self, film_str: str) -> tuple[str, int] | None:
        """Parse a film string like 'Toy Story (1995)' into (title, year)."""
        match = self.FILM_PATTERN.match(film_str.strip())
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None

    def determine_studio(self, json_data: dict, source_dir: str) -> str | None:
        """Determine the studio from JSON data or directory name."""
        # First, check if studio is explicitly in the JSON
        if "studio" in json_data:
            studio_name = json_data["studio"]
            # Normalize common variations
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

        # Fall back to directory-based inference
        return self.DIR_TO_STUDIO.get(source_dir)

    def determine_animation_type(self, json_data: dict, title: str) -> str:
        """Determine if a film is fully animated or hybrid."""
        # Check for explicit marking
        if "animation_type" in json_data:
            return json_data["animation_type"]

        # Known hybrid films
        hybrid_films = {
            "enchanted", "mary poppins", "song of the south", "bedknobs and broomsticks",
            "pete's dragon", "who framed roger rabbit", "james and the giant peach",
            "the jungle book (2016)", "christopher robin", "chip 'n dale: rescue rangers",
        }

        if title.lower() in hybrid_films or any(h in title.lower() for h in hybrid_films):
            return "hybrid"

        return "fully_animated"

    def extract_films_from_file(self, file_path: Path) -> Iterator[dict]:
        """Extract film records from a single JSON file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            return

        source_dir = file_path.parent.name
        source_file = file_path.name

        # Skip summary files and non-character files
        if source_file.startswith("00_") or "summary" in source_file.lower():
            return

        # Skip game-only directories
        if source_dir in ["kingdom_hearts_characters", "disney_interactive_characters"]:
            logger.debug(f"Skipping games directory: {source_dir}")
            return

        studio = self.determine_studio(data, source_dir)
        franchise = data.get("franchise", "Unknown")

        # Extract films from the "films" array
        films_list = data.get("films", [])
        for film_str in films_list:
            parsed = self.parse_film_string(film_str)
            if not parsed:
                logger.warning(f"Could not parse film string: {film_str}")
                continue

            title, year = parsed
            film_key = (title.lower(), year)

            # Skip if we've already seen this film
            if film_key in self.seen_films:
                continue

            self.seen_films.add(film_key)

            yield {
                "title": title,
                "year": year,
                "studio": studio,
                "franchise": franchise,
                "animation_type": self.determine_animation_type(data, title),
                "source_file": f"{source_dir}/{source_file}",
            }

    def scan_all_files(self) -> Iterator[dict]:
        """Scan all JSON files in the data directory."""
        for json_file in self.data_dir.rglob("*.json"):
            yield from self.extract_films_from_file(json_file)

    def produce(self):
        """Produce all film records to Kafka."""
        with JSONProducer("raw-films") as producer:
            count = 0
            for film in self.scan_all_files():
                if film["studio"] is None:
                    logger.debug(f"Skipping film without studio: {film['title']}")
                    continue

                key = f"{film['title']}-{film['year']}"
                producer.produce(key, film)
                count += 1

                if count % 50 == 0:
                    logger.info(f"Produced {count} films...")
                    producer.producer.poll(0)  # Trigger delivery callbacks

            producer.flush()
            logger.info(f"Finished producing {count} film records")
            return count


def main():
    """CLI entry point for the films producer."""
    import argparse

    parser = argparse.ArgumentParser(description="Produce film records to Kafka")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print records without producing")
    args = parser.parse_args()

    producer = FilmsProducer(data_dir=args.data_dir)

    if args.dry_run:
        print("=== DRY RUN - Films to be produced ===\n")
        count = 0
        for film in producer.scan_all_files():
            if film["studio"]:
                print(f"{film['title']} ({film['year']}) - {film['studio']} - {film['franchise']}")
                count += 1
        print(f"\nTotal: {count} films")
    else:
        producer.produce()


if __name__ == "__main__":
    main()
