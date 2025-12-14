#!/usr/bin/env python3
"""
Kafka producer for character data.

Reads character JSON files and publishes character records
to the raw-characters Kafka topic.
"""

import json
import logging
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Iterator

from config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

try:
    from confluent_kafka import Producer
except ImportError:
    logger.error("confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)


class CharactersProducer:
    """
    Produces character records to Kafka from source JSON files.

    Extracts character data with their film appearances, voice actors,
    and other metadata.
    """

    FILM_PATTERN = re.compile(r"^(.+?)\s*\((\d{4})\)$")

    # Normalize common role variations
    ROLE_MAPPINGS = {
        "main protagonist": "protagonist",
        "main antagonist": "antagonist",
        "primary antagonist": "antagonist",
        "secondary antagonist": "antagonist",
        "main villain": "villain",
        "primary villain": "villain",
        "secondary villain": "villain",
        "main hero": "hero",
        "main character": "protagonist",
        "lead": "protagonist",
        "co-protagonist": "deuteragonist",
        "co-lead": "deuteragonist",
        "secondary protagonist": "deuteragonist",
        "comedic relief": "comic_relief",
        "comedy relief": "comic_relief",
        "romantic interest": "love_interest",
        "romantic lead": "love_interest",
        "side character": "supporting",
        "recurring": "supporting",
        "cameo": "minor",
        "background": "minor",
        "extra": "minor",
    }

    # Normalize gender values
    GENDER_MAPPINGS = {
        "m": "male",
        "f": "female",
        "man": "male",
        "woman": "female",
        "boy": "male",
        "girl": "female",
        "nb": "non-binary",
        "nonbinary": "non-binary",
        "none": "n/a",
        "unknown": "n/a",
        "varies": "various",
        "multiple": "various",
    }

    def __init__(self, topic: str = "raw-characters"):
        self.topic = topic
        self.data_dir = Path(settings.data_dir)
        self.producer = Producer(settings.kafka_config)
        self.produced_count = 0

    def normalize_role(self, role: str) -> str:
        """Normalize role to canonical value."""
        if not role:
            return "supporting"
        role_lower = role.lower().strip()
        return self.ROLE_MAPPINGS.get(role_lower, role_lower)

    def normalize_gender(self, gender: str) -> str:
        """Normalize gender to canonical value."""
        if not gender:
            return "n/a"
        gender_lower = gender.lower().strip()
        return self.GENDER_MAPPINGS.get(gender_lower, gender_lower)

    def parse_film_string(self, film_str: str) -> tuple[str, int] | None:
        """Parse 'Title (Year)' string into components."""
        match = self.FILM_PATTERN.match(film_str.strip())
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None

    def extract_characters(self) -> Iterator[dict]:
        """Extract character records from JSON files."""
        for json_file in self.data_dir.rglob("*.json"):
            source_dir = json_file.parent.name
            source_file = json_file.name

            # Skip summary and soundtrack files
            if source_file.startswith("00_") or "summary" in source_file.lower():
                continue
            if "soundtracks" in source_file.lower():
                continue

            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse {json_file}: {e}")
                continue

            if not isinstance(data, dict):
                continue

            franchise = data.get("franchise", "Unknown")
            films = data.get("films", [])
            categories = data.get("categories", {})

            # Handle files with categories structure
            if isinstance(categories, dict):
                for category_key, category_data in categories.items():
                    if not isinstance(category_data, dict):
                        continue

                    category_year = category_data.get("year")
                    characters = category_data.get("characters", [])

                    for char in characters:
                        if not isinstance(char, dict):
                            continue

                        yield self._build_character_record(
                            char=char,
                            franchise=franchise,
                            films=films,
                            category_year=category_year,
                            source_dir=source_dir,
                            source_file=source_file,
                        )

            # Handle files with direct characters list
            direct_characters = data.get("characters", [])
            for char in direct_characters:
                if not isinstance(char, dict):
                    continue

                yield self._build_character_record(
                    char=char,
                    franchise=franchise,
                    films=films,
                    category_year=None,
                    source_dir=source_dir,
                    source_file=source_file,
                )

    def _build_character_record(
        self,
        char: dict,
        franchise: str,
        films: list,
        category_year: int | None,
        source_dir: str,
        source_file: str,
    ) -> dict:
        """Build a character record from raw data."""
        name = char.get("name", "Unknown")

        # Parse film appearances
        appearances = []
        for film_str in films:
            parsed = self.parse_film_string(film_str)
            if parsed:
                appearances.append({"title": parsed[0], "year": parsed[1]})

        return {
            "name": name,
            "franchise": franchise,
            "role": self.normalize_role(char.get("role")),
            "gender": self.normalize_gender(char.get("gender")),
            "species": char.get("species", "unknown"),
            "voice_actor": char.get("voice_actor"),
            "first_appearance_year": category_year,
            "appearances": appearances,
            "source_dir": source_dir,
            "source_file": source_file,
            "extracted_at": datetime.utcnow().isoformat(),
        }

    def delivery_callback(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error(f"Delivery failed for {msg.key()}: {err}")
        else:
            self.produced_count += 1

    def produce(self) -> int:
        """Produce all characters to Kafka."""
        logger.info(f"Starting character extraction from {self.data_dir}")
        count = 0

        for character in self.extract_characters():
            # Create unique key from name + franchise
            key = f"{character['name'].lower().replace(' ', '_')}_{character['franchise'].lower().replace(' ', '_')}"
            value = json.dumps(character).encode("utf-8")

            self.producer.produce(
                self.topic,
                key=key.encode("utf-8"),
                value=value,
                callback=self.delivery_callback,
            )
            count += 1

            if count % 100 == 0:
                self.producer.poll(0)
                logger.info(f"Queued {count} characters...")

        # Flush remaining messages
        logger.info("Flushing producer...")
        self.producer.flush()

        logger.info(f"Produced {self.produced_count} characters to topic '{self.topic}'")
        return self.produced_count


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Produce character data to Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Produce all characters to default topic
  python characters_producer.py

  # Produce to custom topic
  python characters_producer.py --topic my-characters-topic

  # Dry run - just print what would be produced
  python characters_producer.py --dry-run
        """
    )
    parser.add_argument("--topic", default="raw-characters", help="Kafka topic name")
    parser.add_argument("--dry-run", action="store_true", help="Print characters without producing")
    parser.add_argument("--limit", type=int, help="Limit number of characters (for testing)")

    args = parser.parse_args()

    producer = CharactersProducer(topic=args.topic)

    if args.dry_run:
        count = 0
        for char in producer.extract_characters():
            print(json.dumps(char, indent=2))
            count += 1
            if args.limit and count >= args.limit:
                break
        print(f"\nTotal: {count} characters")
    else:
        producer.produce()


if __name__ == "__main__":
    main()
