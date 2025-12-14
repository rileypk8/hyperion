#!/usr/bin/env python3
"""
Kafka producer for film data.

Reads character JSON files, extracts film information, and publishes
to the raw-films Kafka topic for downstream processing.
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


class FilmsProducer:
    """
    Produces film records to Kafka from source JSON files.

    Extracts film metadata from character JSON files and publishes
    deduplicated records to the raw-films topic.
    """

    FILM_PATTERN = re.compile(r"^(.+?)\s*\((\d{4})\)$")

    DIR_TO_STUDIO = {
        "pixar_characters": "Pixar Animation Studios",
        "wdas_characters": "Walt Disney Animation Studios",
        "blue_sky_characters": "Blue Sky Studios",
        "disneytoon_characters": "DisneyToon Studios",
        "marvel_animation_characters": "Marvel Animation",
        "20th_century_animation": "20th Century Animation",
        "fox_disney_plus_characters": "20th Century Animation",
    }

    STUDIO_METADATA = {
        "Pixar Animation Studios": {"founded": 1986, "closed": None, "status": "active"},
        "Walt Disney Animation Studios": {"founded": 1937, "closed": None, "status": "active"},
        "Blue Sky Studios": {"founded": 1987, "closed": 2021, "status": "closed"},
        "DisneyToon Studios": {"founded": 1988, "closed": 2018, "status": "closed"},
        "Marvel Animation": {"founded": 2008, "closed": None, "status": "active"},
        "20th Century Animation": {"founded": 2020, "closed": None, "status": "active"},
    }

    def __init__(self, topic: str = "raw-films"):
        self.topic = topic
        self.data_dir = Path(settings.data_dir)
        self.producer = Producer(settings.kafka_config)
        self.seen_films = set()
        self.produced_count = 0

    def parse_film_string(self, film_str: str) -> tuple[str, int] | None:
        """Parse 'Title (Year)' string into components."""
        match = self.FILM_PATTERN.match(film_str.strip())
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None

    def determine_studio(self, json_data: dict, source_dir: str) -> str | None:
        """Determine studio from JSON data or directory name."""
        if "studio" in json_data:
            studio_name = json_data["studio"].lower()
            if studio_name in ["pixar", "pixar animation"]:
                return "Pixar Animation Studios"
            if studio_name in ["disney", "wdas", "walt disney"]:
                return "Walt Disney Animation Studios"
            if "blue sky" in studio_name:
                return "Blue Sky Studios"
            if "disneytoon" in studio_name:
                return "DisneyToon Studios"
            if "marvel" in studio_name:
                return "Marvel Animation"
            if "20th century" in studio_name or "fox" in studio_name:
                return "20th Century Animation"
            return json_data["studio"]
        return self.DIR_TO_STUDIO.get(source_dir)

    def determine_animation_type(self, json_data: dict, title: str) -> str:
        """Determine animation type from JSON data or title."""
        if "animation_type" in json_data:
            return json_data["animation_type"]

        hybrid_keywords = [
            "enchanted", "mary poppins", "song of the south",
            "bedknobs and broomsticks", "pete's dragon",
            "who framed roger rabbit", "james and the giant peach",
        ]
        title_lower = title.lower()
        if any(kw in title_lower for kw in hybrid_keywords):
            return "hybrid"
        return "fully_animated"

    def extract_films(self) -> Iterator[dict]:
        """Extract film records from JSON files."""
        skip_dirs = {"kingdom_hearts_characters", "disney_interactive_characters"}

        for json_file in self.data_dir.rglob("*.json"):
            source_dir = json_file.parent.name
            source_file = json_file.name

            # Skip non-film files
            if source_file.startswith("00_") or "summary" in source_file.lower():
                continue
            if source_dir in skip_dirs:
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

            studio = self.determine_studio(data, source_dir)
            if not studio:
                continue

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

                studio_meta = self.STUDIO_METADATA.get(studio, {})

                yield {
                    "title": title,
                    "year": year,
                    "studio": studio,
                    "studio_founded": studio_meta.get("founded"),
                    "studio_closed": studio_meta.get("closed"),
                    "studio_status": studio_meta.get("status", "unknown"),
                    "franchise": franchise,
                    "animation_type": self.determine_animation_type(data, title),
                    "source_file": str(json_file.relative_to(self.data_dir)),
                    "extracted_at": datetime.utcnow().isoformat(),
                }

    def delivery_callback(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error(f"Delivery failed for {msg.key()}: {err}")
        else:
            self.produced_count += 1

    def produce(self) -> int:
        """Produce all films to Kafka."""
        logger.info(f"Starting film extraction from {self.data_dir}")
        count = 0

        for film in self.extract_films():
            key = f"{film['title'].lower().replace(' ', '_')}_{film['year']}"
            value = json.dumps(film).encode("utf-8")

            self.producer.produce(
                self.topic,
                key=key.encode("utf-8"),
                value=value,
                callback=self.delivery_callback,
            )
            count += 1

            if count % 50 == 0:
                self.producer.poll(0)
                logger.info(f"Queued {count} films...")

        # Flush remaining messages
        logger.info("Flushing producer...")
        self.producer.flush()

        logger.info(f"Produced {self.produced_count} films to topic '{self.topic}'")
        return self.produced_count


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Produce film data to Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Produce all films to default topic
  python films_producer.py

  # Produce to custom topic
  python films_producer.py --topic my-films-topic

  # Dry run - just print what would be produced
  python films_producer.py --dry-run
        """
    )
    parser.add_argument("--topic", default="raw-films", help="Kafka topic name")
    parser.add_argument("--dry-run", action="store_true", help="Print films without producing")

    args = parser.parse_args()

    producer = FilmsProducer(topic=args.topic)

    if args.dry_run:
        for film in producer.extract_films():
            print(json.dumps(film, indent=2))
        print(f"\nTotal: {len(producer.seen_films)} films")
    else:
        producer.produce()


if __name__ == "__main__":
    main()
