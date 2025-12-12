"""Producer for award nomination records from soundtrack data."""

import json
import logging
import sys
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONProducer
from shared.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AwardsProducer:
    """
    Extracts award nominations from wdas_soundtracks_complete.json
    and publishes to Kafka.

    Awards are embedded in:
    - Track level (song awards like "Best Original Song")
    - Album level (album_awards like "Best Original Score")
    """

    # Map award names to standardized award body names
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

    def load_data(self) -> list[dict]:
        """Load the soundtracks JSON file."""
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
        """Normalize award body name."""
        if not award_name:
            return "Academy Awards"

        award_lower = award_name.lower()
        for key, value in self.AWARD_BODY_MAP.items():
            if key in award_lower:
                return value

        return award_name

    def extract_awards(self) -> Iterator[dict]:
        """Extract award nomination records."""
        data = self.load_data()

        for film_entry in data:
            film_title = film_entry.get("film")
            film_year = film_entry.get("film_year")

            if not film_title or not film_year:
                continue

            releases = film_entry.get("releases", [])

            for release in releases:
                soundtrack_title = release.get("title")

                # Album-level awards (Best Score, etc.)
                for award in release.get("album_awards", []):
                    yield {
                        "film_title": film_title,
                        "film_year": film_year,
                        "award_body": self.normalize_award_body(award.get("award")),
                        "category": award.get("category"),
                        "ceremony_year": award.get("year"),
                        "outcome": award.get("result", "nominated"),
                        "song_title": None,
                        "talent_name": None,
                    }

                # Track-level awards (Best Song, etc.)
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
                            "talent_name": None,  # Could extract from performers
                        }

    def produce(self):
        """Produce all award records to Kafka."""
        with JSONProducer("raw-awards") as producer:
            count = 0

            for award in self.extract_awards():
                key = f"{award['film_title']}-{award['ceremony_year']}-{award['category']}"
                producer.produce(key, award)
                count += 1

                if count % 20 == 0:
                    logger.info(f"Produced {count} award nominations...")
                    producer.producer.poll(0)

            producer.flush()
            logger.info(f"Finished producing {count} award nominations")
            return count


def main():
    """CLI entry point for the awards producer."""
    import argparse

    parser = argparse.ArgumentParser(description="Produce award records to Kafka")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print records without producing")
    args = parser.parse_args()

    producer = AwardsProducer(data_dir=args.data_dir)

    if args.dry_run:
        print("=== DRY RUN - Awards to be produced ===\n")
        wins = 0
        noms = 0
        for award in producer.extract_awards():
            outcome = award["outcome"]
            song_info = f" (Song: {award['song_title']})" if award["song_title"] else ""
            print(f"{award['film_title']} ({award['film_year']}){song_info}")
            print(f"  {award['award_body']} - {award['category']} - {outcome.upper()} ({award['ceremony_year']})")
            if outcome == "won":
                wins += 1
            else:
                noms += 1
        print(f"\nTotal: {wins} wins, {noms} nominations")
    else:
        producer.produce()


if __name__ == "__main__":
    main()
