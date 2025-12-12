"""Producer for soundtrack records from JSON source files."""

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


class SoundtracksProducer:
    """
    Extracts soundtrack and song data from wdas_soundtracks_complete.json
    and publishes to Kafka.

    Produces records containing:
    - Soundtrack metadata (title, label, source film)
    - Songs with track numbers
    - Credits (composers, performers)
    """

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

    def extract_soundtracks(self) -> Iterator[dict]:
        """Extract soundtrack records with songs and credits."""
        data = self.load_data()

        for film_entry in data:
            film_title = film_entry.get("film")
            film_year = film_entry.get("film_year")

            if not film_title or not film_year:
                continue

            releases = film_entry.get("releases", [])

            for release in releases:
                soundtrack_title = release.get("title")
                if not soundtrack_title:
                    continue

                # Extract composers as credits
                composers = release.get("composers", [])
                conductor = release.get("conductor")
                orchestra = release.get("orchestra")
                label = release.get("label")

                # Build songs list with track numbers
                tracks = release.get("tracks", [])
                songs = []

                for idx, track in enumerate(tracks, start=1):
                    song_title = track.get("title")
                    if not song_title:
                        continue

                    # Extract performers for this song
                    performers = []
                    for perf in track.get("performers", []):
                        performers.append({
                            "name": perf.get("name"),
                            "type": perf.get("type", "performer"),
                            "character": perf.get("character"),
                        })

                    # Track-specific composer (for classical pieces)
                    track_composer = track.get("composer")

                    songs.append({
                        "title": song_title,
                        "track_number": idx,
                        "type": track.get("type", "song"),
                        "composer": track_composer,
                        "performers": performers,
                    })

                # Build credits list
                credits = []

                # Add composers
                for composer in composers:
                    if composer and composer != "Various Classical":
                        credits.append({
                            "name": composer,
                            "type": "composer",
                        })

                # Add conductor
                if conductor:
                    credits.append({
                        "name": conductor,
                        "type": "conductor",
                    })

                yield {
                    "title": soundtrack_title,
                    "film_title": film_title,
                    "film_year": film_year,
                    "label": label,
                    "orchestra": orchestra,
                    "songs": songs,
                    "credits": credits,
                    "notes": release.get("notes"),
                }

    def produce(self):
        """Produce all soundtrack records to Kafka."""
        with JSONProducer("raw-soundtracks") as producer:
            count = 0
            song_count = 0

            for soundtrack in self.extract_soundtracks():
                key = f"{soundtrack['film_title']}-{soundtrack['film_year']}"
                producer.produce(key, soundtrack)
                count += 1
                song_count += len(soundtrack.get("songs", []))

                if count % 20 == 0:
                    logger.info(f"Produced {count} soundtracks ({song_count} songs)...")
                    producer.producer.poll(0)

            producer.flush()
            logger.info(f"Finished producing {count} soundtracks with {song_count} songs")
            return count


def main():
    """CLI entry point for the soundtracks producer."""
    import argparse

    parser = argparse.ArgumentParser(description="Produce soundtrack records to Kafka")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print records without producing")
    args = parser.parse_args()

    producer = SoundtracksProducer(data_dir=args.data_dir)

    if args.dry_run:
        print("=== DRY RUN - Soundtracks to be produced ===\n")
        count = 0
        song_count = 0
        for st in producer.extract_soundtracks():
            songs = len(st.get("songs", []))
            credits = len(st.get("credits", []))
            print(f"{st['title']}")
            print(f"  Film: {st['film_title']} ({st['film_year']})")
            print(f"  Songs: {songs}, Credits: {credits}")
            count += 1
            song_count += songs
            if count >= 20:
                print("  ... (showing first 20)")
                break
        print(f"\nTotal: {count}+ soundtracks, {song_count}+ songs")
    else:
        producer.produce()


if __name__ == "__main__":
    main()
