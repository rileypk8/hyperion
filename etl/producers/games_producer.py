"""Producer for game records from JSON source files."""

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


class GamesProducer:
    """
    Extracts game data from JSON files and publishes to Kafka.

    Parses game titles, platforms, developers from disney_interactive_characters
    and kingdom_hearts_characters directories.
    """

    # Pattern to extract title and year from strings like "Epic Mickey (2010)"
    GAME_PATTERN = re.compile(r"^(.+?)\s*\((\d{4})\)$")

    # Directories containing game data
    GAME_DIRS = [
        "disney_interactive_characters",
        "kingdom_hearts_characters",
    ]

    # Platform normalization map
    PLATFORM_MAP = {
        "playstation 2": "PlayStation 2",
        "ps2": "PlayStation 2",
        "playstation 3": "PlayStation 3",
        "ps3": "PlayStation 3",
        "playstation 4": "PlayStation 4",
        "ps4": "PlayStation 4",
        "playstation 5": "PlayStation 5",
        "ps5": "PlayStation 5",
        "playstation portable": "PlayStation Portable",
        "psp": "PlayStation Portable",
        "xbox": "Xbox",
        "xbox 360": "Xbox 360",
        "xbox one": "Xbox One",
        "xbox series x/s": "Xbox Series X/S",
        "nintendo ds": "Nintendo DS",
        "ds": "Nintendo DS",
        "nintendo 3ds": "Nintendo 3DS",
        "3ds": "Nintendo 3DS",
        "nintendo switch": "Nintendo Switch",
        "switch": "Nintendo Switch",
        "game boy advance": "Game Boy Advance",
        "gba": "Game Boy Advance",
        "wii": "Nintendo Wii",
        "wii u": "Nintendo Wii U",
        "pc": "PC",
        "ios": "iOS",
        "android": "Android",
        "mobile": "iOS",  # Default mobile to iOS
    }

    def __init__(self, data_dir: str = None):
        self.data_dir = Path(data_dir or settings.data_dir)
        self.seen_games = set()  # Track (title, year) to avoid duplicates

    def parse_game_string(self, game_str: str) -> tuple[str, int] | None:
        """Parse a game string like 'Epic Mickey (2010)' into (title, year)."""
        match = self.GAME_PATTERN.match(game_str.strip())
        if match:
            return match.group(1).strip(), int(match.group(2))
        return None

    def normalize_platforms(self, platform_str: str) -> list[str]:
        """Parse and normalize platform string to list of standard platform names."""
        if not platform_str:
            return []

        # Split on common delimiters
        parts = re.split(r"[/,]", platform_str)
        platforms = []

        for part in parts:
            part = part.strip().lower()
            if part in self.PLATFORM_MAP:
                platforms.append(self.PLATFORM_MAP[part])
            elif part:
                # Keep original if not in map, with title case
                platforms.append(part.title())

        return list(set(platforms))  # Deduplicate

    def extract_games_from_file(self, file_path: Path) -> Iterator[dict]:
        """Extract game records from a single JSON file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse {file_path}: {e}")
            return

        source_dir = file_path.parent.name
        source_file = f"{source_dir}/{file_path.name}"

        # Skip if not in game directories
        if source_dir not in self.GAME_DIRS:
            return

        franchise = data.get("franchise", "Unknown")
        studio = data.get("studio", "Disney Interactive Studios")

        # Extract publisher from studio (often "Publisher / Developer" format)
        publisher = studio.split("/")[0].strip() if "/" in studio else studio
        default_developer = studio.split("/")[-1].strip() if "/" in studio else None

        # Get games list
        games_list = data.get("games", [])

        # Also check categories for detailed game info
        categories = data.get("categories", {})

        for game_str in games_list:
            parsed = self.parse_game_string(game_str)
            if not parsed:
                logger.warning(f"Could not parse game string: {game_str}")
                continue

            title, year = parsed
            game_key = (title.lower(), year)

            if game_key in self.seen_games:
                continue
            self.seen_games.add(game_key)

            # Try to find detailed info in categories
            developer = default_developer
            platforms = []

            for cat_name, cat_data in categories.items():
                if not isinstance(cat_data, dict):
                    continue

                cat_year = cat_data.get("year")
                if cat_year == year:
                    developer = cat_data.get("developer", developer)
                    platform_str = cat_data.get("platform", "")
                    platforms = self.normalize_platforms(platform_str)
                    break

            yield {
                "title": title,
                "year": year,
                "franchise": franchise,
                "developer": developer,
                "publisher": publisher,
                "platforms": platforms,
                "source_file": source_file,
            }

    def scan_all_files(self) -> Iterator[dict]:
        """Scan all JSON files in game directories."""
        for game_dir in self.GAME_DIRS:
            dir_path = self.data_dir / game_dir
            if not dir_path.exists():
                logger.warning(f"Game directory not found: {dir_path}")
                continue

            for json_file in dir_path.glob("*.json"):
                yield from self.extract_games_from_file(json_file)

    def produce(self):
        """Produce all game records to Kafka."""
        with JSONProducer("raw-games") as producer:
            count = 0
            for game in self.scan_all_files():
                key = f"{game['title']}-{game['year']}"
                producer.produce(key, game)
                count += 1

                if count % 20 == 0:
                    logger.info(f"Produced {count} games...")
                    producer.producer.poll(0)

            producer.flush()
            logger.info(f"Finished producing {count} game records")
            return count


def main():
    """CLI entry point for the games producer."""
    import argparse

    parser = argparse.ArgumentParser(description="Produce game records to Kafka")
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print records without producing")
    args = parser.parse_args()

    producer = GamesProducer(data_dir=args.data_dir)

    if args.dry_run:
        print("=== DRY RUN - Games to be produced ===\n")
        count = 0
        for game in producer.scan_all_files():
            platforms = ", ".join(game["platforms"]) if game["platforms"] else "Unknown"
            print(f"{game['title']} ({game['year']}) - {game['developer']} - [{platforms}]")
            count += 1
        print(f"\nTotal: {count} games")
    else:
        producer.produce()


if __name__ == "__main__":
    main()
