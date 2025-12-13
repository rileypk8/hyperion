#!/usr/bin/env python3
"""Orchestrator for batch loading all reference data to PostgreSQL."""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_all(data_dir: str = None, dry_run: bool = False, skip: list[str] = None):
    """
    Load all reference data in the correct order.

    Order matters:
    1. Films - creates studios, franchises, media records
    2. Games - creates additional franchises, media records
    3. Characters - depends on media for appearances
    4. Soundtracks - depends on films for source_media_id
    5. Awards - depends on films and soundtracks
    """
    skip = skip or []
    results = {}
    start_time = time.time()

    # Import loaders
    from load_films import FilmsLoader
    from load_games import GamesLoader
    from load_characters import CharactersLoader
    from load_soundtracks import SoundtracksLoader
    from load_awards import AwardsLoader

    loaders = [
        ("films", FilmsLoader),
        ("games", GamesLoader),
        ("characters", CharactersLoader),
        ("soundtracks", SoundtracksLoader),
        ("awards", AwardsLoader),
    ]

    for name, LoaderClass in loaders:
        if name in skip:
            logger.info(f"Skipping {name}...")
            continue

        logger.info(f"\n{'='*50}")
        logger.info(f"Loading {name}...")
        logger.info(f"{'='*50}")

        try:
            loader = LoaderClass(data_dir=data_dir)
            count = loader.load(dry_run=dry_run)
            results[name] = count
            logger.info(f"Completed {name}: {count} records")
        except Exception as e:
            logger.error(f"Failed to load {name}: {e}")
            results[name] = f"ERROR: {e}"

    elapsed = time.time() - start_time

    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("BATCH LOAD SUMMARY")
    logger.info(f"{'='*50}")
    for name, count in results.items():
        status = f"{count} records" if isinstance(count, int) else count
        logger.info(f"  {name}: {status}")
    logger.info(f"\nTotal time: {elapsed:.1f}s")
    logger.info(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Load all reference data from JSON to PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load all data
  python load_all.py

  # Dry run to see what would be loaded
  python load_all.py --dry-run

  # Skip certain loaders
  python load_all.py --skip characters --skip awards

  # Use custom data directory
  python load_all.py --data-dir /path/to/data
        """
    )
    parser.add_argument("--data-dir", help="Path to data directory")
    parser.add_argument("--dry-run", action="store_true", help="Print records without loading")
    parser.add_argument("--skip", action="append", default=[],
                        choices=["films", "games", "characters", "soundtracks", "awards"],
                        help="Skip specific loaders")

    args = parser.parse_args()
    load_all(data_dir=args.data_dir, dry_run=args.dry_run, skip=args.skip)


if __name__ == "__main__":
    main()
