#!/usr/bin/env python3
"""
Hyperion ETL Pipeline Orchestrator

Runs the complete ETL pipeline:
1. Produce film records to Kafka
2. Produce character records to Kafka
3. Produce box office data to Kafka
4. Consume films -> PostgreSQL
5. Consume characters -> PostgreSQL
6. Consume box office -> PostgreSQL

Usage:
    # Run everything
    python run_pipeline.py

    # Run only producers
    python run_pipeline.py --producers-only

    # Run only consumers
    python run_pipeline.py --consumers-only

    # Dry run (show what would be produced)
    python run_pipeline.py --dry-run
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from producers.films_producer import FilmsProducer
from producers.characters_producer import CharactersProducer
from producers.boxoffice_producer import BoxOfficeProducer
from consumers.films_consumer import FilmsConsumer
from consumers.characters_consumer import CharactersConsumer
from consumers.boxoffice_consumer import BoxOfficeConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_producers(data_dir: str = None, skip_boxoffice: bool = False, limit_films: int = None):
    """Run all producers to publish data to Kafka."""
    logger.info("=" * 60)
    logger.info("PHASE 1: Running Producers")
    logger.info("=" * 60)

    results = {}

    # Films producer
    logger.info("\n--- Films Producer ---")
    films_producer = FilmsProducer(data_dir=data_dir)
    results["films"] = films_producer.produce()
    logger.info(f"Films produced: {results['films']}")

    # Characters producer
    logger.info("\n--- Characters Producer ---")
    chars_producer = CharactersProducer(data_dir=data_dir)
    results["characters"] = chars_producer.produce()
    logger.info(f"Characters produced: {results['characters']}")

    # Box office producer (optional, generates lots of data)
    if not skip_boxoffice:
        logger.info("\n--- Box Office Producer ---")
        bo_producer = BoxOfficeProducer()
        bo_producer.load_films_from_producer(data_dir=data_dir)
        results["boxoffice"] = bo_producer.produce(limit_films=limit_films)
        logger.info(f"Box office records produced: {results['boxoffice']}")
    else:
        logger.info("\n--- Skipping Box Office Producer ---")
        results["boxoffice"] = 0

    return results


def run_consumers(skip_boxoffice: bool = False):
    """Run all consumers to load data from Kafka to PostgreSQL."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Running Consumers")
    logger.info("=" * 60)

    results = {}

    # Films consumer (must run first - other consumers depend on films existing)
    logger.info("\n--- Films Consumer ---")
    films_consumer = FilmsConsumer()
    results["films"] = films_consumer.consume()
    logger.info(f"Films consumed: {results['films']}")

    # Wait a bit for commits to propagate
    time.sleep(1)

    # Characters consumer
    logger.info("\n--- Characters Consumer ---")
    chars_consumer = CharactersConsumer()
    results["characters"] = chars_consumer.consume()
    logger.info(f"Characters consumed: {results['characters']}")

    # Box office consumer
    if not skip_boxoffice:
        logger.info("\n--- Box Office Consumer ---")
        bo_consumer = BoxOfficeConsumer()
        results["boxoffice"] = bo_consumer.consume()
        logger.info(f"Box office records consumed: {results['boxoffice']}")
    else:
        logger.info("\n--- Skipping Box Office Consumer ---")
        results["boxoffice"] = 0

    return results


def dry_run(data_dir: str = None, limit_films: int = 5):
    """Show what would be produced without actually producing."""
    logger.info("=" * 60)
    logger.info("DRY RUN - Showing sample data")
    logger.info("=" * 60)

    # Films
    print("\n--- Sample Films ---")
    films_producer = FilmsProducer(data_dir=data_dir)
    count = 0
    for film in films_producer.scan_all_files():
        if film["studio"]:
            print(f"  {film['title']} ({film['year']}) - {film['studio']} - {film['franchise']}")
            count += 1
            if count >= 10:
                break
    print(f"  ... and more (total: {sum(1 for f in films_producer.scan_all_files() if f['studio'])})")

    # Characters
    print("\n--- Sample Characters ---")
    chars_producer = CharactersProducer(data_dir=data_dir)
    count = 0
    for char in chars_producer.scan_all_files():
        print(f"  {char['name']} ({char['franchise']}) - {char['role']}")
        count += 1
        if count >= 10:
            break
    print(f"  ... and more")

    # Box office estimate
    print("\n--- Box Office Estimate ---")
    bo_producer = BoxOfficeProducer()
    bo_producer.load_films_from_producer(data_dir=data_dir)
    films = bo_producer.films_data[:limit_films]
    total = 0
    for film in films:
        count = sum(1 for _ in bo_producer.generate_for_film(film))
        total += count
        print(f"  {film['title']} ({film['year']}): ~{count:,} records")
    print(f"\n  Estimated total for {limit_films} films: ~{total:,} records")
    print(f"  Full estimate for all films: ~{total * len(bo_producer.films_data) // limit_films:,} records")


def main():
    parser = argparse.ArgumentParser(
        description="Hyperion ETL Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--data-dir",
        default="../data",
        help="Path to data directory (default: ../data)"
    )
    parser.add_argument(
        "--producers-only",
        action="store_true",
        help="Only run producers (publish to Kafka)"
    )
    parser.add_argument(
        "--consumers-only",
        action="store_true",
        help="Only run consumers (load from Kafka to PostgreSQL)"
    )
    parser.add_argument(
        "--skip-boxoffice",
        action="store_true",
        help="Skip box office data (reduces data volume significantly)"
    )
    parser.add_argument(
        "--limit-films",
        type=int,
        help="Limit number of films for box office generation"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be produced without producing"
    )

    args = parser.parse_args()

    if args.dry_run:
        dry_run(data_dir=args.data_dir, limit_films=args.limit_films or 5)
        return

    start_time = time.time()

    producer_results = {}
    consumer_results = {}

    if not args.consumers_only:
        producer_results = run_producers(
            data_dir=args.data_dir,
            skip_boxoffice=args.skip_boxoffice,
            limit_films=args.limit_films
        )

    if not args.producers_only:
        # Give Kafka a moment to stabilize
        if not args.consumers_only:
            logger.info("\nWaiting for Kafka to stabilize...")
            time.sleep(3)

        consumer_results = run_consumers(skip_boxoffice=args.skip_boxoffice)

    elapsed = time.time() - start_time

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)

    if producer_results:
        logger.info("\nProducer Results:")
        for key, val in producer_results.items():
            logger.info(f"  {key}: {val:,} records")

    if consumer_results:
        logger.info("\nConsumer Results:")
        for key, val in consumer_results.items():
            logger.info(f"  {key}: {val:,} records")

    logger.info(f"\nTotal time: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
