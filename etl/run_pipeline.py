#!/usr/bin/env python3
"""
Hyperion ETL Pipeline Orchestrator

Hybrid approach:
- Batch loaders for reference data (direct Python -> PostgreSQL)
- Kafka streaming for revenue data (real-time updates)

Usage:
    # Load all reference data (batch mode)
    python run_pipeline.py --batch

    # Start revenue streaming (requires Kafka)
    python run_pipeline.py --stream

    # Full pipeline: batch first, then stream
    python run_pipeline.py --full

    # Dry run to see what would be loaded
    python run_pipeline.py --dry-run
"""

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_batch_loaders(data_dir: str = None, dry_run: bool = False, skip: list[str] = None):
    """
    Run batch loaders for all reference data.

    Order: films -> games -> characters -> soundtracks -> awards
    """
    logger.info("=" * 60)
    logger.info("BATCH LOADING REFERENCE DATA")
    logger.info("=" * 60)

    from batch.load_films import FilmsLoader
    from batch.load_games import GamesLoader
    from batch.load_characters import CharactersLoader
    from batch.load_soundtracks import SoundtracksLoader
    from batch.load_awards import AwardsLoader

    skip = skip or []
    results = {}

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

        logger.info(f"\n--- Loading {name} ---")
        try:
            loader = LoaderClass(data_dir=data_dir)
            count = loader.load(dry_run=dry_run)
            results[name] = count
        except Exception as e:
            logger.error(f"Failed to load {name}: {e}")
            results[name] = f"ERROR: {e}"

    return results


def check_kafka_available() -> bool:
    """Check if Kafka is reachable."""
    try:
        from confluent_kafka import Producer
        from shared.config import settings

        p = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        # Try to get metadata - this will fail if Kafka isn't running
        p.list_topics(timeout=5)
        return True
    except Exception as e:
        logger.warning(f"Kafka not available: {e}")
        return False


def start_streaming(continuous: bool = False):
    """
    Start revenue streaming (producer and consumer).

    Requires Kafka to be running.
    """
    logger.info("=" * 60)
    logger.info("STARTING REVENUE STREAMING")
    logger.info("=" * 60)

    if not check_kafka_available():
        logger.error("Kafka is not available. Start it with:")
        logger.error("  cd etl/streaming && docker-compose up -d")
        return False

    from streaming.revenue_producer import RevenueProducer
    from streaming.revenue_consumer import RevenueConsumer

    # Load films for producer
    producer = RevenueProducer()
    producer.load_active_films()

    if not producer.films:
        logger.error("No films found in database. Run batch loaders first.")
        return False

    if continuous:
        # In continuous mode, run producer and consumer in separate threads
        import threading

        consumer = RevenueConsumer()

        consumer_thread = threading.Thread(
            target=consumer.consume,
            daemon=True
        )
        consumer_thread.start()

        # Give consumer time to start
        time.sleep(2)

        try:
            producer.run_continuous(interval_seconds=30)
        except KeyboardInterrupt:
            logger.info("Stopping streaming...")
            consumer.stop()
    else:
        # Single batch mode
        producer.produce_batch()

        # Consume what was produced
        consumer = RevenueConsumer()
        consumer.consume(max_messages=len(producer.films) * len(producer.STATE_WEIGHTS))

    return True


def print_usage():
    """Print quick usage guide."""
    print("""
Hyperion ETL Pipeline
=====================

Quick Start:
  1. Start PostgreSQL and run schema:
     psql -f docs/hyperion_schema.sql

  2. Load reference data (no Kafka needed):
     python run_pipeline.py --batch

  3. (Optional) Start Kafka for streaming:
     cd streaming && docker-compose up -d

  4. (Optional) Start revenue streaming:
     python run_pipeline.py --stream

Directory Structure:
  batch/          Direct Python loaders (no Kafka)
  streaming/      Kafka-based revenue streaming
  shared/         Common utilities

Environment Variables:
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
  KAFKA_BOOTSTRAP_SERVERS (only for streaming)
""")


def main():
    parser = argparse.ArgumentParser(
        description="Hyperion ETL Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --batch              # Load all reference data
  python run_pipeline.py --batch --dry-run    # Preview what would be loaded
  python run_pipeline.py --stream             # Start revenue streaming
  python run_pipeline.py --full               # Batch + streaming
        """
    )
    parser.add_argument(
        "--data-dir",
        default="../data",
        help="Path to data directory (default: ../data)"
    )
    parser.add_argument(
        "--batch",
        action="store_true",
        help="Run batch loaders for reference data"
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Start revenue streaming (requires Kafka)"
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run batch loaders then start streaming"
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run streaming continuously"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without loading"
    )
    parser.add_argument(
        "--skip",
        action="append",
        default=[],
        choices=["films", "games", "characters", "soundtracks", "awards"],
        help="Skip specific batch loaders"
    )
    parser.add_argument(
        "--help-usage",
        action="store_true",
        help="Show detailed usage guide"
    )

    args = parser.parse_args()

    if args.help_usage:
        print_usage()
        return

    if not any([args.batch, args.stream, args.full]):
        parser.print_help()
        print("\nRun with --batch, --stream, or --full to start the pipeline.")
        return

    start_time = time.time()
    batch_results = {}

    # Run batch loaders
    if args.batch or args.full:
        batch_results = run_batch_loaders(
            data_dir=args.data_dir,
            dry_run=args.dry_run,
            skip=args.skip
        )

    # Run streaming
    if args.stream or args.full:
        if not args.dry_run:
            time.sleep(2)  # Brief pause between batch and stream
            start_streaming(continuous=args.continuous)
        else:
            logger.info("\n--- Streaming (dry-run) ---")
            logger.info("Would start revenue producer and consumer")

    elapsed = time.time() - start_time

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)

    if batch_results:
        logger.info("\nBatch Results:")
        for key, val in batch_results.items():
            status = f"{val} records" if isinstance(val, int) else val
            logger.info(f"  {key}: {status}")

    logger.info(f"\nTotal time: {elapsed:.1f}s")
    logger.info(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")


if __name__ == "__main__":
    main()
