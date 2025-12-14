#!/usr/bin/env python3
"""
Run all HDFS consumers in parallel.

Starts consumer processes for each topic and manages their lifecycle.
"""

import logging
import multiprocessing
import signal
import sys
import time

from hdfs_consumer import HDFSConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Topic to HDFS path mapping
TOPIC_CONFIG = {
    "raw-films": {
        "hdfs_path": "/raw/films",
        "group_id": "hyperion-films-consumer",
        "batch_size": 50,
    },
    "raw-characters": {
        "hdfs_path": "/raw/characters",
        "group_id": "hyperion-characters-consumer",
        "batch_size": 100,
    },
    "raw-boxoffice": {
        "hdfs_path": "/raw/boxoffice",
        "group_id": "hyperion-boxoffice-consumer",
        "batch_size": 500,
    },
    "raw-soundtracks": {
        "hdfs_path": "/raw/soundtracks",
        "group_id": "hyperion-soundtracks-consumer",
        "batch_size": 50,
    },
    "raw-games": {
        "hdfs_path": "/raw/games",
        "group_id": "hyperion-games-consumer",
        "batch_size": 50,
    },
}


def run_consumer(topic: str, config: dict):
    """Run a single consumer process."""
    logger.info(f"Starting consumer for topic: {topic}")

    consumer = HDFSConsumer(
        topics=[topic],
        hdfs_base_path=config["hdfs_path"],
        batch_size=config.get("batch_size", 100),
        group_id=config.get("group_id"),
    )

    consumer.consume()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Run all HDFS consumers in parallel",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        choices=list(TOPIC_CONFIG.keys()) + ["all"],
        default=["all"],
        help="Topics to consume (default: all)"
    )

    args = parser.parse_args()

    # Determine which topics to run
    if "all" in args.topics:
        topics_to_run = list(TOPIC_CONFIG.keys())
    else:
        topics_to_run = args.topics

    logger.info(f"Starting consumers for topics: {topics_to_run}")

    # Start consumer processes
    processes = []
    for topic in topics_to_run:
        config = TOPIC_CONFIG[topic]
        p = multiprocessing.Process(
            target=run_consumer,
            args=(topic, config),
            name=f"consumer-{topic}",
        )
        p.start()
        processes.append(p)
        time.sleep(0.5)  # Stagger startup

    logger.info(f"Started {len(processes)} consumer processes")

    # Wait for all processes
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Shutting down all consumers...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.join(timeout=5)

    logger.info("All consumers stopped")


if __name__ == "__main__":
    main()
