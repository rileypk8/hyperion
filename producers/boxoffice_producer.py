#!/usr/bin/env python3
"""
Kafka producer for box office revenue data.

Generates synthetic daily box office revenue events and publishes
to the raw-boxoffice Kafka topic. Simulates real-time theater reporting.
"""

import json
import logging
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
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


class BoxOfficeProducer:
    """
    Produces synthetic box office revenue events to Kafka.

    Simulates daily state-level box office reports with realistic patterns:
    - Weekend vs weekday variations
    - Theatrical decay over time
    - State population weighting
    """

    # State weights based on approximate population/theater distribution
    STATE_WEIGHTS = {
        "CA": 0.118, "TX": 0.087, "FL": 0.065, "NY": 0.059, "PA": 0.039,
        "IL": 0.038, "OH": 0.035, "GA": 0.032, "NC": 0.031, "MI": 0.030,
        "NJ": 0.027, "VA": 0.026, "WA": 0.023, "AZ": 0.022, "MA": 0.021,
        "TN": 0.020, "IN": 0.020, "MO": 0.018, "MD": 0.018, "WI": 0.017,
        "CO": 0.017, "MN": 0.017, "SC": 0.015, "AL": 0.015, "LA": 0.014,
        "KY": 0.013, "OR": 0.013, "OK": 0.012, "CT": 0.011, "UT": 0.010,
    }

    # Decay and pattern multipliers
    WEEKLY_DECAY = 0.55  # Revenue drops ~45% each week
    WEEKEND_MULTIPLIER = 2.5  # Weekend days earn 2.5x weekday

    def __init__(self, topic: str = "raw-boxoffice", seed: int = None):
        self.topic = topic
        self.data_dir = Path(settings.data_dir)
        self.producer = Producer(settings.kafka_config)
        self.films = []
        self.produced_count = 0

        if seed is not None:
            random.seed(seed)

    def load_films(self) -> list[dict]:
        """Load film data from JSON files for revenue generation."""
        import re
        film_pattern = re.compile(r"^(.+?)\s*\((\d{4})\)$")
        seen = set()

        for json_file in self.data_dir.rglob("*.json"):
            source_dir = json_file.parent.name
            if source_dir in ["kingdom_hearts_characters", "disney_interactive_characters"]:
                continue
            if "soundtracks" in json_file.name.lower():
                continue

            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError):
                continue

            if not isinstance(data, dict):
                continue

            franchise = data.get("franchise", "Unknown")

            for film_str in data.get("films", []):
                match = film_pattern.match(film_str.strip())
                if not match:
                    continue

                title = match.group(1).strip()
                year = int(match.group(2))
                key = (title.lower(), year)

                if key in seen:
                    continue
                seen.add(key)

                self.films.append({
                    "title": title,
                    "year": year,
                    "franchise": franchise,
                })

        logger.info(f"Loaded {len(self.films)} films for revenue generation")
        return self.films

    def estimate_opening_revenue(self, film: dict) -> float:
        """Estimate opening day domestic revenue based on year."""
        year = film["year"]

        # Base opening revenue by era (inflation-adjusted feel)
        if year >= 2015:
            base = random.uniform(5_000_000, 50_000_000)
        elif year >= 2005:
            base = random.uniform(3_000_000, 40_000_000)
        elif year >= 1995:
            base = random.uniform(2_000_000, 30_000_000)
        elif year >= 1985:
            base = random.uniform(1_000_000, 20_000_000)
        else:
            base = random.uniform(500_000, 10_000_000)

        return base

    def calculate_daily_revenue(
        self,
        opening_revenue: float,
        days_since_release: int,
        day_of_week: int,
    ) -> float:
        """Calculate daily revenue with decay and day-of-week patterns."""
        # Week-over-week decay
        week = days_since_release // 7
        decay = self.WEEKLY_DECAY ** week if week > 0 else 1.0

        # Weekend boost (Friday=4, Saturday=5, Sunday=6)
        if day_of_week >= 4:
            day_multiplier = self.WEEKEND_MULTIPLIER
        else:
            day_multiplier = 1.0

        # Add some randomness
        noise = random.uniform(0.7, 1.3)

        return opening_revenue * decay * day_multiplier * noise / 7

    def generate_events(
        self,
        start_date: date = None,
        days: int = 90,
        states: list[str] = None,
    ) -> Iterator[dict]:
        """Generate box office events for all films over a date range."""
        if not self.films:
            self.load_films()

        start_date = start_date or date(2023, 1, 1)
        states = states or list(self.STATE_WEIGHTS.keys())

        for film in self.films:
            # Simulate release date based on film year
            # Use actual year if recent, otherwise simulate historical release
            film_year = film["year"]
            if film_year >= start_date.year - 1:
                release_date = date(film_year, random.randint(1, 12), random.randint(1, 28))
            else:
                # For older films, generate data as if re-released
                release_date = start_date - timedelta(days=random.randint(0, 30))

            opening_revenue = self.estimate_opening_revenue(film)

            for day_offset in range(days):
                report_date = release_date + timedelta(days=day_offset)
                days_since_release = day_offset
                day_of_week = report_date.weekday()

                # Stop generating after 16 weeks (typical theatrical run)
                if days_since_release > 112:
                    break

                daily_total = self.calculate_daily_revenue(
                    opening_revenue, days_since_release, day_of_week
                )

                # Skip if revenue negligible
                if daily_total < 1000:
                    continue

                # Generate state-level breakdown
                for state in states:
                    state_weight = self.STATE_WEIGHTS.get(state, 0.01)
                    state_weight *= random.uniform(0.8, 1.2)  # Add variance
                    state_revenue = daily_total * state_weight

                    if state_revenue < 100:
                        continue

                    yield {
                        "film_title": film["title"],
                        "film_year": film["year"],
                        "franchise": film["franchise"],
                        "report_date": report_date.isoformat(),
                        "state_code": state,
                        "revenue": round(state_revenue, 2),
                        "days_since_release": days_since_release,
                        "week_number": days_since_release // 7 + 1,
                        "is_weekend": day_of_week >= 4,
                        "generated_at": datetime.utcnow().isoformat(),
                    }

    def delivery_callback(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            self.produced_count += 1

    def produce(
        self,
        start_date: date = None,
        days: int = 90,
        states: list[str] = None,
        limit: int = None,
    ) -> int:
        """Produce box office events to Kafka."""
        logger.info(f"Generating box office data for {days} days")
        count = 0

        for event in self.generate_events(start_date, days, states):
            key = f"{event['film_title']}_{event['report_date']}_{event['state_code']}"
            value = json.dumps(event).encode("utf-8")

            self.producer.produce(
                self.topic,
                key=key.encode("utf-8"),
                value=value,
                callback=self.delivery_callback,
            )
            count += 1

            if count % 1000 == 0:
                self.producer.poll(0)
                logger.info(f"Queued {count} events...")

            if limit and count >= limit:
                break

        logger.info("Flushing producer...")
        self.producer.flush()

        logger.info(f"Produced {self.produced_count} box office events to topic '{self.topic}'")
        return self.produced_count


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Produce box office revenue data to Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 90 days of data for all films
  python boxoffice_producer.py

  # Generate 30 days starting from specific date
  python boxoffice_producer.py --start-date 2023-06-01 --days 30

  # Limit to specific states
  python boxoffice_producer.py --states CA TX NY

  # Dry run with limit
  python boxoffice_producer.py --dry-run --limit 100
        """
    )
    parser.add_argument("--topic", default="raw-boxoffice", help="Kafka topic name")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=90, help="Number of days to generate")
    parser.add_argument("--states", nargs="+", help="Limit to specific state codes")
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    parser.add_argument("--limit", type=int, help="Limit total events")
    parser.add_argument("--dry-run", action="store_true", help="Print events without producing")

    args = parser.parse_args()

    start_date = None
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)

    producer = BoxOfficeProducer(topic=args.topic, seed=args.seed)

    if args.dry_run:
        count = 0
        for event in producer.generate_events(start_date, args.days, args.states):
            print(json.dumps(event))
            count += 1
            if args.limit and count >= args.limit:
                break
        print(f"\nTotal: {count} events", file=sys.stderr)
    else:
        producer.produce(start_date, args.days, args.states, args.limit)


if __name__ == "__main__":
    main()
