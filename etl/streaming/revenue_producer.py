#!/usr/bin/env python3
"""
Streaming producer for box office / revenue data.

Generates synthetic daily revenue events and publishes to Kafka.
Can run continuously to simulate real-time theater reporting.
"""

import json
import logging
import random
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.config import settings
from shared.db import get_db_connection, get_cursor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Try to import kafka - provide helpful error if missing
try:
    from confluent_kafka import Producer
except ImportError:
    logger.error("confluent-kafka not installed. Run: pip install confluent-kafka")
    sys.exit(1)


class RevenueProducer:
    """
    Produces streaming revenue events for films.

    Simulates real-time box office data from theaters:
    - State-level revenue reports
    - Weekend vs weekday patterns
    - Decay over theatrical run
    """

    STATE_WEIGHTS = {
        "CA": 0.118, "TX": 0.087, "FL": 0.065, "NY": 0.059, "PA": 0.039,
        "IL": 0.038, "OH": 0.035, "GA": 0.032, "NC": 0.031, "MI": 0.030,
        "NJ": 0.027, "VA": 0.026, "WA": 0.023, "AZ": 0.022, "MA": 0.021,
        "TN": 0.020, "IN": 0.020, "MO": 0.018, "MD": 0.018, "WI": 0.017,
        "CO": 0.017, "MN": 0.017, "SC": 0.015, "AL": 0.015, "LA": 0.014,
        "KY": 0.013, "OR": 0.013, "OK": 0.012, "CT": 0.011, "UT": 0.010,
    }

    WEEKLY_DECAY = 0.55
    WEEKEND_MULTIPLIER = 2.5

    def __init__(self, topic: str = "revenue-events", seed: int = None):
        self.topic = topic
        self.producer = Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})
        self.films = []

        if seed:
            random.seed(seed)

    def load_active_films(self, days_in_theaters: int = 120):
        """
        Load films from database that are "currently in theaters".

        For simulation, considers films released within the last N days.
        """
        today = date.today()
        cutoff = today - timedelta(days=days_in_theaters)

        with get_db_connection() as conn:
            with get_cursor(conn, dict_cursor=True) as cursor:
                cursor.execute(
                    """
                    SELECT f.id as film_id, m.title, m.year,
                           m.year as release_year
                    FROM films f
                    JOIN media m ON f.media_id = m.id
                    ORDER BY m.year DESC
                    LIMIT 50
                    """
                )
                self.films = [dict(row) for row in cursor.fetchall()]

        logger.info(f"Loaded {len(self.films)} films for revenue simulation")
        return self.films

    def estimate_daily_revenue(self, film: dict, report_date: date) -> float:
        """
        Estimate daily revenue for a film on a given date.

        Uses release year to determine base, applies decay and day-of-week patterns.
        """
        year = film.get("release_year", 2020)

        # Base daily revenue by era (in dollars)
        if year >= 2020:
            base = random.uniform(50000, 500000)
        elif year >= 2010:
            base = random.uniform(100000, 800000)
        elif year >= 2000:
            base = random.uniform(75000, 600000)
        else:
            base = random.uniform(25000, 300000)

        # Simulate theatrical decay (week 1 = full, week 16 = nearly zero)
        # Use day of year as proxy for "days since release"
        days_out = (report_date.timetuple().tm_yday % 112)  # Cycle for simulation
        week = days_out // 7 + 1
        decay = self.WEEKLY_DECAY ** (week - 1) if week > 1 else 1.0

        # Weekend boost
        day_of_week = report_date.weekday()
        if day_of_week >= 4:  # Fri-Sun
            day_multiplier = self.WEEKEND_MULTIPLIER
        else:
            day_multiplier = 1.0

        # Calculate with some noise
        daily = base * decay * day_multiplier * random.uniform(0.7, 1.3)
        return max(daily, 0)

    def generate_event(self, film: dict, report_date: date, state: str) -> dict:
        """Generate a single revenue event."""
        daily_total = self.estimate_daily_revenue(film, report_date)
        state_weight = self.STATE_WEIGHTS.get(state, 0.01) * random.uniform(0.8, 1.2)
        state_revenue = daily_total * state_weight

        return {
            "event_type": "revenue_report",
            "film_id": film["film_id"],
            "film_title": film["title"],
            "film_year": film["year"],
            "report_date": report_date.isoformat(),
            "state_code": state,
            "revenue": round(state_revenue, 2),
            "timestamp": datetime.utcnow().isoformat(),
        }

    def delivery_callback(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")

    def produce_batch(self, report_date: date = None, states: list[str] = None):
        """
        Produce revenue events for all films for a single day.

        Returns count of events produced.
        """
        report_date = report_date or date.today()
        states = states or list(self.STATE_WEIGHTS.keys())

        if not self.films:
            logger.warning("No films loaded - call load_active_films first")
            return 0

        count = 0
        for film in self.films:
            for state in states:
                event = self.generate_event(film, report_date, state)
                key = f"{film['film_id']}-{report_date.isoformat()}-{state}"

                self.producer.produce(
                    self.topic,
                    key=key.encode("utf-8"),
                    value=json.dumps(event).encode("utf-8"),
                    callback=self.delivery_callback,
                )
                count += 1

                if count % 500 == 0:
                    self.producer.poll(0)

        self.producer.flush()
        logger.info(f"Produced {count} revenue events for {report_date}")
        return count

    def run_continuous(self, interval_seconds: int = 60, states: list[str] = None):
        """
        Run continuously, producing events at regular intervals.

        Simulates real-time revenue reporting.
        """
        logger.info(f"Starting continuous revenue producer (interval: {interval_seconds}s)")
        logger.info("Press Ctrl+C to stop")

        report_date = date.today()

        try:
            while True:
                self.produce_batch(report_date=report_date, states=states)

                # Advance date for next batch (simulate next day)
                report_date += timedelta(days=1)

                logger.info(f"Sleeping {interval_seconds}s before next batch...")
                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
            self.producer.flush()


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Stream revenue events to Kafka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Produce single day's events
  python revenue_producer.py --single-batch

  # Run continuously with 30s interval
  python revenue_producer.py --continuous --interval 30

  # Limit to specific states
  python revenue_producer.py --states CA TX NY
        """
    )
    parser.add_argument("--topic", default="revenue-events", help="Kafka topic name")
    parser.add_argument("--single-batch", action="store_true", help="Produce one batch and exit")
    parser.add_argument("--continuous", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between batches")
    parser.add_argument("--states", nargs="+", help="Limit to specific state codes")
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")

    args = parser.parse_args()

    producer = RevenueProducer(topic=args.topic, seed=args.seed)
    producer.load_active_films()

    if args.continuous:
        producer.run_continuous(interval_seconds=args.interval, states=args.states)
    else:
        producer.produce_batch(states=args.states)


if __name__ == "__main__":
    main()
