"""Producer for synthetic box office data."""

import json
import logging
import math
import random
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

sys.path.insert(0, str(Path(__file__).parent.parent))
from shared.kafka_utils import JSONProducer
from shared.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BoxOfficeProducer:
    """
    Generates synthetic daily box office data for films.

    Uses realistic patterns:
    - Opening weekend spike
    - Exponential decay over weeks
    - State population-weighted distribution
    - Weekend vs weekday patterns
    - Holiday bumps
    """

    # US states with relative population weights (approximate)
    STATE_WEIGHTS = {
        "CA": 0.118, "TX": 0.087, "FL": 0.065, "NY": 0.059, "PA": 0.039,
        "IL": 0.038, "OH": 0.035, "GA": 0.032, "NC": 0.031, "MI": 0.030,
        "NJ": 0.027, "VA": 0.026, "WA": 0.023, "AZ": 0.022, "MA": 0.021,
        "TN": 0.020, "IN": 0.020, "MO": 0.018, "MD": 0.018, "WI": 0.017,
        "CO": 0.017, "MN": 0.017, "SC": 0.015, "AL": 0.015, "LA": 0.014,
        "KY": 0.013, "OR": 0.013, "OK": 0.012, "CT": 0.011, "UT": 0.010,
        "IA": 0.010, "NV": 0.009, "AR": 0.009, "MS": 0.009, "KS": 0.009,
        "NM": 0.006, "NE": 0.006, "ID": 0.006, "WV": 0.005, "HI": 0.004,
        "NH": 0.004, "ME": 0.004, "MT": 0.003, "RI": 0.003, "DE": 0.003,
        "SD": 0.003, "ND": 0.002, "AK": 0.002, "VT": 0.002, "WY": 0.002,
        "DC": 0.002,
    }

    # Typical theatrical release length in weeks
    TYPICAL_RUN_WEEKS = 16

    # Revenue decay factor per week (after opening)
    WEEKLY_DECAY = 0.55

    # Weekend multiplier (Fri-Sun vs Mon-Thu)
    WEEKEND_MULTIPLIER = 2.5

    # Base domestic total estimates by era (in millions)
    ERA_TOTALS = {
        (1990, 1999): (50, 400),
        (2000, 2009): (100, 600),
        (2010, 2019): (150, 800),
        (2020, 2030): (100, 500),  # COVID era, streaming impact
    }

    def __init__(self, films_data: list[dict] = None, seed: int = 42):
        """
        Initialize the producer.

        Args:
            films_data: List of film dicts with title/year, or None to load from JSON
            seed: Random seed for reproducibility
        """
        self.films_data = films_data or []
        random.seed(seed)

    def load_films_from_producer(self, data_dir: str = None):
        """Load film data using the FilmsProducer."""
        from .films_producer import FilmsProducer

        producer = FilmsProducer(data_dir=data_dir)
        self.films_data = [f for f in producer.scan_all_files() if f["studio"]]
        logger.info(f"Loaded {len(self.films_data)} films for box office generation")

    def estimate_total_gross(self, year: int) -> float:
        """Estimate total domestic gross based on release year."""
        for (start, end), (low, high) in self.ERA_TOTALS.items():
            if start <= year <= end:
                # Add some variance
                base = random.uniform(low, high)
                # Occasional blockbuster
                if random.random() < 0.1:
                    base *= random.uniform(2, 4)
                return base * 1_000_000  # Convert to dollars

        # Default for older films
        return random.uniform(20, 200) * 1_000_000

    def get_release_date(self, year: int) -> date:
        """Generate a plausible release date for the year."""
        # Common release windows: summer (May-July), holidays (Nov-Dec), spring (March-April)
        windows = [
            (5, 1, 7, 31),   # Summer
            (11, 1, 12, 25),  # Holiday
            (3, 1, 4, 30),   # Spring
        ]

        window = random.choice(windows)
        start = date(year, window[0], window[1])
        end = date(year, window[2], window[3])
        days_range = (end - start).days
        release = start + timedelta(days=random.randint(0, days_range))

        # Adjust to Friday (typical release day)
        days_to_friday = (4 - release.weekday()) % 7
        return release + timedelta(days=days_to_friday)

    def generate_daily_pattern(
        self,
        total_gross: float,
        release_date: date,
        weeks: int = None,
    ) -> Iterator[tuple[date, float, int]]:
        """
        Generate daily revenue following realistic patterns.

        Yields (date, daily_revenue, week_number) tuples.
        """
        weeks = weeks or self.TYPICAL_RUN_WEEKS

        # Calculate opening weekend gross (typically 25-40% of total)
        opening_pct = random.uniform(0.25, 0.40)
        opening_weekend = total_gross * opening_pct
        remaining = total_gross - opening_weekend

        current_date = release_date
        week_num = 1

        for week in range(weeks):
            if week == 0:
                # Opening weekend (Fri-Sun)
                week_gross = opening_weekend
            else:
                # Subsequent weeks decay exponentially
                week_gross = remaining * (1 - self.WEEKLY_DECAY) * (self.WEEKLY_DECAY ** (week - 1))

            # Distribute across days of the week
            for day_offset in range(7):
                current = current_date + timedelta(days=day_offset)
                day_of_week = current.weekday()

                # Weekend (Fri=4, Sat=5, Sun=6) vs weekday
                if day_of_week >= 4:
                    day_weight = self.WEEKEND_MULTIPLIER
                else:
                    day_weight = 1.0

                # Calculate day's share
                total_week_weight = 3 * self.WEEKEND_MULTIPLIER + 4 * 1.0
                day_pct = day_weight / total_week_weight
                daily = week_gross * day_pct

                # Add some random noise
                daily *= random.uniform(0.85, 1.15)

                if daily > 0:
                    yield current, daily, week_num

            current_date += timedelta(days=7)
            week_num += 1

    def distribute_by_state(self, daily_revenue: float) -> Iterator[tuple[str, float]]:
        """
        Distribute daily revenue across states by population weight.

        Yields (state_code, state_revenue) tuples.
        """
        for state, weight in self.STATE_WEIGHTS.items():
            # Add some variance to state distribution
            adjusted_weight = weight * random.uniform(0.8, 1.2)
            state_revenue = daily_revenue * adjusted_weight

            if state_revenue > 0:
                yield state, round(state_revenue, 2)

    def generate_for_film(self, film: dict) -> Iterator[dict]:
        """Generate all box office records for a single film."""
        title = film["title"]
        year = film["year"]

        total_gross = self.estimate_total_gross(year)
        release_date = self.get_release_date(year)

        logger.debug(f"Generating box office for {title} ({year}): ${total_gross:,.0f} total")

        for day_date, daily_revenue, week_num in self.generate_daily_pattern(total_gross, release_date):
            for state_code, state_revenue in self.distribute_by_state(daily_revenue):
                yield {
                    "film_title": title,
                    "film_year": year,
                    "date": day_date.isoformat(),
                    "state_code": state_code,
                    "revenue": state_revenue,
                    "week_num": week_num,
                }

    def scan_all_films(self) -> Iterator[dict]:
        """Generate box office data for all films."""
        for film in self.films_data:
            yield from self.generate_for_film(film)

    def produce(self, limit_films: int = None):
        """Produce all box office records to Kafka."""
        if not self.films_data:
            logger.warning("No films data loaded. Call load_films_from_producer first.")
            return 0

        films_to_process = self.films_data[:limit_films] if limit_films else self.films_data

        with JSONProducer("raw-boxoffice") as producer:
            count = 0
            for film in films_to_process:
                film_count = 0
                for record in self.generate_for_film(film):
                    key = f"{record['film_title']}-{record['date']}-{record['state_code']}"
                    producer.produce(key, record)
                    count += 1
                    film_count += 1

                    if count % 10000 == 0:
                        logger.info(f"Produced {count} box office records...")
                        producer.producer.poll(0)

                logger.info(f"Generated {film_count} records for {film['title']}")

            producer.flush()
            logger.info(f"Finished producing {count} box office records for {len(films_to_process)} films")
            return count


def main():
    """CLI entry point for the box office producer."""
    import argparse

    parser = argparse.ArgumentParser(description="Produce synthetic box office data to Kafka")
    parser.add_argument("--data-dir", help="Path to data directory for loading films")
    parser.add_argument("--dry-run", action="store_true", help="Print stats without producing")
    parser.add_argument("--limit-films", type=int, help="Limit number of films to process")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    producer = BoxOfficeProducer(seed=args.seed)
    producer.load_films_from_producer(data_dir=args.data_dir)

    if args.dry_run:
        films = producer.films_data[:args.limit_films] if args.limit_films else producer.films_data
        print("=== DRY RUN - Box Office Generation Stats ===\n")

        total_records = 0
        for film in films:
            count = sum(1 for _ in producer.generate_for_film(film))
            total_records += count
            print(f"{film['title']} ({film['year']}): ~{count:,} records")

        print(f"\nTotal: {total_records:,} records for {len(films)} films")
        print(f"Estimated size: ~{total_records * 100 / 1024 / 1024:.1f} MB")
    else:
        producer.produce(limit_films=args.limit_films)


if __name__ == "__main__":
    main()
