#!/usr/bin/env python3
"""
Spark job to load and aggregate box office data.

Reads raw box office JSON from HDFS, aggregates by various dimensions,
and writes Parquet tables for analysis.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import (
    HDFS_RAW_PATH, HDFS_PROCESSED_PATH,
    LOCAL_RAW_PATH, LOCAL_PROCESSED_PATH,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_LOCAL_MASTER
)


def create_spark_session(local=False):
    """Create Spark session."""
    builder = SparkSession.builder.appName("{}-boxoffice".format(SPARK_APP_NAME))

    if local:
        builder = builder.master(SPARK_LOCAL_MASTER)
    else:
        builder = builder.master(SPARK_MASTER)

    return builder.getOrCreate()


def get_paths(local=False):
    """Get input/output paths based on mode."""
    if local:
        return LOCAL_RAW_PATH, LOCAL_PROCESSED_PATH
    return HDFS_RAW_PATH, HDFS_PROCESSED_PATH


def load_boxoffice(spark: SparkSession, raw_path: str, processed_path: str):
    """
    Load and aggregate box office data.

    Creates:
    - box_office_daily: Daily state-level revenue records
    - box_office_weekly: Weekly aggregations
    - box_office_total: Total revenue by film
    """
    print(f"Reading raw box office from {raw_path}/boxoffice/")

    # Read all JSON files from raw boxoffice directory
    raw_boxoffice = spark.read.json(f"{raw_path}/boxoffice/*/*.json")

    print(f"Loaded {raw_boxoffice.count()} raw box office records")

    # =========================================================================
    # BOX OFFICE DAILY TABLE
    # =========================================================================
    print("Creating box_office_daily table...")

    # Read films table for joining
    try:
        films = spark.read.parquet(f"{processed_path}/films")
        media = spark.read.parquet(f"{processed_path}/media")

        # Join films with media to get title/year for matching
        films_with_media = films.join(
            media.select(
                F.col("id").alias("media_id"),
                F.col("title").alias("film_title"),
                F.col("year").alias("film_year")
            ),
            films.media_id == F.col("media_id"),
            "inner"
        ).select("id", "film_title", "film_year")

    except Exception as e:
        print(f"Warning: Could not read films/media tables: {e}")
        print("Run 01_load_films.py first")
        films_with_media = None

    daily = raw_boxoffice.select(
        F.col("film_title"),
        F.col("film_year"),
        F.to_date(F.col("report_date")).alias("report_date"),
        F.col("state_code"),
        F.col("revenue"),
        F.col("week_number"),
        F.col("is_weekend"),
    )

    # Join to get film_id if available
    if films_with_media is not None:
        daily = daily.join(
            films_with_media.select(
                F.col("id").alias("film_id"),
                F.col("film_title").alias("ft"),
                F.col("film_year").alias("fy")
            ),
            (daily.film_title == F.col("ft")) & (daily.film_year == F.col("fy")),
            "left"
        ).drop("ft", "fy")
    else:
        daily = daily.withColumn("film_id", F.lit(None).cast("long"))

    daily = daily.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    daily = daily.select(
        "id", "film_id", "film_title", "film_year",
        "report_date", "state_code", "revenue", "week_number", "is_weekend"
    )

    daily.write.mode("overwrite").parquet(f"{processed_path}/box_office_daily")
    print(f"Wrote {daily.count()} daily records")

    # =========================================================================
    # BOX OFFICE WEEKLY TABLE
    # =========================================================================
    print("Creating box_office_weekly table...")

    weekly = daily.groupBy(
        "film_id", "film_title", "film_year", "week_number"
    ).agg(
        F.sum("revenue").alias("total_revenue"),
        F.avg("revenue").alias("avg_daily_revenue"),
        F.count("*").alias("days_reporting"),
        F.sum(F.when(F.col("is_weekend"), F.col("revenue")).otherwise(0)).alias("weekend_revenue"),
        F.sum(F.when(~F.col("is_weekend"), F.col("revenue")).otherwise(0)).alias("weekday_revenue"),
        F.countDistinct("state_code").alias("states_reporting"),
    )

    # Calculate week-over-week change
    window = Window.partitionBy("film_id").orderBy("week_number")
    weekly = weekly.withColumn(
        "prev_week_revenue",
        F.lag("total_revenue").over(window)
    ).withColumn(
        "wow_change",
        F.when(
            F.col("prev_week_revenue").isNotNull() & (F.col("prev_week_revenue") > 0),
            (F.col("total_revenue") - F.col("prev_week_revenue")) / F.col("prev_week_revenue")
        ).otherwise(None)
    )

    weekly = weekly.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    weekly.write.mode("overwrite").parquet(f"{processed_path}/box_office_weekly")
    print(f"Wrote {weekly.count()} weekly records")

    # =========================================================================
    # BOX OFFICE TOTAL TABLE
    # =========================================================================
    print("Creating box_office_total table...")

    total = daily.groupBy(
        "film_id", "film_title", "film_year"
    ).agg(
        F.sum("revenue").alias("total_revenue"),
        F.min("report_date").alias("first_report_date"),
        F.max("report_date").alias("last_report_date"),
        F.max("week_number").alias("weeks_in_release"),
        F.countDistinct("report_date").alias("days_in_release"),
        F.countDistinct("state_code").alias("states_reached"),
    )

    # Calculate opening weekend (first 3 days / first weekend)
    opening = daily.filter(F.col("week_number") == 1).groupBy(
        "film_id"
    ).agg(
        F.sum("revenue").alias("opening_week_revenue")
    )

    total = total.join(
        opening,
        "film_id",
        "left"
    )

    # Calculate average per-day revenue
    total = total.withColumn(
        "avg_daily_revenue",
        F.col("total_revenue") / F.col("days_in_release")
    )

    total = total.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    total.write.mode("overwrite").parquet(f"{processed_path}/box_office_total")
    print(f"Wrote {total.count()} total records")

    return {
        "box_office_daily": daily.count(),
        "box_office_weekly": weekly.count(),
        "box_office_total": total.count(),
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Load and aggregate box office data")
    parser.add_argument("--local", action="store_true", help="Run in local mode")

    args = parser.parse_args()

    spark = create_spark_session(local=args.local)
    raw_path, processed_path = get_paths(local=args.local)

    try:
        results = load_boxoffice(spark, raw_path, processed_path)
        print("\n=== Summary ===")
        for table, count in results.items():
            print(f"  {table}: {count} records")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
