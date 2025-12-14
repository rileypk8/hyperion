#!/usr/bin/env python3
"""
Spark job to load and transform film data.

Reads raw film JSON from HDFS, normalizes the data, and writes
Parquet tables for studios, franchises, media, and films.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

from config import (
    HDFS_RAW_PATH, HDFS_PROCESSED_PATH,
    LOCAL_RAW_PATH, LOCAL_PROCESSED_PATH,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_LOCAL_MASTER
)


def create_spark_session(local=False):
    """Create Spark session."""
    builder = SparkSession.builder.appName("{}-films".format(SPARK_APP_NAME))

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


def load_films(spark: SparkSession, raw_path: str, processed_path: str):
    """
    Load and transform film data.

    Creates:
    - studios: Studio dimension table
    - franchises: Franchise dimension table
    - media: Parent media table
    - films: Film fact table with foreign keys
    """
    print(f"Reading raw films from {raw_path}/films/")

    # Read all JSON files from raw films directory
    raw_films = spark.read.json(f"{raw_path}/films/*/*.json")

    print(f"Loaded {raw_films.count()} raw film records")

    # =========================================================================
    # STUDIOS TABLE
    # =========================================================================
    print("Creating studios table...")

    studios = raw_films.select(
        F.col("studio").alias("name"),
        F.col("studio_founded").alias("years_active_start"),
        F.col("studio_closed").alias("years_active_end"),
        F.col("studio_status").alias("status"),
    ).dropDuplicates(["name"]).filter(F.col("name").isNotNull())

    # Add surrogate key
    studios = studios.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    studios = studios.select("id", "name", "years_active_start", "years_active_end", "status")

    studios.write.mode("overwrite").parquet(f"{processed_path}/studios")
    print(f"Wrote {studios.count()} studios")

    # =========================================================================
    # FRANCHISES TABLE
    # =========================================================================
    print("Creating franchises table...")

    franchises = raw_films.select(
        F.col("franchise").alias("name")
    ).dropDuplicates(["name"]).filter(F.col("name").isNotNull())

    franchises = franchises.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    franchises = franchises.select("id", "name")

    franchises.write.mode("overwrite").parquet(f"{processed_path}/franchises")
    print(f"Wrote {franchises.count()} franchises")

    # =========================================================================
    # MEDIA TABLE
    # =========================================================================
    print("Creating media table...")

    media = raw_films.select(
        F.col("title"),
        F.col("year"),
        F.col("franchise"),
        F.lit("film").alias("media_type"),
    ).dropDuplicates(["title", "year", "media_type"])

    # Join to get franchise_id
    media = media.join(
        franchises.select(F.col("id").alias("franchise_id"), F.col("name").alias("franchise_name")),
        media.franchise == F.col("franchise_name"),
        "left"
    ).drop("franchise", "franchise_name")

    media = media.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    media = media.select("id", "title", "year", "franchise_id", "media_type")

    media.write.mode("overwrite").parquet(f"{processed_path}/media")
    print(f"Wrote {media.count()} media records")

    # =========================================================================
    # FILMS TABLE
    # =========================================================================
    print("Creating films table...")

    films = raw_films.select(
        F.col("title"),
        F.col("year"),
        F.col("studio"),
        F.col("animation_type"),
    ).dropDuplicates(["title", "year"])

    # Join to get media_id
    films = films.join(
        media.select(
            F.col("id").alias("media_id"),
            F.col("title").alias("media_title"),
            F.col("year").alias("media_year")
        ),
        (films.title == F.col("media_title")) & (films.year == F.col("media_year")),
        "left"
    ).drop("media_title", "media_year")

    # Join to get studio_id
    films = films.join(
        studios.select(F.col("id").alias("studio_id"), F.col("name").alias("studio_name")),
        films.studio == F.col("studio_name"),
        "left"
    ).drop("studio", "studio_name")

    films = films.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    films = films.select("id", "media_id", "studio_id", "animation_type")

    films.write.mode("overwrite").parquet(f"{processed_path}/films")
    print(f"Wrote {films.count()} films")

    return {
        "studios": studios.count(),
        "franchises": franchises.count(),
        "media": media.count(),
        "films": films.count(),
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Load and transform film data")
    parser.add_argument("--local", action="store_true", help="Run in local mode")

    args = parser.parse_args()

    spark = create_spark_session(local=args.local)
    raw_path, processed_path = get_paths(local=args.local)

    try:
        results = load_films(spark, raw_path, processed_path)
        print("\n=== Summary ===")
        for table, count in results.items():
            print(f"  {table}: {count} records")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
