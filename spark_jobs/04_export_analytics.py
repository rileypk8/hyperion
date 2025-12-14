#!/usr/bin/env python3
"""
Spark job to export denormalized analytics data for frontend.

Creates optimized Parquet files for DuckDB-WASM consumption:
- films_complete.parquet: All film data denormalized
- characters_complete.parquet: All character data with appearances
- boxoffice_summary.parquet: Aggregated revenue data
- studio_stats.parquet: Pre-computed studio metrics
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (
    HDFS_PROCESSED_PATH, HDFS_EXPORTS_PATH,
    LOCAL_PROCESSED_PATH, LOCAL_EXPORTS_PATH,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_LOCAL_MASTER
)


def create_spark_session(local=False):
    """Create Spark session."""
    builder = SparkSession.builder.appName("{}-export".format(SPARK_APP_NAME))

    if local:
        builder = builder.master(SPARK_LOCAL_MASTER)
    else:
        builder = builder.master(SPARK_MASTER)

    return builder.getOrCreate()


def get_paths(local=False):
    """Get input/output paths based on mode."""
    if local:
        return LOCAL_PROCESSED_PATH, LOCAL_EXPORTS_PATH
    return HDFS_PROCESSED_PATH, HDFS_EXPORTS_PATH


def export_analytics(spark: SparkSession, processed_path: str, exports_path: str):
    """
    Export denormalized analytics data for frontend.

    Creates optimized single-file Parquet exports for browser loading.
    """
    results = {}

    # =========================================================================
    # FILMS COMPLETE
    # =========================================================================
    print("Creating films_complete export...")

    try:
        films = spark.read.parquet(f"{processed_path}/films")
        media = spark.read.parquet(f"{processed_path}/media")
        studios = spark.read.parquet(f"{processed_path}/studios")
        franchises = spark.read.parquet(f"{processed_path}/franchises")

        films_complete = films.join(
            media.select(
                F.col("id").alias("media_id"),
                F.col("title"),
                F.col("year"),
                F.col("franchise_id"),
            ),
            "media_id"
        ).join(
            studios.select(
                F.col("id").alias("studio_id"),
                F.col("name").alias("studio_name"),
                F.col("status").alias("studio_status"),
            ),
            "studio_id",
            "left"
        ).join(
            franchises.select(
                F.col("id").alias("franchise_id"),
                F.col("name").alias("franchise_name"),
            ),
            "franchise_id",
            "left"
        )

        # Try to add box office totals
        try:
            box_office_total = spark.read.parquet(f"{processed_path}/box_office_total")
            films_complete = films_complete.join(
                box_office_total.select(
                    F.col("film_id"),
                    F.col("total_revenue"),
                    F.col("opening_week_revenue"),
                    F.col("weeks_in_release"),
                ),
                films_complete.id == F.col("film_id"),
                "left"
            ).drop("film_id")
        except Exception:
            films_complete = films_complete.withColumn("total_revenue", F.lit(None))
            films_complete = films_complete.withColumn("opening_week_revenue", F.lit(None))
            films_complete = films_complete.withColumn("weeks_in_release", F.lit(None))

        films_complete = films_complete.select(
            "id", "title", "year", "animation_type",
            "studio_name", "studio_status", "franchise_name",
            "total_revenue", "opening_week_revenue", "weeks_in_release"
        )

        # Write as single file for browser loading
        films_complete.coalesce(1).write.mode("overwrite").parquet(
            f"{exports_path}/films_complete"
        )
        results["films_complete"] = films_complete.count()
        print(f"Wrote {results['films_complete']} films")

    except Exception as e:
        print(f"Error creating films_complete: {e}")
        results["films_complete"] = 0

    # =========================================================================
    # CHARACTERS COMPLETE
    # =========================================================================
    print("Creating characters_complete export...")

    try:
        characters = spark.read.parquet(f"{processed_path}/characters")
        roles = spark.read.parquet(f"{processed_path}/roles")
        genders = spark.read.parquet(f"{processed_path}/genders")
        talent = spark.read.parquet(f"{processed_path}/talent")
        appearances = spark.read.parquet(f"{processed_path}/character_appearances")

        # Get appearance count per character
        appearance_counts = appearances.groupBy("character_id").agg(
            F.count("*").alias("appearance_count")
        )

        # Get voice actors per character
        char_talent = appearances.join(
            talent.select(F.col("id").alias("talent_id"), F.col("name").alias("voice_actor")),
            "talent_id",
            "left"
        ).groupBy("character_id").agg(
            F.collect_set("voice_actor").alias("voice_actors")
        )

        characters_complete = characters.join(
            roles.select(F.col("id").alias("role_id"), F.col("name").alias("role")),
            "role_id",
            "left"
        ).join(
            genders.select(F.col("id").alias("gender_id"), F.col("name").alias("gender")),
            "gender_id",
            "left"
        ).join(
            appearance_counts,
            characters.id == F.col("character_id"),
            "left"
        ).drop("character_id").join(
            char_talent,
            characters.id == char_talent.character_id,
            "left"
        ).drop("character_id")

        # Convert voice_actors array to string for easier frontend use
        characters_complete = characters_complete.withColumn(
            "voice_actors_str",
            F.concat_ws(", ", F.col("voice_actors"))
        ).drop("voice_actors")

        characters_complete = characters_complete.select(
            "id", "name", "franchise", "role", "gender", "species",
            "first_appearance_year", "appearance_count", "voice_actors_str"
        ).fillna({"appearance_count": 0})

        characters_complete.coalesce(1).write.mode("overwrite").parquet(
            f"{exports_path}/characters_complete"
        )
        results["characters_complete"] = characters_complete.count()
        print(f"Wrote {results['characters_complete']} characters")

    except Exception as e:
        print(f"Error creating characters_complete: {e}")
        results["characters_complete"] = 0

    # =========================================================================
    # BOXOFFICE SUMMARY
    # =========================================================================
    print("Creating boxoffice_summary export...")

    try:
        box_office_total = spark.read.parquet(f"{processed_path}/box_office_total")
        box_office_weekly = spark.read.parquet(f"{processed_path}/box_office_weekly")

        # Add franchise info via films
        boxoffice_summary = box_office_total.join(
            films_complete.select(
                F.col("title"),
                F.col("year"),
                F.col("studio_name"),
                F.col("franchise_name"),
            ),
            (box_office_total.film_title == F.col("title")) &
            (box_office_total.film_year == F.col("year")),
            "left"
        ).drop("title", "year")

        boxoffice_summary = boxoffice_summary.select(
            "film_title", "film_year", "studio_name", "franchise_name",
            "total_revenue", "opening_week_revenue",
            "weeks_in_release", "days_in_release",
            "avg_daily_revenue", "states_reached"
        )

        boxoffice_summary.coalesce(1).write.mode("overwrite").parquet(
            f"{exports_path}/boxoffice_summary"
        )
        results["boxoffice_summary"] = boxoffice_summary.count()
        print(f"Wrote {results['boxoffice_summary']} box office records")

    except Exception as e:
        print(f"Error creating boxoffice_summary: {e}")
        results["boxoffice_summary"] = 0

    # =========================================================================
    # STUDIO STATS
    # =========================================================================
    print("Creating studio_stats export...")

    try:
        # Aggregate stats by studio
        studio_stats = films_complete.groupBy(
            "studio_name", "studio_status"
        ).agg(
            F.count("*").alias("film_count"),
            F.min("year").alias("first_film_year"),
            F.max("year").alias("last_film_year"),
            F.sum("total_revenue").alias("total_box_office"),
            F.avg("total_revenue").alias("avg_box_office"),
            F.countDistinct("franchise_name").alias("franchise_count"),
        )

        # Add character counts
        char_by_studio = characters_complete.join(
            films_complete.select("franchise_name", "studio_name").dropDuplicates(),
            "franchise_name",
            "left"
        ).groupBy("studio_name").agg(
            F.count("*").alias("character_count"),
            F.countDistinct("voice_actors_str").alias("unique_voice_actors"),
        )

        studio_stats = studio_stats.join(
            char_by_studio,
            "studio_name",
            "left"
        )

        studio_stats.coalesce(1).write.mode("overwrite").parquet(
            f"{exports_path}/studio_stats"
        )
        results["studio_stats"] = studio_stats.count()
        print(f"Wrote {results['studio_stats']} studio stat records")

    except Exception as e:
        print(f"Error creating studio_stats: {e}")
        results["studio_stats"] = 0

    return results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Export analytics data for frontend")
    parser.add_argument("--local", action="store_true", help="Run in local mode")

    args = parser.parse_args()

    spark = create_spark_session(local=args.local)
    processed_path, exports_path = get_paths(local=args.local)

    try:
        results = export_analytics(spark, processed_path, exports_path)
        print("\n=== Export Summary ===")
        for export, count in results.items():
            print(f"  {export}: {count} records")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
