#!/usr/bin/env python3
"""
Spark job to load and transform character data.

Reads raw character JSON from HDFS, normalizes the data, and writes
Parquet tables for characters, talent, and character_appearances.
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

from config import (
    HDFS_RAW_PATH, HDFS_PROCESSED_PATH,
    LOCAL_RAW_PATH, LOCAL_PROCESSED_PATH,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_LOCAL_MASTER
)


# Canonical role values
ROLE_MAPPINGS = {
    "main protagonist": "protagonist",
    "main antagonist": "antagonist",
    "primary antagonist": "antagonist",
    "secondary antagonist": "antagonist",
    "main villain": "villain",
    "primary villain": "villain",
    "secondary villain": "villain",
    "main hero": "hero",
    "main character": "protagonist",
    "lead": "protagonist",
    "co-protagonist": "deuteragonist",
    "co-lead": "deuteragonist",
    "secondary protagonist": "deuteragonist",
    "comedic relief": "comic_relief",
    "comedy relief": "comic_relief",
    "romantic interest": "love_interest",
    "romantic lead": "love_interest",
    "side character": "supporting",
    "recurring": "supporting",
    "cameo": "minor",
    "background": "minor",
    "extra": "minor",
}

# Canonical gender values
GENDER_MAPPINGS = {
    "m": "male",
    "f": "female",
    "man": "male",
    "woman": "female",
    "boy": "male",
    "girl": "female",
    "nb": "non-binary",
    "nonbinary": "non-binary",
    "none": "n/a",
    "unknown": "n/a",
    "varies": "various",
    "multiple": "various",
}


def create_spark_session(local=False):
    """Create Spark session."""
    builder = SparkSession.builder.appName("{}-characters".format(SPARK_APP_NAME))

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


def normalize_role_udf():
    """Create UDF for role normalization."""
    def normalize(role):
        if role is None:
            return "supporting"
        role_lower = role.lower().strip()
        return ROLE_MAPPINGS.get(role_lower, role_lower)
    return F.udf(normalize, StringType())


def normalize_gender_udf():
    """Create UDF for gender normalization."""
    def normalize(gender):
        if gender is None:
            return "n/a"
        gender_lower = gender.lower().strip()
        return GENDER_MAPPINGS.get(gender_lower, gender_lower)
    return F.udf(normalize, StringType())


def load_characters(spark: SparkSession, raw_path: str, processed_path: str):
    """
    Load and transform character data.

    Creates:
    - characters: Character dimension table
    - talent: Voice actor dimension table
    - roles: Role lookup table
    - genders: Gender lookup table
    - character_appearances: Character-media-talent junction table
    """
    print(f"Reading raw characters from {raw_path}/characters/")

    # Read all JSON files from raw characters directory
    raw_chars = spark.read.json(f"{raw_path}/characters/*/*.json")

    print(f"Loaded {raw_chars.count()} raw character records")

    # Register UDFs
    normalize_role = normalize_role_udf()
    normalize_gender = normalize_gender_udf()

    # =========================================================================
    # ROLES LOOKUP TABLE
    # =========================================================================
    print("Creating roles table...")

    roles_list = [
        "protagonist", "deuteragonist", "tritagonist", "hero",
        "villain", "antagonist", "henchman",
        "sidekick", "mentor", "love_interest",
        "comic_relief", "supporting", "minor"
    ]

    roles = spark.createDataFrame(
        [(i + 1, role) for i, role in enumerate(roles_list)],
        ["id", "name"]
    )

    roles.write.mode("overwrite").parquet(f"{processed_path}/roles")
    print(f"Wrote {roles.count()} roles")

    # =========================================================================
    # GENDERS LOOKUP TABLE
    # =========================================================================
    print("Creating genders table...")

    genders_list = ["male", "female", "non-binary", "n/a", "various"]

    genders = spark.createDataFrame(
        [(i + 1, gender) for i, gender in enumerate(genders_list)],
        ["id", "name"]
    )

    genders.write.mode("overwrite").parquet(f"{processed_path}/genders")
    print(f"Wrote {genders.count()} genders")

    # =========================================================================
    # TALENT TABLE
    # =========================================================================
    print("Creating talent table...")

    talent = raw_chars.select(
        F.col("voice_actor").alias("name")
    ).filter(
        F.col("name").isNotNull() & (F.col("name") != "")
    ).dropDuplicates(["name"])

    talent = talent.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    talent = talent.select("id", "name")

    talent.write.mode("overwrite").parquet(f"{processed_path}/talent")
    print(f"Wrote {talent.count()} talent records")

    # =========================================================================
    # CHARACTERS TABLE
    # =========================================================================
    print("Creating characters table...")

    # Normalize role and gender
    characters = raw_chars.select(
        F.col("name"),
        F.col("franchise"),
        normalize_role(F.col("role")).alias("role"),
        normalize_gender(F.col("gender")).alias("gender"),
        F.col("species"),
        F.col("first_appearance_year"),
    ).dropDuplicates(["name", "franchise"])

    # Join to get role_id
    characters = characters.join(
        roles.select(F.col("id").alias("role_id"), F.col("name").alias("role_name")),
        characters.role == F.col("role_name"),
        "left"
    ).drop("role", "role_name")

    # Join to get gender_id
    characters = characters.join(
        genders.select(F.col("id").alias("gender_id"), F.col("name").alias("gender_name")),
        characters.gender == F.col("gender_name"),
        "left"
    ).drop("gender", "gender_name")

    characters = characters.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    characters = characters.select(
        "id", "name", "franchise", "role_id", "gender_id",
        "species", "first_appearance_year"
    )

    characters.write.mode("overwrite").parquet(f"{processed_path}/characters")
    print(f"Wrote {characters.count()} characters")

    # =========================================================================
    # CHARACTER APPEARANCES TABLE
    # =========================================================================
    print("Creating character_appearances table...")

    # Read media table for joining
    try:
        media = spark.read.parquet(f"{processed_path}/media")
    except Exception as e:
        print(f"Warning: Could not read media table: {e}")
        print("Run 01_load_films.py first")
        return {
            "roles": roles.count(),
            "genders": genders.count(),
            "talent": talent.count(),
            "characters": characters.count(),
            "character_appearances": 0,
        }

    # Explode appearances array and create junction records
    appearances = raw_chars.select(
        F.col("name").alias("character_name"),
        F.col("franchise"),
        F.col("voice_actor"),
        normalize_role(F.col("role")).alias("role"),
        F.explode_outer(F.col("appearances")).alias("appearance"),
    ).filter(F.col("appearance").isNotNull())

    appearances = appearances.select(
        F.col("character_name"),
        F.col("franchise"),
        F.col("voice_actor"),
        F.col("role"),
        F.col("appearance.title").alias("media_title"),
        F.col("appearance.year").alias("media_year"),
    )

    # Join to get character_id
    appearances = appearances.join(
        characters.select(
            F.col("id").alias("character_id"),
            F.col("name").alias("char_name"),
            F.col("franchise").alias("char_franchise")
        ),
        (appearances.character_name == F.col("char_name")) &
        (appearances.franchise == F.col("char_franchise")),
        "left"
    ).drop("char_name", "char_franchise")

    # Join to get media_id
    appearances = appearances.join(
        media.select(
            F.col("id").alias("media_id"),
            F.col("title").alias("m_title"),
            F.col("year").alias("m_year")
        ),
        (appearances.media_title == F.col("m_title")) &
        (appearances.media_year == F.col("m_year")),
        "left"
    ).drop("m_title", "m_year")

    # Join to get talent_id
    appearances = appearances.join(
        talent.select(F.col("id").alias("talent_id"), F.col("name").alias("t_name")),
        appearances.voice_actor == F.col("t_name"),
        "left"
    ).drop("t_name")

    # Join to get role_id
    appearances = appearances.join(
        roles.select(F.col("id").alias("role_id"), F.col("name").alias("r_name")),
        appearances.role == F.col("r_name"),
        "left"
    ).drop("r_name")

    appearances = appearances.withColumn(
        "id",
        F.monotonically_increasing_id() + 1
    )

    appearances = appearances.select(
        "id", "character_id", "media_id", "talent_id", "role_id"
    ).filter(
        F.col("character_id").isNotNull() &
        F.col("media_id").isNotNull()
    ).dropDuplicates(["character_id", "media_id"])

    appearances.write.mode("overwrite").parquet(f"{processed_path}/character_appearances")
    print(f"Wrote {appearances.count()} character appearances")

    return {
        "roles": roles.count(),
        "genders": genders.count(),
        "talent": talent.count(),
        "characters": characters.count(),
        "character_appearances": appearances.count(),
    }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Load and transform character data")
    parser.add_argument("--local", action="store_true", help="Run in local mode")

    args = parser.parse_args()

    spark = create_spark_session(local=args.local)
    raw_path, processed_path = get_paths(local=args.local)

    try:
        results = load_characters(spark, raw_path, processed_path)
        print("\n=== Summary ===")
        for table, count in results.items():
            print(f"  {table}: {count} records")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
