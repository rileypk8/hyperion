-- Create external tables over Parquet files in HDFS
-- Run this first to set up the Trino/Presto schema

-- Create schema for Hyperion data
CREATE SCHEMA IF NOT EXISTS hive.hyperion;

-- Studios dimension table
CREATE TABLE IF NOT EXISTS hive.hyperion.studios (
    id BIGINT,
    name VARCHAR,
    years_active_start INTEGER,
    years_active_end INTEGER,
    status VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/studios',
    format = 'PARQUET'
);

-- Franchises dimension table
CREATE TABLE IF NOT EXISTS hive.hyperion.franchises (
    id BIGINT,
    name VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/franchises',
    format = 'PARQUET'
);

-- Media parent table
CREATE TABLE IF NOT EXISTS hive.hyperion.media (
    id BIGINT,
    title VARCHAR,
    year INTEGER,
    franchise_id BIGINT,
    media_type VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/media',
    format = 'PARQUET'
);

-- Films fact table
CREATE TABLE IF NOT EXISTS hive.hyperion.films (
    id BIGINT,
    media_id BIGINT,
    studio_id BIGINT,
    animation_type VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/films',
    format = 'PARQUET'
);

-- Roles lookup table
CREATE TABLE IF NOT EXISTS hive.hyperion.roles (
    id BIGINT,
    name VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/roles',
    format = 'PARQUET'
);

-- Genders lookup table
CREATE TABLE IF NOT EXISTS hive.hyperion.genders (
    id BIGINT,
    name VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/genders',
    format = 'PARQUET'
);

-- Talent dimension table
CREATE TABLE IF NOT EXISTS hive.hyperion.talent (
    id BIGINT,
    name VARCHAR
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/talent',
    format = 'PARQUET'
);

-- Characters dimension table
CREATE TABLE IF NOT EXISTS hive.hyperion.characters (
    id BIGINT,
    name VARCHAR,
    franchise VARCHAR,
    role_id BIGINT,
    gender_id BIGINT,
    species VARCHAR,
    first_appearance_year INTEGER
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/characters',
    format = 'PARQUET'
);

-- Character appearances junction table
CREATE TABLE IF NOT EXISTS hive.hyperion.character_appearances (
    id BIGINT,
    character_id BIGINT,
    media_id BIGINT,
    talent_id BIGINT,
    role_id BIGINT
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/character_appearances',
    format = 'PARQUET'
);

-- Box office daily table
CREATE TABLE IF NOT EXISTS hive.hyperion.box_office_daily (
    id BIGINT,
    film_id BIGINT,
    film_title VARCHAR,
    film_year INTEGER,
    report_date DATE,
    state_code VARCHAR,
    revenue DOUBLE,
    week_number INTEGER,
    is_weekend BOOLEAN
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/box_office_daily',
    format = 'PARQUET'
);

-- Box office weekly table
CREATE TABLE IF NOT EXISTS hive.hyperion.box_office_weekly (
    id BIGINT,
    film_id BIGINT,
    film_title VARCHAR,
    film_year INTEGER,
    week_number INTEGER,
    total_revenue DOUBLE,
    avg_daily_revenue DOUBLE,
    days_reporting BIGINT,
    weekend_revenue DOUBLE,
    weekday_revenue DOUBLE,
    states_reporting BIGINT,
    prev_week_revenue DOUBLE,
    wow_change DOUBLE
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/box_office_weekly',
    format = 'PARQUET'
);

-- Box office total table
CREATE TABLE IF NOT EXISTS hive.hyperion.box_office_total (
    id BIGINT,
    film_id BIGINT,
    film_title VARCHAR,
    film_year INTEGER,
    total_revenue DOUBLE,
    first_report_date DATE,
    last_report_date DATE,
    weeks_in_release INTEGER,
    days_in_release BIGINT,
    states_reached BIGINT,
    opening_week_revenue DOUBLE,
    avg_daily_revenue DOUBLE
)
WITH (
    external_location = 'hdfs://namenode:9000/processed/box_office_total',
    format = 'PARQUET'
);
