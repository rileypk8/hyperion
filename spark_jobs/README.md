# PySpark Transformation Jobs

Spark jobs that transform raw JSON data from HDFS into normalized Parquet tables.

## Jobs

### 01_load_films.py
Loads film data and creates dimension/fact tables:
- `studios` - Studio dimension
- `franchises` - Franchise dimension
- `media` - Parent media table
- `films` - Film fact table

### 02_load_characters.py
Loads character data and creates:
- `roles` - Role lookup table
- `genders` - Gender lookup table
- `talent` - Voice actor dimension
- `characters` - Character dimension
- `character_appearances` - Junction table

### 03_load_boxoffice.py
Loads and aggregates box office data:
- `box_office_daily` - Daily state-level records
- `box_office_weekly` - Weekly aggregations with WoW change
- `box_office_total` - Total revenue per film

### 04_export_analytics.py
Creates denormalized exports for frontend:
- `films_complete.parquet` - All film data denormalized
- `characters_complete.parquet` - Character data with appearances
- `boxoffice_summary.parquet` - Aggregated revenue
- `studio_stats.parquet` - Pre-computed studio metrics

## Usage

### Submit to Spark cluster
```bash
# From inside Spark container or with spark-submit available
spark-submit --master spark://spark-master:7077 01_load_films.py
spark-submit --master spark://spark-master:7077 02_load_characters.py
spark-submit --master spark://spark-master:7077 03_load_boxoffice.py
spark-submit --master spark://spark-master:7077 04_export_analytics.py
```

### Local mode (for testing)
```bash
pip install pyspark

python 01_load_films.py --local
python 02_load_characters.py --local
python 03_load_boxoffice.py --local
python 04_export_analytics.py --local
```

### Run via Docker
```bash
docker exec -it hyperion-spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/01_load_films.py
```

## Execution Order

Jobs must be run in order due to dependencies:

```
01_load_films.py      # Creates studios, franchises, media, films
        ↓
02_load_characters.py # Depends on media table
        ↓
03_load_boxoffice.py  # Depends on films table
        ↓
04_export_analytics.py # Depends on all above
```

## HDFS Paths

| Type | Path |
|------|------|
| Raw input | `hdfs://namenode:9000/raw/` |
| Processed output | `hdfs://namenode:9000/processed/` |
| Analytics exports | `hdfs://namenode:9000/analytics/exports/` |

## Local Paths (for testing)

| Type | Path |
|------|------|
| Raw input | `/opt/data/raw/` |
| Processed output | `/opt/data/processed/` |
| Analytics exports | `/opt/data/exports/` |
