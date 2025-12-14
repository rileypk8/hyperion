# Presto/Trino SQL Queries

SQL queries for analyzing Hyperion data via Trino.

## Setup

1. Start the stack:
   ```bash
   ./scripts/start-stack.sh
   ```

2. Run Spark jobs to create Parquet tables:
   ```bash
   ./scripts/run-spark-jobs.sh
   ```

3. Create external tables in Trino:
   ```bash
   docker exec -it hyperion-trino trino --execute "$(cat queries/00_create_external_tables.sql)"
   ```

## Running Queries

### Via Trino CLI
```bash
# Interactive mode
docker exec -it hyperion-trino trino

# Run a specific query file
docker exec -it hyperion-trino trino --file /queries/revenue_by_studio.sql

# Single query
docker exec -it hyperion-trino trino --execute "SELECT * FROM hive.hyperion.studios LIMIT 10"
```

### Via Trino Web UI
Open http://localhost:8083 and use the query editor.

## Query Files

| File | Description |
|------|-------------|
| `00_create_external_tables.sql` | Create schema and external tables over Parquet |
| `revenue_by_studio.sql` | Box office revenue analysis by studio |
| `character_counts.sql` | Character demographics by franchise/studio |
| `voice_actor_stats.sql` | Voice talent statistics and career analysis |
| `boxoffice_trends.sql` | Revenue patterns, decay rates, geographic distribution |

## Sample Queries

### Quick Studio Summary
```sql
SELECT name, status,
       (SELECT COUNT(*) FROM hive.hyperion.films f WHERE f.studio_id = s.id) as film_count
FROM hive.hyperion.studios s
ORDER BY film_count DESC;
```

### Top Characters by Appearances
```sql
SELECT c.name, c.franchise, COUNT(*) as appearances
FROM hive.hyperion.character_appearances ca
JOIN hive.hyperion.characters c ON ca.character_id = c.id
GROUP BY c.name, c.franchise
ORDER BY appearances DESC
LIMIT 20;
```

### Revenue by Animation Type
```sql
SELECT f.animation_type,
       COUNT(*) as films,
       SUM(bot.total_revenue) as total_revenue
FROM hive.hyperion.films f
LEFT JOIN hive.hyperion.box_office_total bot ON f.id = bot.film_id
GROUP BY f.animation_type;
```

## Notes

- All tables use the `hive.hyperion` schema
- External tables read directly from HDFS Parquet files
- No data is duplicated - Trino queries HDFS in place
- Queries work with Presto as well (minor syntax differences)
