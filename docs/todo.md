# Hyperion Implementation TODO

## Overview

Pivoting to new architecture: Local Docker development → Static cloud deployment with client-side analytics.

**Budget:** <$5/month + one-time ~$5 EMR proof run
**Stack:** Kafka, Hadoop/HDFS, Spark, Presto, S3, React + DuckDB-WASM

---

## Current State vs New Plan

| Aspect | Current State | New Plan |
|--------|---------------|----------|
| **Storage** | PostgreSQL + SQLite | HDFS → Parquet files |
| **Consumers** | Kafka → PostgreSQL | Kafka → HDFS (webhdfs) |
| **Processing** | Batch Python loaders | PySpark jobs |
| **Query Engine** | PostgreSQL/SQLite | Presto/Trino |
| **Frontend Data** | Mock data in JS | DuckDB-WASM + Parquet |
| **Deployment** | AWS RDS + services | Static S3 + CloudFront |
| **Monthly Cost** | $50-100+ (RDS) | <$5 |

---

## Assets to Reuse

| Asset | Reusability | Notes |
|-------|-------------|-------|
| 160 JSON source files | **100%** | Same input data |
| Kafka docker-compose | **70%** | Extend with Hadoop/Spark/Presto |
| Data normalization schemas | **100%** | Role/gender/species mappings in etl/shared/schemas.py |
| React frontend (19 components) | **80%** | Keep UI, swap data layer to DuckDB-WASM |
| S3 Terraform | **90%** | Repurpose for static hosting |
| Producer logic | **60%** | Adapt for HDFS landing |

---

## Deliverables

### Phase 1: Infrastructure

- [ ] **Docker Compose (full stack)** - P0, critical path
  - Extend `etl/streaming/docker-compose.yml`
  - Add: Hadoop NameNode + DataNode, Spark master + worker, Hive Metastore, Presto/Trino
  - Output: `docker-compose.yml`, `hadoop.env`, init scripts

- [ ] **DuckDB-WASM in frontend** - P1, parallel track
  - Add @duckdb/duckdb-wasm to web/
  - Create data loading hook
  - Replace mockData with Parquet queries

### Phase 2: Pipeline

- [ ] **Kafka producers** - P1
  - Reuse existing logic from etl/batch/
  - Topics: raw-films, raw-characters, raw-boxoffice
  - Output: `producers/` directory

- [ ] **HDFS consumers** - P1
  - Write Kafka messages to HDFS as JSON
  - Use webhdfs or hdfs3 library
  - Output: `consumers/` directory, data lands in `hdfs:///raw/`

- [ ] **PySpark jobs** - P1
  - `01_load_films.py` - Parse films, create media/films/studios
  - `02_load_characters.py` - Parse characters, resolve FKs
  - `03_load_boxoffice.py` - Aggregate daily data
  - `04_export_analytics.py` - Create denormalized views for frontend
  - Output: Parquet files in `hdfs:///processed/`

### Phase 3: Query & Export

- [ ] **Presto SQL queries** - P2
  - Revenue by studio/year
  - Character counts by franchise
  - Top voice actors by appearance count
  - Output: `queries/` directory with .sql files

- [ ] **Analytics export job** - P2
  - Spark job producing small Parquet files (<10MB total)
  - `films_complete.parquet`
  - `characters_complete.parquet`
  - `boxoffice_summary.parquet`
  - `studio_stats.parquet`
  - Output: `exports/` directory

### Phase 4: Deployment

- [ ] **S3 + CloudFront** - P3
  - Static site hosting
  - Parquet files in `/data/` path
  - CloudFront distribution with HTTPS
  - Output: Deploy scripts, ~$0.50/month

- [ ] **DNS config** - P3
  - Namecheap CNAME to CloudFront
  - Custom domain with HTTPS

### Phase 5: Cloud Validation

- [ ] **EMR proof run** - P4, one-shot
  - Upload source data to S3
  - Create EMR cluster (1 master, 2 core, m5.xlarge)
  - Run Spark jobs, screenshot results
  - Terminate immediately
  - Output: `docs/emr_proof/` with screenshots, cost ~$3-5

---

## Deprecate

- [ ] RDS Terraform (`terraform/rds.tf`) - Not needed for static deployment
- [ ] PostgreSQL consumers (`etl/streaming/revenue_consumer.py`) - Replace with HDFS
- [ ] SQLite workflow - Parquet is the new target

---

## Execution Order

```
Phase 1: Foundation
├── [1] Extend docker-compose.yml with Hadoop/Spark/Presto
├── [2] Add DuckDB-WASM to frontend (parallel)
└── [3] Generate sample Parquet from SQLite for frontend testing

Phase 2: Pipeline
├── [4] Kafka producers (mostly reuse existing)
├── [5] HDFS consumers (new)
└── [6] Spark jobs (adapt batch loader logic)

Phase 3: Query & Export
├── [7] Presto SQL queries
└── [8] Analytics export Spark job

Phase 4: Deploy
├── [9] S3 + CloudFront Terraform
├── [10] DNS config
└── [11] EMR proof run (optional)
```

---

## File Structure Target

```
hyperion/
├── docker-compose.yml          # Full stack: Kafka, Hadoop, Spark, Presto
├── hadoop.env
├── scripts/
│   └── init-hdfs.sh
├── producers/
│   ├── films_producer.py
│   ├── characters_producer.py
│   └── boxoffice_producer.py
├── consumers/
│   ├── hdfs_consumer.py
│   └── consumer_config.py
├── spark_jobs/
│   ├── 01_load_films.py
│   ├── 02_load_characters.py
│   ├── 03_load_boxoffice.py
│   └── 04_export_analytics.py
├── queries/
│   ├── revenue_by_studio.sql
│   ├── character_counts.sql
│   └── voice_actor_stats.sql
├── web/                        # Existing React app
│   └── src/
│       └── hooks/
│           └── useDuckDB.ts    # New: DuckDB-WASM integration
├── exports/
│   └── (generated parquet files)
├── docs/
│   ├── todo.md                 # This file
│   ├── emr_proof/
│   └── architecture.md
├── deploy/
│   ├── s3_sync.sh
│   └── cloudfront_config.json
└── data/
    └── (existing JSON files)
```
