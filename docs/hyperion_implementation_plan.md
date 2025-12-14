# Hyperion Implementation Plan

## Overview

**Goal:** Portfolio project demonstrating big data pipeline skills  
**Budget:** <$5/month + one-time ~$5 EMR proof run  
**Stack:** Kafka, Hadoop/HDFS, Spark, Impala, S3, React + DuckDB-WASM  
**Approach:** Local Docker development, static cloud deployment, client-side analytics

---

## Deliverables

### 1. Docker Compose - Local Big Data Stack

| Aspect | Description |
|--------|-------------|
| **Why** | Run Hadoop/Spark/Kafka/Impala locally without cloud costs. |
| **What** | docker-compose.yml with all big data services configured. |
| **How** | Use bitnami/spark, confluentinc/kafka, apache/hadoop images. |

**Services:**
- Zookeeper (Kafka dependency)
- Kafka (single broker)
- Hadoop NameNode + DataNode (HDFS)
- Spark master + worker
- Hive Metastore (for Impala/Presto)
- Presto or Trino (Impala alternative, easier to containerize)

**Output:** `docker-compose.yml`, `hadoop.env`, startup scripts

---

### 2. Kafka Producers - Ingest Source Data

| Aspect | Description |
|--------|-------------|
| **Why** | Demonstrate streaming data ingestion patterns. |
| **What** | Python scripts that publish JSON to Kafka topics. |
| **How** | Read source files, serialize to JSON, send via kafka-python. |

**Topics:**
- `raw-films` ← from complete_movie_list.json
- `raw-characters` ← from character JSON files
- `raw-boxoffice` ← from box office faker output

**Output:** `producers/` directory with Python scripts per topic

---

### 3. Kafka Consumers - Land to HDFS

| Aspect | Description |
|--------|-------------|
| **Why** | Show Kafka-to-storage pipeline pattern. |
| **What** | Consumers that write Kafka messages to HDFS as JSON. |
| **How** | kafka-python consumer, write batches to HDFS via webhdfs. |

**Output:** `consumers/` directory, data lands in `hdfs:///raw/`

---

### 4. Spark Jobs - Transform and Normalize

| Aspect | Description |
|--------|-------------|
| **Why** | Demonstrate distributed data processing with Spark. |
| **What** | PySpark jobs that clean, join, and aggregate data. |
| **How** | Read from HDFS, apply transformations, write Parquet to HDFS. |

**Jobs:**
- `01_load_films.py` - Parse films, create media/films/studios tables
- `02_load_characters.py` - Parse characters, resolve FKs, create joins
- `03_load_boxoffice.py` - Aggregate daily data, compute summaries
- `04_export_analytics.py` - Create denormalized views for frontend

**Output:** `spark_jobs/` directory, Parquet files in `hdfs:///processed/`

---

### 5. Impala/Presto Queries - Big Data SQL Demo

| Aspect | Description |
|--------|-------------|
| **Why** | Show SQL query engine experience on distributed data. |
| **What** | SQL scripts querying Parquet data via Presto/Trino. |
| **How** | External tables over HDFS Parquet, run sample analytics queries. |

**Queries:**
- Revenue by studio/year
- Character counts by franchise
- Top voice actors by appearance count
- Box office performance trends

**Output:** `queries/` directory with .sql files, screenshot results

---

### 6. EMR Proof Run - Cloud Validation

| Aspect | Description |
|--------|-------------|
| **Why** | Prove pipeline runs on real cloud infrastructure. |
| **What** | Same Spark jobs executed on AWS EMR cluster. |
| **How** | Spin up EMR, upload scripts, run, screenshot, terminate. |

**Steps:**
1. Upload source data to S3
2. Create EMR cluster (1 master, 2 core, m5.xlarge)
3. SSH in, run Spark jobs against S3
4. Query with Presto/Hive
5. Screenshot logs, Spark UI, query results
6. Terminate cluster immediately

**Output:** `docs/emr_proof/` with screenshots, cost ~$3-5

---

### 7. Analytics Export - Frontend Data Package

| Aspect | Description |
|--------|-------------|
| **Why** | Enable client-side querying without live backend. |
| **What** | Parquet files optimized for DuckDB-WASM consumption. |
| **How** | Spark job exports denormalized views as small Parquet files. |

**Exports:**
- `films_complete.parquet` - All film data denormalized
- `characters_complete.parquet` - All character appearances
- `boxoffice_summary.parquet` - Aggregated revenue data
- `studio_stats.parquet` - Pre-computed studio metrics

**Output:** `exports/` directory, <10MB total

---

### 8. React Frontend - Interactive Dashboard

| Aspect | Description |
|--------|-------------|
| **Why** | Deliver portfolio-worthy UI showing data insights. |
| **What** | React SPA with DuckDB-WASM for client-side queries. |
| **How** | Vite + React + DuckDB-WASM + Recharts/Victory for viz. |

**Pages:**
- `/` - Overview dashboard (studio counts, total films, highlights)
- `/studios` - Compare studios, filterable charts
- `/films` - Searchable/sortable film list with details
- `/characters` - Character browser with franchise filter
- `/analytics` - Custom query builder (optional stretch goal)

**Features:**
- Load Parquet files on startup
- SQL queries execute in browser via DuckDB-WASM
- No backend API calls for data

**Output:** `frontend/` directory, builds to static files

---

### 9. S3 + CloudFront Deployment

| Aspect | Description |
|--------|-------------|
| **Why** | Host static site cheaply with global CDN. |
| **What** | S3 bucket serving React build + Parquet data files. |
| **How** | AWS CLI sync, CloudFront distribution, HTTPS via ACM. |

**Structure:**
```
s3://hyperion-site/
├── index.html
├── assets/
│   ├── *.js
│   └── *.css
└── data/
    ├── films_complete.parquet
    ├── characters_complete.parquet
    └── ...
```

**Output:** Deployed site, CloudFront URL, ~$0.50/month

---

### 10. Namecheap DNS Configuration

| Aspect | Description |
|--------|-------------|
| **Why** | Professional custom domain for portfolio. |
| **What** | DNS records pointing domain to CloudFront. |
| **How** | CNAME or ALIAS record to CloudFront distribution. |

**Records:**
- `yourdomain.com` → CloudFront (via ALIAS or redirect)
- `www.yourdomain.com` → CloudFront (CNAME)

**Output:** Live site at custom domain with HTTPS

---

## Execution Order

```
Phase 1: Infrastructure (do first)
├── [1] Docker Compose setup
└── [8] React scaffold (parallel)

Phase 2: Pipeline (after Docker works)
├── [2] Kafka producers
├── [3] Kafka consumers
├── [4] Spark jobs
└── [5] Presto queries

Phase 3: Export & Frontend (after pipeline works)
├── [7] Analytics export
└── [8] React frontend (complete)

Phase 4: Deployment (after frontend works)
├── [9] S3 + CloudFront
└── [10] DNS config

Phase 5: Cloud Proof (anytime, one-shot)
└── [6] EMR proof run
```

---

## File Structure

```
hyperion/
├── docker-compose.yml
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
├── frontend/
│   ├── package.json
│   ├── src/
│   └── public/
├── exports/
│   └── (generated parquet files)
├── docs/
│   ├── emr_proof/
│   └── architecture.md
├── deploy/
│   ├── s3_sync.sh
│   └── cloudfront_config.json
└── data/
    ├── complete_movie_list.json
    ├── CHARACTER_SCHEMA.json
    └── characters/
        └── (character JSON files)
```

---

## Notes for Claude Code

- Start with deliverable [1] Docker Compose - everything else depends on it
- Deliverable [8] React scaffold can start in parallel with basic mock data
- Test each deliverable independently before integrating
- Keep Parquet exports small (<10MB) for browser performance
- Presto is used instead of Impala (easier Docker setup, same skill demonstration)
- All source data files are in project context (complete_movie_list.json, etc.)
