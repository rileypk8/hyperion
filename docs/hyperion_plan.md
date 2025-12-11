# Hyperion Project Plan

## Overview

**Codename:** Hyperion  
**Domain:** Disney media properties database  
**Scope:** ETL pipeline + analytics platform demonstrating big data engineering concepts using Disney's animation portfolio as source data  
**Hosting:** Personal AWS account  
**Timeline:** ASAP (parallelize aggressively)

---

## Required Stack

| Layer | Technology | Status |
|-------|------------|--------|
| Database | PostgreSQL (normalized) | Not started |
| ETL | Kafka | Not started |
| Storage | AWS S3 | Not started |
| Compute | Spark | Not started |
| Query | Hive | Not started |
| Cloud | AWS (IAM, access controls) | Not started |
| Web | React + Node (auth stubbed) | Not started |

---

## Data Assets

| Asset | Status |
|-------|--------|
| Film catalog (188 films, 8 studios) | ✅ Complete |
| Character schema | ✅ Complete |
| Character files (Pixar, WDAS, Blue Sky, DisneyToon, KH, Marvel) | ✅ Complete |
| Box office faker (disaggregation provider) | ✅ Complete |
| Box office time-series data | Not generated yet |

---

## Data Model (Entities)

Fully normalized. Target tables:

| Table | Source | Notes |
|-------|--------|-------|
| `studios` | complete_movie_list.json | Parent company subsidiaries |
| `franchises` | Derived from film groupings | Toy Story, Frozen, etc. |
| `films` | complete_movie_list.json + character files | FK to studio, franchise |
| `characters` | Character JSON files | FK to film (via join table) |
| `talent` | Extracted from character files | Normalized voice actors |
| `character_talent` | Derived | Join table: who voiced whom |
| `character_films` | Derived | Join table: appearances |
| `box_office_daily` | Generated via faker | FK to film, date, state |

---

## Work Phases & Parallelization

```
                    ┌─────────────────┐
                    │  Phase 1: DB    │
                    │  Schema (DDL)   │
                    └────────┬────────┘
                             │
           ┌─────────────────┼─────────────────┐
           │                 │                 │
           ▼                 ▼                 ▼
    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
    │ Phase 2: AWS │  │ Phase 3: ETL │  │ Phase 5: UI  │
    │ Infra (S3,   │  │ (Kafka, seed │  │ (React app,  │
    │ RDS, IAM)    │  │ scripts)     │  │ stub auth)   │
    └──────┬───────┘  └──────┬───────┘  └──────────────┘
           │                 │
           └────────┬────────┘
                    ▼
           ┌──────────────┐
           │ Phase 4:     │
           │ Spark + Hive │
           └──────────────┘
```

**Critical path:** Schema → everything else  
**Can parallelize after schema:** Infra, ETL, UI (3 chats)  
**Blocked on infra + ETL:** Spark/Hive

---

## Phase 1: Data Model (BLOCKING)

- [ ] ER diagram
- [ ] PostgreSQL DDL
- [ ] Seed data generation scripts
  - [ ] JSON → studios, films, franchises
  - [ ] Character files → characters, talent, joins
  - [ ] Box office faker → box_office_daily

## Phase 2: AWS Infrastructure (after schema)

- [ ] S3 buckets: `hyperion-raw`, `hyperion-staging`, `hyperion-processed`
- [ ] RDS PostgreSQL instance (or EC2 + self-hosted)
- [ ] IAM roles: ETL runner, Spark runner, web app
- [ ] Terraform templates (optional but recommended)

## Phase 3: ETL Pipeline (after schema)

- [ ] Kafka setup (local Docker or MSK)
- [ ] Topics: `raw-films`, `raw-characters`, `raw-boxoffice`
- [ ] Producers: read source files → Kafka
- [ ] Consumers: Kafka → staging → normalized tables
- [ ] Validation / deduplication logic

## Phase 4: Analytics (after infra + ETL)

- [ ] Hive external tables over S3
- [ ] Spark jobs:
  - [ ] Revenue by studio/year
  - [ ] Character counts by franchise
  - [ ] Voice actor filmography stats
- [ ] Materialized views or summary tables

## Phase 5: Web UI (after schema, can stub data)

- [ ] React scaffold (Vite or CRA)
- [ ] Stubbed auth (hardcoded user, no OAuth)
- [ ] Routes:
  - [ ] `/` — aggregate dashboard
  - [ ] `/studio/:id` — per-subsidiary view
  - [ ] `/film/:id` — film detail
  - [ ] `/character/:id` — character detail
- [ ] Charts: revenue over time, character counts, etc.

---

## Immediate Next Steps

1. **This chat:** Nail down ER diagram + DDL (Phase 1)
2. **Spin up Chat 2:** AWS infra (once schema exists)
3. **Spin up Chat 3:** Kafka + seed scripts (once schema exists)
4. **Spin up Chat 4:** React scaffold + routing (can start now with mock data)

Ready to start on the ER diagram?
