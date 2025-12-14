# Hyperion Project Plan

## Overview

**Codename:** Hyperion  
**Domain:** Disney media properties database  
**Scope:** ETL pipeline + analytics platform demonstrating big data engineering concepts using Disney's animation portfolio as source data  
**Hosting:** Personal AWS account  
**Timeline:** ASAP (parallelize aggressively)

---

## Required Stack

| Layer | Technology | Status | Branch |
|-------|------------|--------|--------|
| Database | PostgreSQL (normalized) | ✅ Schema complete | `main` |
| ETL | Kafka | ✅ Pipeline complete | `claude/etl-work-planning-*` |
| Storage | AWS S3 | ✅ Terraform ready | `claude/hyperion-infra-planning-*` |
| Compute | Spark | Not started | — |
| Query | Hive | Not started | — |
| Cloud | AWS (IAM, access controls) | ✅ Terraform ready | `claude/hyperion-infra-planning-*` |
| Web | React + Node (auth stubbed) | ✅ Complete | `main` |

---

## Data Assets

| Asset | Status |
|-------|--------|
| Film catalog (188 films, 8 studios) | ✅ Complete |
| Character schema | ✅ Complete |
| Character files (Pixar, WDAS, Blue Sky, DisneyToon, KH, Marvel, Disney Interactive) | ✅ Complete (158 JSON files) |
| Box office faker (disaggregation provider) | ✅ Complete |
| Box office time-series data | Not generated yet |
| Game catalog (Kingdom Hearts + Disney Interactive) | ✅ Complete |
| Game sales faker | Not started |
| Soundtrack/song data | ✅ Complete (`wdas_soundtracks_complete.json`) |
| Awards data | ✅ Complete (in soundtrack data + ETL branch) |

---

## Data Model (Entities)

Fully normalized with media parent table pattern. Target tables:

**Lookup tables (7):**
| Table | Notes |
|-------|-------|
| `roles` | protagonist, villain, etc. |
| `genders` | male, female, non-binary, etc. |
| `species` | human, toy, monster, heartless, etc. |
| `platforms` | PS2, Switch, PC, etc. |
| `credit_types` | composer, lyricist, performer, etc. |
| `award_bodies` | Academy Awards, Annie Awards, The Game Awards, etc. |
| `award_categories` | FK to body, is_media_level flag |

**Core tables (11):**
| Table | Notes |
|-------|-------|
| `studios` | Pixar, WDAS, Blue Sky, etc. |
| `franchises` | Toy Story, Frozen, Kingdom Hearts, etc. |
| `media` | Parent table: title, year, franchise, media_type |
| `films` | Child of media: studio, animation_type |
| `games` | Child of media: developer, publisher |
| `game_platforms` | M:N junction for games ↔ platforms |
| `soundtracks` | Child of media: label, source_media_id |
| `songs` | Tracks on soundtracks |
| `song_credits` | M:N junction for songs ↔ talent ↔ credit_type |
| `characters` | origin_franchise, species, gender |
| `talent` | Voice actors, composers, etc. |

**Relationship tables (2):**
| Table | Notes |
|-------|-------|
| `character_appearances` | character + media + talent + role + variant |
| `nominations` | media + category + talent + song |

**Financial data (2):**
| Table | Notes |
|-------|-------|
| `box_office_daily` | Film revenue by date/state |
| `game_sales` | Game sales by date/platform/region |

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

## Phase 1: Data Model ✅ COMPLETE

- [ ] ER diagram
- [x] PostgreSQL DDL (`docs/hyperion_schema.sql`)
- [ ] Seed data generation scripts
  - [ ] JSON → studios, films, franchises
  - [ ] Character files → characters, talent, joins
  - [ ] Box office faker → box_office_daily

> **Note:** DDL complete with 22 tables, views, and indexes. Enhanced species normalization available in `claude/summarize-species-distribution-*` branch.

## Phase 2: AWS Infrastructure ✅ COMPLETE (unmerged)

- [x] S3 buckets: `hyperion-raw`, `hyperion-staging`, `hyperion-processed`
- [x] RDS PostgreSQL instance
- [x] IAM roles: ETL runner, Spark runner, web app
- [x] Terraform templates

> **Branch:** `claude/hyperion-infra-planning-*` — 9 files, 1,121 lines (VPC, S3, RDS, IAM modules)

## Phase 3: ETL Pipeline ✅ COMPLETE (unmerged)

- [x] Kafka setup (Docker Compose)
- [x] Topics: films, characters, games, soundtracks, awards, boxoffice
- [x] Producers: read source files → Kafka
- [x] Consumers: Kafka → staging → normalized tables
- [x] Validation / deduplication logic

> **Branch:** `claude/etl-work-planning-*` — 26 files, 4,384 lines (full pipeline with orchestration)

## Phase 4: Analytics ❌ NOT STARTED

- [ ] Hive external tables over S3
- [ ] Spark jobs:
  - [ ] Revenue by studio/year
  - [ ] Character counts by franchise
  - [ ] Voice actor filmography stats
- [ ] Materialized views or summary tables

> **Blocked on:** Merging infra + ETL branches, deploying to AWS

## Phase 5: Web UI ✅ COMPLETE (MVP)

- [x] React scaffold (Vite + TypeScript)
- [x] Stubbed auth (AuthContext)
- [x] Routes:
  - [x] `/` — aggregate dashboard with 6+ visualizations
  - [x] `/studio/:id` — per-subsidiary view (hub pages for WDAS, KH, generic)
  - [x] `/film/:id` — film detail
  - [x] `/character/:id` — character detail
- [x] Charts: voice actors, species breakdown, cross-media appearances, etc.
- [x] Hub-and-spoke landing page with studio visualizations

## Phase 6: Web UI Refinement ❌ NOT STARTED

- [ ] Design system / consistent styling
- [ ] Responsive layout improvements
- [ ] Component library cleanup
- [ ] Loading states and error handling
- [ ] Connect to live database (replace mock data)
- [ ] Performance optimization (lazy loading, memoization)
- [ ] Accessibility improvements
- [ ] Search and filtering functionality

> **Blocked on:** Phase 4 (analytics layer) or at minimum Phase 3 merge (ETL for live data)

---

## Immediate Next Steps

1. **Merge remaining branches to main:**
   - `claude/hyperion-infra-planning-*` (Terraform)
   - `claude/etl-work-planning-*` (Kafka ETL)
   - `claude/summarize-species-distribution-*` (Schema enhancements)

2. **Deploy infrastructure:** Run Terraform to provision AWS resources

3. **Generate seed data:** Run ETL pipeline to populate PostgreSQL

4. **Phase 4 implementation:** Spark + Hive analytics layer

5. **Phase 6 implementation:** UI refinement and live data integration

6. **Integration testing:** End-to-end validation
