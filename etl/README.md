# Hyperion ETL Pipeline

Kafka-based ETL pipeline for loading Disney media data into PostgreSQL.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   JSON Files    │     │      Kafka      │     │   PostgreSQL    │
│   (Source)      │ ──▶ │    (Staging)    │ ──▶ │   (Target)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
        │                       │                       │
   Producers              Topics:              Tables:
   - films_producer      - raw-films          - studios
   - characters_producer - raw-characters     - franchises
   - boxoffice_producer  - raw-boxoffice      - media, films
                                              - characters
                                              - talent
                                              - character_appearances
                                              - box_office_daily
```

## Quick Start

### 1. Start Infrastructure

```bash
cd etl
docker-compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Schema Registry (port 8081)
- PostgreSQL (port 5432)
- Kafka UI (port 8080)

### 2. Create Topics

```bash
./scripts/create_topics.sh
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the Pipeline

```bash
# Full pipeline
python run_pipeline.py

# Dry run (preview what will be produced)
python run_pipeline.py --dry-run

# Skip box office data (faster, less data)
python run_pipeline.py --skip-boxoffice

# Only run producers
python run_pipeline.py --producers-only

# Only run consumers
python run_pipeline.py --consumers-only
```

## Components

### Producers

| Producer | Source | Kafka Topic | Records |
|----------|--------|-------------|---------|
| `FilmsProducer` | Character JSON files | `raw-films` | ~200 |
| `CharactersProducer` | Character JSON files | `raw-characters` | ~3000 |
| `BoxOfficeProducer` | Generated synthetic | `raw-boxoffice` | ~500K+ |

### Consumers

| Consumer | Kafka Topic | Target Tables |
|----------|-------------|---------------|
| `FilmsConsumer` | `raw-films` | studios, franchises, media, films |
| `CharactersConsumer` | `raw-characters` | characters, talent, character_appearances |
| `BoxOfficeConsumer` | `raw-boxoffice` | box_office_daily |

### Topics

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `raw-films` | 3 | Film/media records |
| `raw-characters` | 6 | Character records |
| `raw-boxoffice` | 6 | Box office daily data |
| `dlq-*` | 1 | Dead letter queues for failures |

## Configuration

Copy `.env.example` to `.env` and adjust as needed:

```bash
cp .env.example .env
```

Key settings:
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker address
- `POSTGRES_*` - Database connection settings
- `BATCH_SIZE` - Consumer batch processing size

## Monitoring

Kafka UI is available at http://localhost:8080 for:
- Topic inspection
- Consumer group monitoring
- Message browsing

## Data Flow

### Films Pipeline

```
JSON files → FilmsProducer → raw-films → FilmsConsumer → PostgreSQL
                                                              │
                                                              ▼
                                                    ┌─────────────────┐
                                                    │ studios         │
                                                    │ franchises      │
                                                    │ media           │
                                                    │ films           │
                                                    └─────────────────┘
```

### Characters Pipeline

```
JSON files → CharactersProducer → raw-characters → CharactersConsumer → PostgreSQL
                                                                              │
                                                                              ▼
                                                                    ┌─────────────────┐
                                                                    │ characters      │
                                                                    │ talent          │
                                                                    │ character_      │
                                                                    │   appearances   │
                                                                    └─────────────────┘
```

### Box Office Pipeline

```
FilmsProducer data → BoxOfficeProducer → raw-boxoffice → BoxOfficeConsumer → PostgreSQL
    (for film list)    (generates fake)                                           │
                                                                                  ▼
                                                                        ┌─────────────────┐
                                                                        │ box_office_daily│
                                                                        └─────────────────┘
```

## Development

### Running Individual Components

```bash
# Individual producers
python -m producers.films_producer --dry-run
python -m producers.characters_producer --dry-run
python -m producers.boxoffice_producer --dry-run --limit-films 3

# Individual consumers
python -m consumers.films_consumer
python -m consumers.characters_consumer
python -m consumers.boxoffice_consumer
```

### Testing

```bash
pytest tests/
```

## Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove all data volumes
docker-compose down -v
```
