# Kafka Producers

Produces data to Kafka topics for downstream HDFS landing and Spark processing.

## Setup

```bash
pip install -r requirements.txt
```

## Producers

### films_producer.py
Extracts film data from character JSON files and publishes to `raw-films` topic.

```bash
# Produce all films
python films_producer.py

# Dry run
python films_producer.py --dry-run
```

### characters_producer.py
Extracts character data with appearances, voice actors, etc. Publishes to `raw-characters` topic.

```bash
# Produce all characters
python characters_producer.py

# Dry run with limit
python characters_producer.py --dry-run --limit 10
```

### boxoffice_producer.py
Generates synthetic daily box office revenue data. Publishes to `raw-boxoffice` topic.

```bash
# Generate 90 days of data
python boxoffice_producer.py

# Generate 30 days from specific date
python boxoffice_producer.py --start-date 2023-06-01 --days 30

# With reproducible random seed
python boxoffice_producer.py --seed 42
```

## Configuration

Environment variables (or `.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `DATA_DIR` | `../data` | Path to source JSON files |
| `BATCH_SIZE` | `100` | Producer batch size |

## Topics Created

| Topic | Description |
|-------|-------------|
| `raw-films` | Film metadata (title, year, studio, franchise) |
| `raw-characters` | Character data with appearances |
| `raw-boxoffice` | Daily state-level revenue events |
