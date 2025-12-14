# Kafka-to-HDFS Consumers

Consumes from Kafka topics and lands data as newline-delimited JSON in HDFS.

## Setup

```bash
pip install -r requirements.txt
```

## Components

### hdfs_consumer.py
Generic consumer that writes batches to HDFS via WebHDFS API.

```bash
# Consume films topic
python hdfs_consumer.py --topic raw-films --hdfs-path /raw/films

# Consume with custom batch settings
python hdfs_consumer.py --topic raw-boxoffice --hdfs-path /raw/boxoffice \
    --batch-size 500 --batch-timeout 10000

# Test with message limit
python hdfs_consumer.py --topic raw-films --hdfs-path /raw/films --max-messages 100
```

### run_all_consumers.py
Runs consumers for all topics in parallel.

```bash
# Run all consumers
python run_all_consumers.py

# Run specific topics
python run_all_consumers.py --topics raw-films raw-characters
```

### hdfs_client.py
WebHDFS client library for directory and file operations.

## Configuration

Environment variables (or `.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `CONSUMER_GROUP_ID` | `hyperion-hdfs-consumer` | Consumer group |
| `HDFS_HOST` | `localhost` | HDFS NameNode host |
| `HDFS_PORT` | `9870` | WebHDFS port |
| `HDFS_USER` | `root` | HDFS user |
| `BATCH_SIZE` | `100` | Messages per batch |
| `BATCH_TIMEOUT_MS` | `5000` | Flush timeout |

## HDFS Output Structure

```
/raw/
├── films/
│   └── 2024-01-15/
│       └── batch_20240115_123456_789012.json
├── characters/
│   └── 2024-01-15/
│       └── batch_20240115_123457_789012.json
└── boxoffice/
    └── 2024-01-15/
        └── batch_20240115_123458_789012.json
```

Files are newline-delimited JSON (NDJSON), partitioned by date.

## Running the Pipeline

1. Start the Docker stack:
   ```bash
   ./scripts/start-stack.sh
   ```

2. Create Kafka topics:
   ```bash
   ./scripts/create-kafka-topics.sh
   ```

3. Run producers (in separate terminal):
   ```bash
   cd producers
   python films_producer.py
   python characters_producer.py
   python boxoffice_producer.py
   ```

4. Run consumers:
   ```bash
   cd consumers
   python run_all_consumers.py
   ```

5. Verify data in HDFS:
   ```bash
   docker exec hyperion-namenode hdfs dfs -ls -R /raw
   ```
