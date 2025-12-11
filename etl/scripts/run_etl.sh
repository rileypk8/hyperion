#!/bin/bash
# Hyperion ETL Pipeline Runner
# Convenience script to run the full ETL pipeline

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ETL_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ETL_DIR"

# Check if Docker containers are running
if ! docker ps | grep -q hyperion-kafka; then
    echo "Kafka is not running. Starting infrastructure..."
    docker-compose up -d

    echo "Waiting for services to be ready..."
    sleep 10

    echo "Creating Kafka topics..."
    ./scripts/create_topics.sh
fi

# Check if Python dependencies are installed
if ! python -c "import confluent_kafka" 2>/dev/null; then
    echo "Installing Python dependencies..."
    pip install -r requirements.txt
fi

# Run the pipeline
echo ""
echo "Starting ETL Pipeline..."
echo ""

python run_pipeline.py "$@"
