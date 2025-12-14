#!/bin/bash
# Initialize HDFS directories for Hyperion data pipeline
# Run this after the Hadoop containers are healthy

set -e

echo "Initializing HDFS directories..."

# Create raw landing zone directories
docker exec hyperion-namenode hdfs dfs -mkdir -p /raw/films
docker exec hyperion-namenode hdfs dfs -mkdir -p /raw/characters
docker exec hyperion-namenode hdfs dfs -mkdir -p /raw/boxoffice

# Create processed zone for Parquet files
docker exec hyperion-namenode hdfs dfs -mkdir -p /processed/films
docker exec hyperion-namenode hdfs dfs -mkdir -p /processed/characters
docker exec hyperion-namenode hdfs dfs -mkdir -p /processed/boxoffice
docker exec hyperion-namenode hdfs dfs -mkdir -p /processed/media

# Create analytics export zone
docker exec hyperion-namenode hdfs dfs -mkdir -p /analytics/exports

# Set permissions (open for development)
docker exec hyperion-namenode hdfs dfs -chmod -R 777 /raw
docker exec hyperion-namenode hdfs dfs -chmod -R 777 /processed
docker exec hyperion-namenode hdfs dfs -chmod -R 777 /analytics

echo "HDFS directories created successfully!"
echo ""
echo "Directory structure:"
docker exec hyperion-namenode hdfs dfs -ls -R /
