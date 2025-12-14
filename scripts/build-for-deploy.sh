#!/bin/bash
# Build frontend and package with Parquet data for static deployment

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
WEB_DIR="$PROJECT_DIR/web"
OUTPUT_DIR="$PROJECT_DIR/dist"
DATA_DIR="$OUTPUT_DIR/data"

echo "=========================================="
echo "Building Hyperion for Static Deployment"
echo "=========================================="

# Clean previous build
echo ""
echo "[1/4] Cleaning previous build..."
rm -rf "$OUTPUT_DIR"

# Build frontend
echo ""
echo "[2/4] Building frontend..."
cd "$WEB_DIR"
npm install
npm run build

# Move build output to project root dist/
mv "$WEB_DIR/dist" "$OUTPUT_DIR"

# Export Parquet files from HDFS
echo ""
echo "[3/4] Exporting Parquet data from HDFS..."
mkdir -p "$DATA_DIR"

# Create temp dir in container and copy files out
docker exec hyperion-namenode bash -c "rm -rf /tmp/exports && mkdir -p /tmp/exports"
docker exec hyperion-namenode hdfs dfs -get /analytics/exports/* /tmp/exports/ 2>/dev/null || true

# Copy each export directory (Spark writes directories, not files)
for table in films_complete characters_complete boxoffice_summary studio_stats; do
    echo "  Exporting $table..."
    # Get the actual parquet file from the directory
    docker exec hyperion-namenode bash -c "
        if [ -d /tmp/exports/$table ]; then
            # Find the .parquet file (not _SUCCESS or metadata)
            parquet_file=\$(ls /tmp/exports/$table/*.parquet 2>/dev/null | head -1)
            if [ -n \"\$parquet_file\" ]; then
                cp \"\$parquet_file\" /tmp/exports/${table}.parquet
            fi
        fi
    "
    docker cp hyperion-namenode:/tmp/exports/${table}.parquet "$DATA_DIR/${table}.parquet" 2>/dev/null || echo "  Warning: $table not found"
done

# Summary
echo ""
echo "[4/4] Build complete!"
echo ""
echo "=========================================="
echo "Output: $OUTPUT_DIR"
echo "=========================================="
echo ""
echo "Contents:"
ls -la "$OUTPUT_DIR"
echo ""
echo "Data files:"
ls -la "$DATA_DIR" 2>/dev/null || echo "  (no data files - run Spark jobs first)"
echo ""
echo "To deploy:"
echo "  1. Upload contents of $OUTPUT_DIR to your static host"
echo "  2. Ensure your host serves index.html for all routes (SPA)"
echo ""
echo "Total size:"
du -sh "$OUTPUT_DIR"
