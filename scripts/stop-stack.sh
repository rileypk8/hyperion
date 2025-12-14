#!/bin/bash
# Stop the Hyperion big data stack

set -e

cd "$(dirname "$0")/.."

echo "Stopping Hyperion Big Data Stack..."
docker-compose down

echo ""
echo "Stack stopped."
echo ""
echo "To also remove volumes (all data): docker-compose down -v"
