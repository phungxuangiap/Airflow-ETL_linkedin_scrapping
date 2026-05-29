#!/bin/bash
# Local development shutdown script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo "🛑 Stopping LinkedIn Jobs ETL - Local Environment"

# Stop Airflow first
echo "Stopping Airflow services..."
docker compose -f docker/airflow/docker-compose.yml down

# Stop Trino before backing services
echo "Stopping Trino service..."
docker compose -f trino/docker-compose.yml down

# Stop infrastructure
echo "Stopping infrastructure services..."
docker compose -f docker/infrastructure/docker-compose.yml down

echo ""
echo "✅ Services stopped successfully!"
echo ""
echo "💡 To remove all data (volumes):"
echo "  docker compose -f docker/infrastructure/docker-compose.yml down -v"
echo "  docker compose -f trino/docker-compose.yml down -v"
echo "  docker compose -f docker/airflow/docker-compose.yml down -v"
