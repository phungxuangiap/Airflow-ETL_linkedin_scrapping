#!/bin/bash
# Production shutdown script

set -e

echo "🛑 Stopping LinkedIn Jobs ETL - Production"

PROJECT_DIR="/opt/linkedin-jobs-etl"
cd $PROJECT_DIR || exit 1

# Stop Airflow first
echo "Stopping Airflow services..."
docker compose -f docker/airflow/docker-compose.yml down

# Stop Trino before backing services
echo "Stopping Trino service..."
docker compose -f trino/docker-compose.yml down

# Stop infrastructure
echo "Stopping infrastructure services..."
docker compose -f docker/infrastructure/docker-compose.yml down

echo "✅ Services stopped successfully!"
