#!/bin/bash
# Production restart script

set -e

echo "🔄 Restarting LinkedIn Jobs ETL - Production"

PROJECT_DIR="/opt/linkedin-jobs-etl"
cd $PROJECT_DIR || exit 1

# Restart infrastructure
echo "🔄 Restarting infrastructure services..."
docker compose -f docker/infrastructure/docker-compose.yml restart

echo "⏳ Waiting for infrastructure..."
sleep 10

# Restart Airflow
echo "🔄 Restarting Airflow services..."
docker compose -f docker/airflow/docker-compose.yml restart

# Wait for services
echo "⏳ Waiting for services..."
sleep 10

# Health check
echo "🔍 Running health check..."
./scripts/health-check.sh

echo "✅ Services restarted successfully!"
