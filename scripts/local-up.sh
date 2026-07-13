#!/bin/bash
# Local development startup script

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo "🚀 Starting LinkedIn Jobs ETL - Local Environment"

# Load environment variables
if [ -f .env.local ]; then
    export $(cat .env.local | grep -v '^#' | xargs)
    export SUPERSET_ENV_FILE=../../.env.local
    echo "✅ Loaded .env.local"
else
    echo "⚠️  .env.local not found. Using defaults"
fi

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/raw data/bronze data/silver data/gold
mkdir -p logs/airflow
mkdir -p tmp/api_sources tmp/scrapping_script
mkdir -p plugins

# Ensure correct permissions
chmod -R 777 tmp/ logs/ 2>/dev/null || true

# Create Docker network if not exists
echo "🔗 Checking Docker network..."
if ! docker network ls | grep -q "data-pipeline-network"; then
    echo "Creating data-pipeline-network..."
    docker network create data-pipeline-network
else
    echo "✅ Network already exists"
fi

# Build ETL Docker image
echo "🔨 Building ETL Docker image..."
docker build -f Dockerfile.etl -t linkedin-etl:latest .

# Start infrastructure first
echo "🐳 Starting infrastructure services..."
docker compose -f docker/infrastructure/docker-compose.yml up -d

echo "⏳ Waiting for infrastructure to be ready..."
sleep 10

# Start Trino
echo "🐳 Starting Trino service..."
docker compose -f trino/docker-compose.yml up -d

# Start Superset after Trino is available for SQL connections
echo "🐳 Starting Superset services..."
docker compose -f docker/infrastructure/docker-compose.superset.yml build
docker compose -f docker/infrastructure/docker-compose.superset.yml up -d superset-db superset-redis
docker compose -f docker/infrastructure/docker-compose.superset.yml run --rm superset-init
docker compose -f docker/infrastructure/docker-compose.superset.yml up -d superset

# Start Airflow
echo "🐳 Starting Airflow services..."
docker compose -f docker/airflow/docker-compose.yml build
docker compose -f docker/airflow/docker-compose.yml up -d

echo "⏳ Waiting for Airflow to be ready..."
sleep 15

# Check service health
echo "🔍 Checking service health..."
echo ""
echo "Infrastructure:"
docker compose -f docker/infrastructure/docker-compose.yml ps
echo ""
echo "Trino:"
docker compose -f trino/docker-compose.yml ps
echo ""
echo "Superset:"
docker compose -f docker/infrastructure/docker-compose.superset.yml ps
echo ""
echo "Airflow:"
docker compose -f docker/airflow/docker-compose.yml ps

echo ""
echo "✅ Services started successfully!"
echo ""
echo "📊 Access points:"
echo "  - Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "  - Trino: http://localhost:8081"
echo "  - Superset: http://localhost:8089 (admin/admin)"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  - MinIO API: http://localhost:9000"
echo ""
echo "📝 Logs:"
echo "  - Infrastructure: docker compose -f docker/infrastructure/docker-compose.yml logs -f"
echo "  - Trino: docker compose -f trino/docker-compose.yml logs -f"
echo "  - Superset: docker compose -f docker/infrastructure/docker-compose.superset.yml logs -f"
echo "  - Airflow: docker compose -f docker/airflow/docker-compose.yml logs -f"
echo ""
echo "🛑 Stop: ./scripts/local-down.sh"
