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

# Create Docker network if not exists
echo "🔗 Checking Docker network..."
if ! docker network ls | grep -q "data-pipeline-network"; then
    echo "Creating data-pipeline-network..."
    docker network create data-pipeline-network
else
    echo "✅ Network already exists"
fi

# Start infrastructure first
echo "🐳 Starting infrastructure services..."
docker compose -f docker/infrastructure/docker-compose.yml up -d

echo "⏳ Waiting for infrastructure to be ready..."
sleep 10

# Start Airflow
echo "🐳 Starting Airflow services..."
docker compose -f docker/airflow/docker-compose.yml up -d

echo "⏳ Waiting for Airflow to be ready..."
sleep 15

# Check service health
echo "🔍 Checking service health..."
echo ""
echo "Infrastructure:"
docker compose -f docker/infrastructure/docker-compose.yml ps
echo ""
echo "Airflow:"
docker compose -f docker/airflow/docker-compose.yml ps

echo ""
echo "✅ Services started successfully!"
echo ""
echo "📊 Access points:"
echo "  - Airflow UI: http://localhost:8080 (airflow/airflow)"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  - MinIO API: http://localhost:9000"
echo ""
echo "📝 Logs:"
echo "  - Infrastructure: docker compose -f docker/infrastructure/docker-compose.yml logs -f"
echo "  - Airflow: docker compose -f docker/airflow/docker-compose.yml logs -f"
echo ""
echo "🛑 Stop: ./scripts/local-down.sh"
