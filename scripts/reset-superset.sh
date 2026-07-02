#!/bin/bash
# Reset local Superset metadata database and recreate the Trino connection.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

if [ -f .env.local ]; then
    export $(grep -v '^#' .env.local | xargs)
    export SUPERSET_ENV_FILE=../../.env.local
fi

echo "🧹 Resetting Superset metadata database and cache volumes..."
docker compose -f docker/infrastructure/docker-compose.superset.yml down -v --remove-orphans

echo "🔨 Rebuilding Superset image..."
docker compose -f docker/infrastructure/docker-compose.superset.yml build --no-cache

echo "🚀 Starting Superset backing services..."
docker compose -f docker/infrastructure/docker-compose.superset.yml up -d superset-db superset-redis

echo "🧱 Initializing Superset metadata and Trino connection..."
docker compose -f docker/infrastructure/docker-compose.superset.yml run --rm superset-init

echo "🚀 Starting Superset webserver..."
docker compose -f docker/infrastructure/docker-compose.superset.yml up -d superset

echo "✅ Superset reset completed. Open http://localhost:8088"
