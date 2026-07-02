#!/bin/bash
# Reset Superset metadata database and recreate the Trino connection.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

if [ -n "${SUPERSET_ENV_FILE:-}" ]; then
    ENV_FILE="${SUPERSET_ENV_FILE#../../}"
elif [ -f .env.local ]; then
    ENV_FILE=".env.local"
    export SUPERSET_ENV_FILE=../../.env.local
else
    ENV_FILE=".env"
    export SUPERSET_ENV_FILE=../../.env
fi

if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Environment file not found: $ENV_FILE"
    exit 1
fi

while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
        ''|'#'*) continue ;;
    esac

    key="${line%%=*}"
    value="${line#*=}"
    value="${value%%#*}"
    value="${value%"${value##*[![:space:]]}"}"
    export "$key=$value"
done < "$ENV_FILE"

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
