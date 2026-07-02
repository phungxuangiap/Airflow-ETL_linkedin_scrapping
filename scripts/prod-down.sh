#!/bin/bash
# Production shutdown script

set -e

echo "🛑 Stopping LinkedIn Jobs ETL - Production"

PROJECT_DIR="${PROJECT_DIR:-/opt/linkedin-jobs-etl}"
cd "$PROJECT_DIR" || exit 1

if sudo docker compose version >/dev/null 2>&1; then
    COMPOSE_BIN="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_BIN="docker-compose"
else
    echo "❌ Docker Compose is not installed"
    exit 1
fi

DOCKER_GID="$(stat -c '%g' /var/run/docker.sock 2>/dev/null || echo 989)"
AIRFLOW_UID="${AIRFLOW_UID:-50000}"
SUPERSET_ENV_FILE="${SUPERSET_ENV_FILE:-../../.env.prod}"
export DOCKER_GID AIRFLOW_UID SUPERSET_ENV_FILE

compose_cmd() {
    sudo env DOCKER_GID="$DOCKER_GID" AIRFLOW_UID="$AIRFLOW_UID" SUPERSET_ENV_FILE="$SUPERSET_ENV_FILE" $COMPOSE_BIN "$@"
}

# Stop Airflow first
echo "Stopping Airflow services..."
compose_cmd -f docker/airflow/docker-compose.yml down -v --remove-orphans

# Stop Superset before Trino/backing services
echo "Stopping Superset services..."
compose_cmd -f docker/infrastructure/docker-compose.superset.yml down -v --remove-orphans

# Stop Trino before backing services
echo "Stopping Trino service..."
compose_cmd -f trino/docker-compose.yml down -v --remove-orphans

# Stop infrastructure
echo "Stopping infrastructure services..."
compose_cmd -f docker/infrastructure/docker-compose.yml down -v --remove-orphans

echo "✅ Services stopped and volumes removed successfully!"
