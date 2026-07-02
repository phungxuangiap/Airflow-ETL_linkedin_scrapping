#!/bin/bash
# Destroy production containers/volumes and deploy a fresh stack.

set -e

echo "♻️  Renewing LinkedIn Jobs ETL - Production"

PROJECT_DIR="${PROJECT_DIR:-/opt/linkedin-jobs-etl}"
CONFIRM_RENEW_PROD="${CONFIRM_RENEW_PROD:-}"

if [ "$CONFIRM_RENEW_PROD" != "RENEW_PROD" ]; then
    echo "❌ Refusing to renew production. Set CONFIRM_RENEW_PROD=RENEW_PROD to continue."
    exit 1
fi

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
export DOCKER_GID AIRFLOW_UID

docker_cmd() {
    sudo docker "$@"
}

compose_cmd() {
    sudo env DOCKER_GID="$DOCKER_GID" AIRFLOW_UID="$AIRFLOW_UID" $COMPOSE_BIN "$@"
}

echo "🛑 Destroying Airflow services and volumes..."
compose_cmd -f docker/airflow/docker-compose.yml down -v --remove-orphans || true

echo "🛑 Destroying Trino services..."
compose_cmd -f trino/docker-compose.yml down -v --remove-orphans || true

echo "🛑 Destroying infrastructure services and volumes..."
compose_cmd -f docker/infrastructure/docker-compose.yml down -v --remove-orphans || true

echo "🧹 Removing ETL image..."
docker_cmd image rm linkedin-etl:latest >/dev/null 2>&1 || true

echo "🚀 Recreating production stack..."
chmod +x scripts/prod-deploy.sh
./scripts/prod-deploy.sh

echo "✅ Production renewed successfully!"
