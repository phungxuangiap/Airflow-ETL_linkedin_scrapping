#!/bin/bash
# Production deployment script (runs on EC2)

set -e

echo "🚀 Deploying LinkedIn Jobs ETL - Production"

# Configuration
PROJECT_DIR="/opt/linkedin-jobs-etl"
REPO_URL="https://github.com/your-username/linkedin-jobs-etl.git"  # Update with your repo
BRANCH="main"

# Navigate to project directory
cd $PROJECT_DIR || exit 1

# Sync latest code
echo "📥 Syncing latest code from $BRANCH..."
git fetch origin $BRANCH
git checkout $BRANCH
git reset --hard origin/$BRANCH

# Create necessary directories
echo "📁 Creating directories..."
mkdir -p data/raw data/bronze data/silver data/gold
mkdir -p logs/airflow
mkdir -p tmp/api_sources tmp/scrapping_script
mkdir -p plugins

# Ensure correct permissions
chmod -R 777 tmp/ logs/ 2>/dev/null || true

DEPLOY_GROQ_API_KEY="${GROQ_API_KEY:-}"

# Load production environment variables
if [ -f .env.prod ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        case "$line" in
            ''|'#'*) continue ;;
        esac

        key="${line%%=*}"
        value="${line#*=}"
        value="${value%%#*}"
        value="${value%"${value##*[![:space:]]}"}"
        export "$key=$value"
    done < .env.prod
    echo "✅ Loaded .env.prod"
else
    echo "❌ .env.prod not found"
    exit 1
fi

if { [ -z "${GROQ_API_KEY:-}" ] || [ "$GROQ_API_KEY" = "CHANGE_ME" ]; } && [ -n "$DEPLOY_GROQ_API_KEY" ]; then
    GROQ_API_KEY="$DEPLOY_GROQ_API_KEY"
fi

if [ -n "${GROQ_API_KEY:-}" ] && [ "$GROQ_API_KEY" != "CHANGE_ME" ]; then
    export GROQ_API_KEY
    echo "✅ Loaded GROQ_API_KEY from deployment environment"
elif [ -z "${GROQ_API_KEY:-}" ] || [ "$GROQ_API_KEY" = "CHANGE_ME" ]; then
    echo "⚠️  GROQ_API_KEY is not configured. Set GitHub Actions secret GROQ_API_KEY or update .env.prod"
fi

cp .env.prod .env
if grep -q '^GROQ_API_KEY=' .env; then
    python3 - <<'PY'
import os
from pathlib import Path

env_path = Path(".env")
groq_api_key = os.environ.get("GROQ_API_KEY", "")
lines = env_path.read_text(encoding="utf-8").splitlines()
updated = []
for line in lines:
    if line.startswith("GROQ_API_KEY="):
        updated.append(f"GROQ_API_KEY={groq_api_key}")
    else:
        updated.append(line)
env_path.write_text("\n".join(updated) + "\n", encoding="utf-8")
PY
else
    printf '\nGROQ_API_KEY=%s\n' "$GROQ_API_KEY" >> .env
fi
echo "✅ Wrote runtime .env from .env.prod"

# Ensure Docker is installed and running
if ! command -v docker >/dev/null 2>&1; then
    echo "Installing Docker..."
    sudo dnf install -y docker
fi

sudo systemctl enable docker
sudo systemctl start docker

if sudo docker compose version >/dev/null 2>&1; then
    COMPOSE_BIN="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_BIN="docker-compose"
else
    echo "Installing Docker Compose..."
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    COMPOSE_BIN="docker-compose"
fi

DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"
AIRFLOW_UID="${AIRFLOW_UID:-50000}"
export DOCKER_GID AIRFLOW_UID
echo "🐳 Docker socket group id: $DOCKER_GID"

docker_cmd() {
    sudo docker "$@"
}

compose_cmd() {
    sudo env DOCKER_GID="$DOCKER_GID" AIRFLOW_UID="$AIRFLOW_UID" $COMPOSE_BIN "$@"
}

# Build ETL Docker image
echo "🔨 Building ETL Docker image..."
docker_cmd build -f Dockerfile.etl -t linkedin-etl:latest .

# Ensure shared Docker network exists
echo "🌐 Ensuring Docker network exists..."
docker_cmd network create data-pipeline-network >/dev/null 2>&1 || true

# Stop existing services
echo "🛑 Stopping existing services..."
compose_cmd -f docker/airflow/docker-compose.yml down
compose_cmd -f docker/infrastructure/docker-compose.superset.yml down
compose_cmd -f trino/docker-compose.yml down
compose_cmd -f docker/infrastructure/docker-compose.yml down

# Pull latest Docker images
echo "🐳 Pulling Docker images..."
compose_cmd -f docker/infrastructure/docker-compose.yml pull
compose_cmd -f trino/docker-compose.yml pull
compose_cmd -f docker/airflow/docker-compose.yml pull

# Start infrastructure first
echo "🚀 Starting infrastructure services..."
compose_cmd -f docker/infrastructure/docker-compose.yml up -d

echo "⏳ Waiting for infrastructure..."
sleep 10

# Start Trino
echo "🚀 Starting Trino service..."
compose_cmd -f trino/docker-compose.yml up -d --force-recreate

# Start Superset
echo "🚀 Starting Superset services..."
compose_cmd -f docker/infrastructure/docker-compose.superset.yml build
compose_cmd -f docker/infrastructure/docker-compose.superset.yml up -d superset-db superset-redis
compose_cmd -f docker/infrastructure/docker-compose.superset.yml run --rm superset-init
compose_cmd -f docker/infrastructure/docker-compose.superset.yml up -d --force-recreate superset

# Start Airflow
echo "🚀 Starting Airflow services..."
compose_cmd -f docker/airflow/docker-compose.yml up -d --force-recreate

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
sleep 15

echo "🐳 Airflow Docker socket access:"
docker_cmd exec airflow-scheduler id || true
docker_cmd exec airflow-scheduler ls -l /var/run/docker.sock || true

# Run health check
echo "🔍 Running health check..."
chmod +x scripts/health-check.sh
./scripts/health-check.sh

echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "📊 Service status:"
echo "Infrastructure:"
compose_cmd -f docker/infrastructure/docker-compose.yml ps
echo ""
echo "Trino:"
compose_cmd -f trino/docker-compose.yml ps
echo ""
echo "Superset:"
compose_cmd -f docker/infrastructure/docker-compose.superset.yml ps
echo ""
echo "Airflow:"
compose_cmd -f docker/airflow/docker-compose.yml ps
