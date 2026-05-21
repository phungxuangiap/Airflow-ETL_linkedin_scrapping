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

# Ensure Docker is installed and running
if ! command -v docker >/dev/null 2>&1; then
    echo "Installing Docker..."
    sudo dnf install -y docker
fi

sudo systemctl enable docker
sudo systemctl start docker

DOCKER="sudo docker"

# Build ETL Docker image
echo "🔨 Building ETL Docker image..."
$DOCKER build -f Dockerfile.etl -t linkedin-etl:latest .

# Stop existing services
echo "🛑 Stopping existing services..."
$DOCKER compose -f docker/airflow/docker-compose.yml down
$DOCKER compose -f docker/infrastructure/docker-compose.yml down

# Pull latest Docker images
echo "🐳 Pulling Docker images..."
$DOCKER compose -f docker/infrastructure/docker-compose.yml pull
$DOCKER compose -f docker/airflow/docker-compose.yml pull

# Start infrastructure first
echo "🚀 Starting infrastructure services..."
$DOCKER compose -f docker/infrastructure/docker-compose.yml up -d

echo "⏳ Waiting for infrastructure..."
sleep 10

# Start Airflow
echo "🚀 Starting Airflow services..."
$DOCKER compose -f docker/airflow/docker-compose.yml up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be ready..."
sleep 15

# Run health check
echo "🔍 Running health check..."
./scripts/health-check.sh

echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "📊 Service status:"
echo "Infrastructure:"
$DOCKER compose -f docker/infrastructure/docker-compose.yml ps
echo ""
echo "Airflow:"
$DOCKER compose -f docker/airflow/docker-compose.yml ps
