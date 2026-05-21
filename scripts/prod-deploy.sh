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

# Pull latest code
echo "📥 Pulling latest code from $BRANCH..."
git fetch origin
git checkout $BRANCH
git pull origin $BRANCH

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
    export $(cat .env.prod | grep -v '^#' | xargs)
    echo "✅ Loaded .env.prod"
else
    echo "❌ .env.prod not found"
    exit 1
fi

# Stop existing services
echo "🛑 Stopping existing services..."
docker compose -f docker/airflow/docker-compose.yml down
docker compose -f docker/infrastructure/docker-compose.yml down

# Pull latest Docker images
echo "🐳 Pulling Docker images..."
docker compose -f docker/infrastructure/docker-compose.yml pull
docker compose -f docker/airflow/docker-compose.yml pull

# Start infrastructure first
echo "🚀 Starting infrastructure services..."
docker compose -f docker/infrastructure/docker-compose.yml up -d

echo "⏳ Waiting for infrastructure..."
sleep 10

# Start Airflow
echo "🚀 Starting Airflow services..."
docker compose -f docker/airflow/docker-compose.yml up -d

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
docker compose -f docker/infrastructure/docker-compose.yml ps
echo ""
echo "Airflow:"
docker compose -f docker/airflow/docker-compose.yml ps
