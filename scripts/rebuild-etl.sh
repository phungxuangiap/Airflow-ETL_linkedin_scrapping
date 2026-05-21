#!/bin/bash
# Rebuild ETL Docker image
# Use this when you update ETL code (src/, main.py, requirements.txt)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo "🔨 Rebuilding ETL Docker image..."

# Build ETL image
docker build -f Dockerfile.etl -t linkedin-etl:latest .

echo ""
echo "✅ ETL image rebuilt successfully!"
echo ""
echo "📦 Image info:"
docker images | grep linkedin-etl

echo ""
echo "💡 Next steps:"
echo "  - Trigger DAG: docker exec airflow-scheduler airflow dags trigger linkedin_jobs_pipeline_docker"
echo "  - Or wait for scheduled run"
