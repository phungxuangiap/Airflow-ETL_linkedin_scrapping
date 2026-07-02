#!/bin/bash
# Health check script for all services

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

DOCKER="${DOCKER:-sudo docker}"

echo "🔍 Running health checks..."

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check service health
check_service() {
    local service_name=$1
    local health_url=$2
    local max_retries=5
    local retry_count=0

    while [ $retry_count -lt $max_retries ]; do
        if curl -f -s "$health_url" > /dev/null 2>&1; then
            echo -e "${GREEN}✅ $service_name is healthy${NC}"
            return 0
        fi
        retry_count=$((retry_count + 1))
        echo "⏳ Waiting for $service_name... (attempt $retry_count/$max_retries)"
        sleep 5
    done

    echo -e "${RED}❌ $service_name is not healthy${NC}"
    return 1
}

echo ""
echo -e "${YELLOW}=== Infrastructure Services ===${NC}"

# Check MinIO
check_service "MinIO" "http://localhost:9000/minio/health/live"

# Check PostgreSQL (Iceberg catalog)
if $DOCKER exec postgres-iceberg pg_isready -U iceberg > /dev/null 2>&1; then
    echo -e "${GREEN}✅ PostgreSQL (Iceberg) is healthy${NC}"
else
    echo -e "${RED}❌ PostgreSQL (Iceberg) is not healthy${NC}"
fi

echo ""
echo -e "${YELLOW}=== Trino Services ===${NC}"

# Check Trino Coordinator
check_service "Trino Coordinator" "http://localhost:8081/v1/info"

echo ""
echo -e "${YELLOW}=== Superset Services ===${NC}"

# Check Superset Webserver
check_service "Superset Webserver" "http://localhost:8088/health"

echo ""
echo -e "${YELLOW}=== Airflow Services ===${NC}"

# Check Airflow Webserver
check_service "Airflow Webserver" "http://localhost:8080/health"

# Check PostgreSQL (Airflow metadata)
if $DOCKER exec airflow-postgres pg_isready -U airflow > /dev/null 2>&1; then
    echo -e "${GREEN}✅ PostgreSQL (Airflow) is healthy${NC}"
else
    echo -e "${RED}❌ PostgreSQL (Airflow) is not healthy${NC}"
fi

echo ""
echo -e "${GREEN}✅ All services are healthy!${NC}"
