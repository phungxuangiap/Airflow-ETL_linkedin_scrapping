.PHONY: help infra-up infra-down airflow-up airflow-down up down restart health-check clean logs ps trigger rebuild-etl reset-superset

help:
	@echo "LinkedIn Jobs ETL - Available Commands"
	@echo ""
	@echo "Quick Start:"
	@echo "  make up             - Start all services (infrastructure + Airflow)"
	@echo "  make down           - Stop all services"
	@echo "  make restart        - Restart all services"
	@echo "  make trigger        - Trigger the ETL pipeline"
	@echo "  make rebuild-etl    - Rebuild ETL Docker image (after code changes)"
	@echo "  make reset-superset - Reset local Superset metadata DB and recreate Trino connection"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make infra-up       - Start infrastructure only (MinIO, PostgreSQL)"
	@echo "  make infra-down     - Stop infrastructure"
	@echo "  make infra-logs     - View infrastructure logs"
	@echo ""
	@echo "Airflow:"
	@echo "  make airflow-up     - Start Airflow only"
	@echo "  make airflow-down   - Stop Airflow"
	@echo "  make airflow-logs   - View Airflow logs"
	@echo ""
	@echo "Monitoring:"
	@echo "  make health-check   - Run health checks on all services"
	@echo "  make logs           - View all service logs"
	@echo "  make ps             - Show service status"
	@echo ""
	@echo "Production:"
	@echo "  make prod-deploy    - Deploy to production"
	@echo "  make prod-restart   - Restart production services"
	@echo "  make prod-down      - Stop production services"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean          - Clean up temporary files and volumes"

# Quick commands
up:
	@chmod +x scripts/local-up.sh
	@./scripts/local-up.sh

down:
	@chmod +x scripts/local-down.sh
	@./scripts/local-down.sh

restart:
	@chmod +x scripts/infrastructure.sh scripts/airflow.sh
	@./scripts/infrastructure.sh restart
	@./scripts/airflow.sh restart

trigger:
	@chmod +x scripts/airflow.sh
	@./scripts/airflow.sh trigger

rebuild-etl:
	@chmod +x scripts/rebuild-etl.sh
	@./scripts/rebuild-etl.sh

reset-superset:
	@chmod +x scripts/reset-superset.sh
	@./scripts/reset-superset.sh

# Infrastructure commands
infra-up:
	@chmod +x scripts/infrastructure.sh
	@./scripts/infrastructure.sh start

infra-down:
	@chmod +x scripts/infrastructure.sh
	@./scripts/infrastructure.sh stop

infra-logs:
	@docker compose -f docker/infrastructure/docker-compose.yml logs -f

# Airflow commands
airflow-up:
	@chmod +x scripts/airflow.sh
	@./scripts/airflow.sh start

airflow-down:
	@chmod +x scripts/airflow.sh
	@./scripts/airflow.sh stop

airflow-logs:
	@docker compose -f docker/airflow/docker-compose.yml logs -f

# Production commands
prod-deploy:
	@chmod +x scripts/prod-deploy.sh
	@./scripts/prod-deploy.sh

prod-restart:
	@chmod +x scripts/prod-restart.sh
	@./scripts/prod-restart.sh

prod-down:
	@chmod +x scripts/prod-down.sh
	@./scripts/prod-down.sh

# Monitoring commands
health-check:
	@chmod +x scripts/health-check.sh
	@./scripts/health-check.sh

logs:
	@echo "Infrastructure logs:"
	@docker compose -f docker/infrastructure/docker-compose.yml logs --tail=50
	@echo ""
	@echo "Airflow logs:"
	@docker compose -f docker/airflow/docker-compose.yml logs --tail=50

ps:
	@echo "Infrastructure services:"
	@docker compose -f docker/infrastructure/docker-compose.yml ps
	@echo ""
	@echo "Airflow services:"
	@docker compose -f docker/airflow/docker-compose.yml ps

# Cleanup
clean:
	@echo "🧹 Cleaning up..."
	@docker compose -f docker/airflow/docker-compose.yml down -v
	@docker compose -f docker/infrastructure/docker-compose.superset.yml down -v
	@docker compose -f docker/infrastructure/docker-compose.yml down -v
	@rm -rf data/raw/* data/bronze/* data/silver/* data/gold/* 2>/dev/null || true
	@rm -rf tmp/api_sources/* tmp/scrapping_script/* 2>/dev/null || true
	@rm -rf logs/airflow/* 2>/dev/null || true
	@echo "✅ Cleanup complete!"
