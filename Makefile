# ─────────────────────────────────────────────────────────
# Data Control Plane — Makefile
# ─────────────────────────────────────────────────────────

.PHONY: up down restart logs seed status add-dataset verify clean

# ── Infrastructure ──

up: ## Start all services
	docker compose up -d --build
	@echo "⏳ Waiting for services to be healthy..."
	@sleep 15
	@echo "✅ Stack is up. Dagster UI: http://localhost:3000"

down: ## Stop all services
	docker compose down

restart: ## Restart all services
	docker compose down
	docker compose up -d --build

clean: ## Stop and remove all data (volumes)
	docker compose down -v
	@echo "🗑️  All data volumes removed"

logs: ## Follow all service logs
	docker compose logs -f

logs-dagster: ## Follow Dagster logs only
	docker compose logs -f dagster-webserver dagster-daemon dagster-user-code

logs-cdc: ## Follow CDC pipeline logs
	docker compose logs -f kafka-connect mongodb kafka

# ── Data Operations ──

seed: ## Seed MongoDB with sample data
	docker compose exec dagster-user-code python /opt/dagster/app/scripts/seed_mongo.py

# ── Dataset Management ──

add-dataset: ## Add a new dataset: make add-dataset name=products
	@if [ -z "$(name)" ]; then echo "❌ Usage: make add-dataset name=<dataset_name>"; exit 1; fi
	@cp templates/dataset_template.yaml datasets/$(name).yaml
	@echo "📝 Created datasets/$(name).yaml — edit it and the sensor will pick it up"

# ── Monitoring ──

status: ## Show status of all services
	@echo "── Docker Services ──"
	@docker compose ps
	@echo ""
	@echo "── Kafka Connect Connectors ──"
	@curl -s http://localhost:8083/connectors 2>/dev/null | python3 -m json.tool || echo "  (Kafka Connect not available)"
	@echo ""
	@echo "── Kafka Topics ──"
	@docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null || echo "  (Kafka not available)"

clickhouse-cli: ## Open ClickHouse client
	docker compose exec clickhouse clickhouse-client

mongo-cli: ## Open MongoDB shell
	docker compose exec mongodb mongosh

# ── Verification ──

verify: ## Run end-to-end verification
	@echo "── 1. Checking services ──"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}"
	@echo ""
	@echo "── 2. Checking Kafka Connect ──"
	@curl -s http://localhost:8083/connectors | python3 -m json.tool
	@echo ""
	@echo "── 3. Checking Kafka Topics ──"
	@docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --list
	@echo ""
	@echo "── 4. Checking ClickHouse Tables ──"
	@docker compose exec clickhouse clickhouse-client --query "SELECT database, name, engine FROM system.tables WHERE database = 'analytics'"
	@echo ""
	@echo "── 5. Checking ClickHouse Row Counts ──"
	@docker compose exec clickhouse clickhouse-client --query "SELECT name, total_rows FROM system.tables WHERE database = 'analytics' AND engine LIKE '%MergeTree%'"

# ── Help ──

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
