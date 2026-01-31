.PHONY: help up down build logs ps \
       lint lint-fix format \
       dbt-run dbt-test dbt-docs dbt-clean dbt-debug \
       ingest-port ingest-gdelt ingest-noaa ingest-comtrade \
       airflow-trigger-daily airflow-trigger-monthly \
       quality \
       superset-init \
       clean

# ── Meta ──────────────────────────────────────────────────

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-26s\033[0m %s\n", $$1, $$2}'

# ── Docker Compose ────────────────────────────────────────

up: ## Start all services (detached)
	docker compose up -d --build

down: ## Stop all services
	docker compose down

build: ## Rebuild all images
	docker compose build

logs: ## Tail logs for all services
	docker compose logs -f

ps: ## Show running containers
	docker compose ps

# ── Lint / Format (Ruff) ─────────────────────────────────

lint: ## Run ruff linter checks
	ruff check pipelines/

lint-fix: ## Run ruff linter with auto-fix
	ruff check --fix pipelines/

format: ## Run ruff formatter
	ruff format pipelines/

format-check: ## Check formatting without modifying files
	ruff format --check pipelines/

# ── dbt ───────────────────────────────────────────────────

dbt-run: ## Run dbt models (inside container)
	docker compose exec dbt bash -lc "dbt deps && dbt run"

dbt-test: ## Run dbt tests (inside container)
	docker compose exec dbt bash -lc "dbt test"

dbt-docs: ## Generate and serve dbt docs
	docker compose exec dbt bash -lc "dbt docs generate && dbt docs serve --port 8081"

dbt-clean: ## Clean dbt target artifacts
	docker compose exec dbt bash -lc "dbt clean"

dbt-debug: ## Debug dbt connection
	docker compose exec dbt bash -lc "dbt debug"

dbt-freshness: ## Check source freshness
	docker compose exec dbt bash -lc "dbt source freshness"

# ── Ingestion (via Airflow container) ─────────────────────

DATE ?= $(shell date +%Y-%m-%d)

ingest-port: ## Ingest port_ops for DATE (default: today)
	docker compose exec airflow_webserver bash -lc \
		"cd /opt/pipelines && python -m pipelines.ingestion.cli ingest --source port_ops --date $(DATE)"

ingest-gdelt: ## Ingest GDELT for DATE (default: today)
	docker compose exec airflow_webserver bash -lc \
		"cd /opt/pipelines && python -m pipelines.ingestion.cli ingest --source gdelt --date $(DATE)"

ingest-noaa: ## Ingest NOAA for DATE (default: today)
	docker compose exec airflow_webserver bash -lc \
		"cd /opt/pipelines && python -m pipelines.ingestion.cli ingest --source noaa --date $(DATE)"

ingest-comtrade: ## Ingest Comtrade for DATE (default: today)
	docker compose exec airflow_webserver bash -lc \
		"cd /opt/pipelines && python -m pipelines.ingestion.cli ingest --source comtrade --date $(DATE)"

ingest-all: ## Ingest all daily sources for DATE
	$(MAKE) ingest-port DATE=$(DATE)
	$(MAKE) ingest-gdelt DATE=$(DATE)
	$(MAKE) ingest-noaa DATE=$(DATE)

# ── Airflow ───────────────────────────────────────────────

airflow-trigger-daily: ## Trigger daily_ingest_transform DAG
	docker compose exec airflow_webserver bash -lc \
		"airflow dags trigger daily_ingest_transform"

airflow-trigger-monthly: ## Trigger monthly_ingest_transform DAG
	docker compose exec airflow_webserver bash -lc \
		"airflow dags trigger monthly_ingest_transform"

airflow-list-dags: ## List all registered DAGs
	docker compose exec airflow_webserver bash -lc \
		"airflow dags list"

airflow-shell: ## Open a shell in the Airflow webserver container
	docker compose exec airflow_webserver bash

# ── Data Quality ──────────────────────────────────────────

quality: ## Run SQL quality checks against DuckDB
	docker compose exec dbt bash -lc \
		"duckdb /opt/warehouse/warehouse.duckdb < /opt/dbt/../quality/checks.sql || true"

quality-dbt: ## Run dbt tests (schema + data)
	docker compose exec dbt bash -lc "dbt test"

# ── Superset ──────────────────────────────────────────────

superset-init: ## Re-initialise Superset (db upgrade + init)
	docker compose exec superset bash -c \
		"superset db upgrade && superset init"

superset-shell: ## Open a shell in the Superset container
	docker compose exec superset bash

# ── MinIO ─────────────────────────────────────────────────

minio-ls: ## List objects in the lake bucket
	docker compose exec minio_init mc ls local/lake/ --recursive

minio-shell: ## Open a mc-enabled shell
	docker compose run --rm minio_init sh

# ── Cleanup ───────────────────────────────────────────────

clean: ## Remove DuckDB warehouse, dbt target, pycache
	rm -f data/duckdb/warehouse.duckdb
	rm -rf warehouse/dbt/target warehouse/dbt/dbt_packages
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true

nuke: ## Stop containers and remove ALL volumes (destructive!)
	docker compose down -v
	$(MAKE) clean
