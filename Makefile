.PHONY: help up down restart logs lint format test test-cov clean install install-dev

PYTHON := python
DC := docker compose

# ── Help ──────────────────────────────────────────────────────────

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ── Docker ────────────────────────────────────────────────────────

up: ## Start all services in background
	$(DC) up -d

down: ## Stop all services
	$(DC) down

restart: ## Restart all services
	$(DC) restart

logs: ## Follow logs for all services
	$(DC) logs -f

logs-api: ## Follow API logs
	$(DC) logs -f api

logs-etl: ## Follow Spark master logs
	$(DC) logs -f spark-master

ps: ## Show running containers
	$(DC) ps

build: ## Rebuild Docker images
	$(DC) build --no-cache

# ── Python Environment ────────────────────────────────────────────

install: ## Install production dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements-dev.txt

# ── Code Quality ──────────────────────────────────────────────────

lint: ## Run Ruff linter (src + etl + tests)
	$(PYTHON) -m ruff check src/ etl/ tests/

format: ## Run Ruff formatter (src + etl + tests)
	$(PYTHON) -m ruff format src/ etl/ tests/

lint-fix: ## Run Ruff linter with auto-fix
	$(PYTHON) -m ruff check --fix src/ etl/ tests/

# ── Tests ─────────────────────────────────────────────────────────

test: ## Run tests
	$(PYTHON) -m pytest

test-cov: ## Run tests with coverage report
	$(PYTHON) -m pytest --cov=src --cov-report=html --cov-report=term-missing

# ── ETL ───────────────────────────────────────────────────────────

etl-run: ## Run full ETL pipeline inside Spark container
	$(DC) exec spark-master spark-submit /app/jobs/run_pipeline.py

# ── Cleanup ───────────────────────────────────────────────────────

clean: ## Remove caches and build artifacts
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .ruff_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".coverage" -delete 2>/dev/null || true
	@echo "Clean done."

clean-data: ## Remove processed and curated data (keeps raw)
	find data/processed -type f ! -name .gitkeep -delete
	find data/curated -type f ! -name .gitkeep -delete
	@echo "Processed and curated data removed."
