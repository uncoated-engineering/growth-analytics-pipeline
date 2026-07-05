.PHONY: help install install-dev setup sync clean format lint assert-typing pre-commit-install pre-commit-run pre-commit-update test test-airflow test-all generate-data airflow-standalone notebook ingest-bronze ingest-silver ingest-gold pipeline

PROJECT_DIR := $(shell pwd)
DAGS_DIR := $(PROJECT_DIR)/airflow/dags

help:
	@echo "Available commands:"
	@echo "  make setup                - Create Python environment and install dependencies"
	@echo "  make install              - Install Python dependencies"
	@echo "  make install-dev          - Install dependencies with dev tools"
	@echo "  make sync                 - Sync dependencies from pyproject.toml"
	@echo "  make clean                - Clean up temporary files and caches"
	@echo "  make format               - Format code with black and ruff"
	@echo "  make lint                 - Lint code with ruff"
	@echo "  make assert-typing        - Check type hints with mypy"
	@echo "  make pre-commit-install   - Install pre-commit hooks"
	@echo "  make pre-commit-run       - Run pre-commit hooks on all files"
	@echo "  make pre-commit-update    - Update pre-commit hooks to latest versions"
	@echo "  make test                 - Run Spark job tests"
	@echo "  make test-airflow         - Run Airflow DAG tests"
	@echo "  make test-all             - Run all tests (Spark + Airflow)"
	@echo "  make generate-data        - Generate synthetic data"
	@echo "  make ingest-bronze        - Run bronze layer ingestion (raw to Delta)"
	@echo "  make ingest-silver        - Run silver layer transformation (SCD Type 2)"
	@echo "  make ingest-gold          - Run gold layer aggregation (cohort analysis)"
	@echo "  make pipeline             - Run full pipeline (bronze → silver → gold)"
	@echo "  make airflow-standalone    - Start Airflow standalone (webserver + scheduler)"
	@echo "  make notebook             - Start Jupyter notebook server"

setup:
	@echo "Creating Python 3.12 environment with uv..."
	uv venv --python 3.12
	@echo "Environment created! Activate with: source .venv/bin/activate"
	@echo "Installing dependencies..."
	uv sync

install:
	@echo "Installing dependencies..."
	uv sync --no-dev

install-dev:
	@echo "Installing dependencies with dev tools..."
	uv sync

sync:
	@echo "Syncing dependencies from pyproject.toml..."
	uv sync

clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.log" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleanup complete!"

format:
	@echo "Formatting code with black..."
	uv run black spark/ scripts/ airflow/
	@echo "Formatting code with ruff..."
	uv run ruff check --fix spark/ scripts/ airflow/
	@echo "Formatting complete!"

lint:
	@echo "Linting code with ruff..."
	uv run ruff check spark/ scripts/ airflow/
	@echo "Linting complete!"

assert-typing:
	@echo "Checking type hints with mypy..."
	uv run mypy spark/ scripts/ airflow/
	@echo "Type checking complete!"

pre-commit-install:
	@echo "Installing pre-commit hooks..."
	uv run pre-commit install
	@echo "Pre-commit hooks installed! They will run automatically on git commit."

pre-commit-run:
	@echo "Running pre-commit hooks on all files..."
	uv run pre-commit run --all-files

pre-commit-update:
	@echo "Updating pre-commit hooks to latest versions..."
	uv run pre-commit autoupdate
	@echo "Pre-commit hooks updated!"

test:
	@echo "Running Spark job tests..."
	uv run pytest tests/spark -v

test-airflow:
	@echo "Running Airflow DAG tests..."
	uv run pytest tests/airflow -v

test-all:
	@echo "Running all tests (Spark + Airflow)..."
	uv run pytest tests/ -v

generate-data:
	@echo "Generating synthetic data..."
	uv run python scripts/generate_synthetic_data.py

airflow-standalone:
	@echo "Starting Airflow standalone with DAGs from $(DAGS_DIR)..."
	AIRFLOW__CORE__DAGS_FOLDER=$(DAGS_DIR) uv run airflow standalone

notebook:
	@echo "Starting Jupyter notebook server..."
	uv run jupyter notebook notebooks/

ingest-bronze:
	@echo "Running bronze layer ingestion..."
	uv run python -m spark.jobs.bronze.main

ingest-silver:
	@echo "Running silver layer transformation..."
	uv run python -m spark.jobs.silver.main

ingest-gold:
	@echo "Running gold layer aggregation..."
	uv run python -m spark.jobs.gold.main

pipeline:
	@echo "Running full data pipeline..."
	@make ingest-bronze
	@echo ""
	@make ingest-silver
	@echo ""
	@make ingest-gold
	@echo ""
	@echo "✓ Full pipeline complete!"
