# Steam Games Data Processing Pipeline - Makefile
# This Makefile provides common commands for development, testing, and deployment

.PHONY: help install test clean docker-build docker-up docker-down docker-logs docker-shell lint format check-env setup-dev run-local

# Default target
help: ## Show this help message
	@echo "Steam Games Data Processing Pipeline - Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Development Setup
setup-dev: ## Set up development environment
	@echo "Setting up development environment..."
	python -m venv venv
	@echo "Virtual environment created. Activate it with:"
	@echo "  source venv/bin/activate  # On Unix/Mac"
	@echo "  venv\\Scripts\\activate     # On Windows"

install: ## Install Python dependencies
	@echo "Installing Python dependencies..."
	pip install -r config/requirements.txt

check-env: ## Check if environment variables are set
	@echo "Checking environment variables..."
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please copy config/env.template to .env and configure it."; \
		exit 1; \
	fi
	@echo "Environment file found."

# Testing
test: ## Run all tests
	@echo "Running all tests..."
	python -m pytest tests/ -v

test-unit: ## Run unit tests only
	@echo "Running unit tests..."
	python -m pytest tests/unit/ -v

test-integration: ## Run integration tests only
	@echo "Running integration tests..."
	python -m pytest tests/integration/ -v

test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	python -m pytest tests/ --cov=src --cov-report=html --cov-report=term-missing

test-watch: ## Run tests in watch mode (requires pytest-watch)
	@echo "Running tests in watch mode..."
	ptw tests/ -- -v

# Code Quality
lint: ## Run linting checks
	@echo "Running linting checks..."
	flake8 src/ tests/ --max-line-length=100 --exclude=__pycache__
	pylint src/ --disable=C0114,C0116

format: ## Format code with black
	@echo "Formatting code..."
	black src/ tests/ --line-length=100

format-check: ## Check code formatting without making changes
	@echo "Checking code formatting..."
	black src/ tests/ --line-length=100 --check

# Docker Operations
docker-build: ## Build Docker images
	@echo "Building Docker images..."
	docker-compose build

docker-up: ## Start all Docker services
	@echo "Starting Docker services..."
	docker-compose up -d

docker-down: ## Stop all Docker services
	@echo "Stopping Docker services..."
	docker-compose down

docker-logs: ## View Docker logs
	@echo "Viewing Docker logs..."
	docker-compose logs -f

docker-shell: ## Access application container shell
	@echo "Accessing application container shell..."
	docker-compose exec app bash

docker-clean: ## Clean up Docker resources
	@echo "Cleaning up Docker resources..."
	docker-compose down -v --remove-orphans
	docker system prune -f

# Local Development
run-local: check-env ## Run application locally
	@echo "Running application locally..."
	python scripts/entrypoint.py

run-extract: check-env ## Run data extraction only
	@echo "Running data extraction..."
	python -c "from src.extractors.steam_data_extractor import SteamDataExtractor; SteamDataExtractor().extract_all_data()"

run-process: check-env ## Run data processing only
	@echo "Running data processing..."
	python -c "from src.processors.steam_data_processor import SteamDataProcessor; SteamDataProcessor().process_all_data()"

run-load: check-env ## Run data loading only
	@echo "Running data loading..."
	python -c "from src.loaders.mongodb_loader import MongoDBLoader; MongoDBLoader().load_all_data()"

# Database Operations
db-shell: ## Access MongoDB shell
	@echo "Accessing MongoDB shell..."
	docker-compose exec mongodb mongosh

db-backup: ## Create MongoDB backup
	@echo "Creating MongoDB backup..."
	docker-compose exec mongodb mongodump --out /backup/$(shell date +%Y%m%d_%H%M%S)

db-restore: ## Restore MongoDB from backup (usage: make db-restore BACKUP_FILE=backup_file)
	@echo "Restoring MongoDB from backup..."
	@if [ -z "$(BACKUP_FILE)" ]; then \
		echo "Error: Please specify BACKUP_FILE parameter"; \
		exit 1; \
	fi
	docker-compose exec mongodb mongorestore /backup/$(BACKUP_FILE)

# Airflow Operations
airflow-shell: ## Access Airflow container shell
	@echo "Accessing Airflow container shell..."
	docker-compose exec airflow-webserver bash

airflow-dags: ## List Airflow DAGs
	@echo "Listing Airflow DAGs..."
	docker-compose exec airflow-webserver airflow dags list

airflow-trigger: ## Trigger Steam games DAG (usage: make airflow-trigger DAG_ID=steam_games_etl_pipeline)
	@echo "Triggering Airflow DAG..."
	@if [ -z "$(DAG_ID)" ]; then \
		echo "Error: Please specify DAG_ID parameter"; \
		exit 1; \
	fi
	docker-compose exec airflow-webserver airflow dags trigger $(DAG_ID)

# Configuration Management
config-validate: ## Validate configuration files
	@echo "Validating configuration..."
	python -c "from config.config_manager import get_config; config = get_config(); print('Configuration is valid')"

config-show: ## Show current configuration
	@echo "Current configuration:"
	python -c "from config.config_manager import get_config; import json; print(json.dumps(get_config(), indent=2, default=str))"

# Performance and Monitoring
benchmark: ## Run performance benchmarks
	@echo "Running performance benchmarks..."
	python -c "from src.utils.benchmark import run_benchmarks; run_benchmarks()"

monitor: ## Start monitoring dashboard
	@echo "Starting monitoring dashboard..."
	docker-compose -f docker-compose.monitoring.yml up -d

# Cleanup
clean: ## Clean up generated files and caches
	@echo "Cleaning up generated files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf dist/
	rm -rf build/

clean-data: ## Clean up data files
	@echo "Cleaning up data files..."
	rm -rf data/raw/*
	rm -rf data/processed/*
	rm -rf data/failed/*

# Security
security-scan: ## Run security scans
	@echo "Running security scans..."
	bandit -r src/ -f json -o security-report.json
	safety check

# Documentation
docs-build: ## Build documentation
	@echo "Building documentation..."
	# Add documentation building commands here when docs are added

docs-serve: ## Serve documentation locally
	@echo "Serving documentation locally..."
	# Add documentation serving commands here when docs are added

# Production Deployment
deploy-prod: ## Deploy to production
	@echo "Deploying to production..."
	@echo "This is a placeholder for production deployment commands"
	@echo "Add your production deployment logic here"

# Development Workflow
dev-setup: setup-dev install check-env ## Complete development setup
	@echo "Development environment setup complete!"

dev-test: format-check lint test ## Run all development checks
	@echo "All development checks passed!"

dev-run: docker-up ## Start development environment
	@echo "Development environment started!"
	@echo "Access services at:"
	@echo "  - Airflow UI: http://localhost:8080"
	@echo "  - MongoDB: mongodb://localhost:27017"

# Utility Commands
status: ## Show project status
	@echo "Project Status:"
	@echo "==============="
	@echo "Python version: $(shell python --version)"
	@echo "Docker version: $(shell docker --version)"
	@echo "Docker Compose version: $(shell docker-compose --version)"
	@if [ -f .env ]; then echo "Environment file: ✓"; else echo "Environment file: ✗"; fi
	@if [ -d venv ]; then echo "Virtual environment: ✓"; else echo "Virtual environment: ✗"; fi
	@echo ""
	@echo "Docker services:"
	docker-compose ps

logs: docker-logs ## Alias for docker-logs

shell: docker-shell ## Alias for docker-shell

# Environment-specific targets
dev: dev-setup dev-test dev-run ## Complete development workflow

prod: docker-build deploy-prod ## Production deployment workflow

# Default variables
PYTHON_VERSION ?= 3.11
DOCKER_IMAGE_TAG ?= latest
