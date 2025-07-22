.PHONY: install install-dev lint format type-check security test test-cov clean help docker-build docker-run

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install production dependencies
	poetry install --only=main

install-dev: ## Install all dependencies including development tools
	poetry install
	poetry run pre-commit install --install-hooks

lint: ## Run all linting tools
	poetry run black --check .
	poetry run isort --check-only .
	poetry run ruff check .

format: ## Auto-format code
	poetry run black .
	poetry run isort .
	poetry run ruff check --fix .

type-check: ## Run type checking
	poetry run mypy data_extractor/

security: ## Run security checks
	poetry run bandit -r data_extractor/
	poetry run safety check

test: ## Run tests
	poetry run pytest tests/ -v

test-cov: ## Run tests with coverage
	poetry run pytest --cov=data_extractor --cov-report=html --cov-report=term tests/

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .coverage htmlcov/ .pytest_cache/ .mypy_cache/ dist/ build/
	rm -rf data_extractor.egg-info/

ci: ## Run all CI checks
        make lint
        make security
        make test

docker-build: ## Build Docker image
	docker build -t data-extractor:latest .

docker-run: ## Run Docker container for testing
	docker run --rm -it data-extractor:latest python -c "print('Docker container working!')"

docker-compose-up: ## Start full environment with docker-compose
	@echo "Setting up environment..."
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env from template"; fi
	@if [ ! -f config/config.yml ]; then echo "Warning: config/config.yml not found"; fi
	docker-compose up --build

docker-compose-down: ## Stop docker-compose environment
	docker-compose down -v

setup-env: ## Setup development environment
	cp .env.example .env
	make install-dev
	@echo "Environment setup complete!"
	@echo "Edit .env file with your database settings."

validate-all: ## Run all validation checks
	@echo "🚀 Running complete validation suite..."
	make clean
	make install-dev
	make lint
        make type-check
        make security
        make test
        @echo "✅ All validation checks passed!"