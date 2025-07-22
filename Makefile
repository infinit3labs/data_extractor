.PHONY: install install-dev lint format type-check security test test-cov clean help

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install production dependencies
	poetry install --only=main

install-dev: ## Install all dependencies including development tools
	poetry install
	poetry run pre-commit install

lint: ## Run all linting tools
	poetry run black --check .
	poetry run isort --check-only .
	poetry run mypy .

format: ## Auto-format code
	poetry run black .
	poetry run isort .

type-check: ## Run type checking
	poetry run mypy .

security: ## Run security checks
	poetry run bandit -r data_extractor/
	poetry run safety check

test: ## Run tests
	poetry run pytest tests/

test-cov: ## Run tests with coverage
	poetry run pytest --cov=data_extractor --cov-report=html --cov-report=term tests/

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .coverage htmlcov/ .pytest_cache/ .mypy_cache/ dist/ build/

ci: ## Run all CI checks
	make lint
	make security
	make test