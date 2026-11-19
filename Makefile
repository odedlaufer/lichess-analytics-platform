SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help setup lint format typecheck test up down month backfill live bench

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup: ## Install deps and git hooks
	uv sync
	uv run pre-commit install

lint: ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

format: ## Auto-format
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## mypy strict
	uv run mypy

test: ## Run tests
	uv run pytest

up: ## Start platform (Phase 0.2)
	@echo "not implemented yet" && exit 1

down: ## Stop platform (Phase 0.2)
	@echo "not implemented yet" && exit 1

month: ## Process one month: make month M=2016-01 (Phase 1)
	@echo "not implemented yet" && exit 1

backfill: ## Backfill: make backfill FROM=2013-01 TO=2016-12 (Phase 2)
	@echo "not implemented yet" && exit 1

live: ## Start live collectors (Phase 4)
	@echo "not implemented yet" && exit 1

bench: ## Run scaling benchmark (Phase 5)
	@echo "not implemented yet" && exit 1
