SHELL := /bin/bash
.DEFAULT_GOAL := help

ifneq (,$(wildcard .env))
include .env
export
endif

PROFILES ?= lake
COMPOSE := docker compose $(addprefix --profile ,$(PROFILES))

.PHONY: help setup lint format typecheck test test-integration up down ps buckets month backfill live bench

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

test: ## Run unit tests
	uv run pytest

test-integration: ## Run integration tests against running services
	uv run pytest -m integration

up: ## Start services: make up PROFILES="lake spark"
	$(COMPOSE) up -d --wait
	@if echo "$(PROFILES)" | grep -qw lake; then $(MAKE) --no-print-directory buckets; fi

buckets: ## Create the lake bucket if missing (idempotent)
	@if $(COMPOSE) exec -T seaweedfs sh -c 'echo "s3.bucket.list" | weed shell' 2>/dev/null | grep -qw "$(LAKE_BUCKET)"; then \
		echo "bucket $(LAKE_BUCKET): exists"; \
	else \
		$(COMPOSE) exec -T seaweedfs sh -c 'echo "s3.bucket.create -name $(LAKE_BUCKET)" | weed shell' >/dev/null 2>&1 \
		&& echo "bucket $(LAKE_BUCKET): created"; \
	fi

down: ## Stop services (keeps data volumes)
	$(COMPOSE) down

ps: ## Show service status
	$(COMPOSE) ps

month: ## Process one month: make month M=2016-01 (Phase 1)
	@echo "not implemented yet" && exit 1

backfill: ## Backfill: make backfill FROM=2013-01 TO=2016-12 (Phase 2)
	@echo "not implemented yet" && exit 1

live: ## Start live collectors (Phase 4)
	@echo "not implemented yet" && exit 1

bench: ## Run scaling benchmark (Phase 5)
	@echo "not implemented yet" && exit 1
