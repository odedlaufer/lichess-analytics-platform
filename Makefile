SHELL := /bin/bash
.DEFAULT_GOAL := help

ifneq (,$(wildcard .env))
include .env
export
endif

PROFILES ?= lake
COMPOSE := docker compose $(addprefix --profile ,$(PROFILES))

.PHONY: help setup lint format typecheck test test-integration up down ps buckets topics build spark-smoke gate month backfill live bench

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
	@# Object store + bucket/markers first: Spark needs s3a://lake/_spark-events at startup.
	@if echo "$(PROFILES)" | grep -qw lake; then \
		docker compose --profile lake up -d --wait seaweedfs && $(MAKE) --no-print-directory buckets; \
	fi
	$(COMPOSE) up -d --wait
	@if echo "$(PROFILES)" | grep -qw stream; then $(MAKE) --no-print-directory topics; fi

buckets: ## Create the lake bucket and directory markers (idempotent)
	uv run python -m tools.bootstrap_lake

topics: ## Create Kafka topics declared in live/topics.py (idempotent)
	uv run python -m live.topics

build: ## Build local images (Spark)
	$(COMPOSE) build

spark-smoke: ## Delta round-trip on the cluster: make spark-smoke PROFILES="lake spark"
	$(COMPOSE) run --rm spark-submit spark/jobs/smoke_delta.py

gate: ## Phase 0 gate: Spark writes Delta, ClickHouse reads it (PROFILES="lake spark olap")
	$(COMPOSE) run --rm spark-submit spark/jobs/write_gate_table.py
	uv run pytest -m integration tests/integration/test_clickhouse.py

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
