SHELL := /bin/bash
.DEFAULT_GOAL := help

ifneq (,$(wildcard .env))
include .env
export
endif

PROFILES ?= lake
ALL_PROFILES := lake spark stream olap orch bi
COMPOSE := docker compose $(addprefix --profile ,$(PROFILES))

.PHONY: help setup lint format typecheck check test test-integration up up-all down down-all ps buckets topics ch-ddl build spark-smoke gate download chunk bronze test-spark month backfill live bench

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

check: ## Local gate before every push: lint + typecheck + unit tests
	@$(MAKE) --no-print-directory lint typecheck test

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
	@if echo "$(PROFILES)" | grep -qw olap; then $(MAKE) --no-print-directory ch-ddl; fi

ch-ddl: ## Apply ClickHouse DDL in clickhouse/ddl/ (idempotent)
	@for f in clickhouse/ddl/*.sql; do \
		docker compose --profile olap exec -T clickhouse sh -c \
			'clickhouse-client --user "$$CLICKHOUSE_USER" --password "$$CLICKHOUSE_PASSWORD" --multiquery' < $$f \
		&& echo "ddl $$f: applied"; \
	done

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

up-all: ## Start every service (all profiles)
	@$(MAKE) --no-print-directory up PROFILES="$(ALL_PROFILES)"

down-all: ## Stop every service (keeps data volumes)
	@$(MAKE) --no-print-directory down PROFILES="$(ALL_PROFILES)"

down: ## Stop services (keeps data volumes)
	$(COMPOSE) down

ps: ## Show service status
	$(COMPOSE) ps

download: ## Download + verify one month: make download M=2013-01
	uv run python -m ingest.download --month $(M)

chunk: ## Chunk a downloaded month into the lake: make chunk M=2013-01 [KEEP=1] [TARGET_MB=256]
	uv run python -m ingest.chunk --month $(M) --target-mb $(or $(TARGET_MB),256) $(if $(KEEP),--keep-download,)

bronze: ## Bronze job on the cluster: make bronze M=2013-01 PROFILES="lake spark olap"
	$(COMPOSE) run --rm spark-submit spark/jobs/bronze.py --month $(M)

silver: ## Silver job on the cluster: make silver M=2013-01 PROFILES="lake spark olap"
	$(COMPOSE) run --rm spark-submit spark/jobs/silver.py --month $(M)

profile-bronze: ## Profile bronze headers for a month (evidence for silver rules): make profile-bronze M=2013-01
	$(COMPOSE) run --rm spark-submit spark/jobs/profile_bronze.py --month $(M)

test-spark: ## Spark job tests in the Spark image (local mode, local files)
	@for t in spark/tests/test_*.py; do \
		echo "== $$t"; \
		docker compose --profile lake --profile spark --profile tools run --rm --no-deps spark-submit \
			--master 'local[2]' --conf spark.eventLog.enabled=false $$t || exit 1; \
	done

month: ## Whole pipeline for one month (so far: download -> chunk -> bronze -> silver): make month M=2016-01
	@test -n "$(M)" || (echo "usage: make month M=YYYY-MM" && exit 1)
	@$(MAKE) --no-print-directory download M=$(M)
	@$(MAKE) --no-print-directory chunk M=$(M)
	@$(MAKE) --no-print-directory bronze M=$(M) PROFILES="lake spark olap"
	@$(MAKE) --no-print-directory silver M=$(M) PROFILES="lake spark olap"

backfill: ## Backfill: make backfill FROM=2013-01 TO=2016-12 (Phase 2)
	@echo "not implemented yet" && exit 1

live: ## Start live collectors (Phase 4)
	@echo "not implemented yet" && exit 1

bench: ## BM-1 throughput vs month size: make bench MONTHS="2013-01 2016-01" [VARIANT=baseline]
	uv run python -m benchmarks.bm1 --variant $(or $(VARIANT),baseline) $(MONTHS)
