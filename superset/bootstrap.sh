#!/bin/sh
# Idempotent init on every start, then hand over to the image's server script.
set -eu

superset db upgrade
superset fab create-admin \
  --username "$SUPERSET_ADMIN_USER" --password "$SUPERSET_ADMIN_PASSWORD" \
  --firstname Admin --lastname User --email admin@localhost || true  # exists on re-runs
superset init

# Warehouse connection as the read-only ClickHouse user.
superset set-database-uri \
  --database_name "lichess_clickhouse" \
  --uri "clickhousedb://superset_ro:${CLICKHOUSE_SUPERSET_PASSWORD}@clickhouse:8123/default"

exec /usr/bin/run-server.sh
