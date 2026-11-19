# Superset 6.1.0 + drivers: Postgres (metadata DB) and ClickHouse (warehouse).
# The image tag is rebuilt weekly upstream, so it is pinned by digest.
FROM apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88

USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python \
      "psycopg2-binary==2.9.13" \
      "clickhouse-connect==1.12.0"
USER superset
