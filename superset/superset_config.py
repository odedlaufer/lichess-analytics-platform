"""Superset config, mounted at /app/pythonpath/superset_config.py. Secrets come from env."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://superset:{os.environ['SUPERSET_DB_PASSWORD']}@postgres:5432/superset"
)

# Single-node laptop setup: no Redis, no Celery, no async queries.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG
FEATURE_FLAGS: dict[str, bool] = {}
SQLLAB_TIMEOUT = 60
