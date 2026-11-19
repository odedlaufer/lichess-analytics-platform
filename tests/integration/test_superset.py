"""Superset checks. Requires `make up PROFILES="olap bi"`."""

import os

import clickhouse_connect
import httpx
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError

pytestmark = pytest.mark.integration

DB_NAME = "lichess_clickhouse"


@pytest.fixture(scope="module")
def api() -> httpx.Client:
    client = httpx.Client(base_url=f"http://localhost:{os.environ['SUPERSET_PORT']}", timeout=30)
    resp = client.post(
        "/api/v1/security/login",
        json={
            "username": os.environ["SUPERSET_ADMIN_USER"],
            "password": os.environ["SUPERSET_ADMIN_PASSWORD"],
            "provider": "db",
            "refresh": False,
        },
    )
    resp.raise_for_status()
    client.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"
    return client


def test_health(api: httpx.Client) -> None:
    resp = api.get("/health")
    assert resp.status_code == 200
    assert resp.text.strip() == "OK"


def test_clickhouse_database_registered_and_reachable(api: httpx.Client) -> None:
    dbs = api.get("/api/v1/database/").json()["result"]
    by_name = {d["database_name"]: d["id"] for d in dbs}
    assert DB_NAME in by_name

    # Listing schemas makes Superset open a real connection to ClickHouse.
    schemas = api.get(f"/api/v1/database/{by_name[DB_NAME]}/schemas/").json()["result"]
    assert "default" in schemas


def test_superset_clickhouse_user_is_read_only() -> None:
    ro = clickhouse_connect.get_client(
        host="localhost",
        port=int(os.environ["CLICKHOUSE_HTTP_PORT"]),
        username="superset_ro",
        password=os.environ["CLICKHOUSE_SUPERSET_PASSWORD"],
    )
    assert ro.command("SELECT 1") == 1
    with pytest.raises(DatabaseError):
        ro.command("CREATE TABLE default._ro_probe (x UInt8) ENGINE = Memory")
