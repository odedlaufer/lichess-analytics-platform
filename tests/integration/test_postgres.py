"""Postgres metadata DB checks. Requires a profile that includes postgres (olap, orch or bi)."""

import os

import psycopg
import pytest

pytestmark = pytest.mark.integration

SERVICE_DBS = {
    "sqlmesh_state": ("sqlmesh", "SQLMESH_DB_PASSWORD"),
    "prefect": ("prefect", "PREFECT_DB_PASSWORD"),
    "superset": ("superset", "SUPERSET_DB_PASSWORD"),
}


def _connect(db: str, role: str, password: str) -> psycopg.Connection[tuple[object, ...]]:
    return psycopg.connect(
        host="localhost",
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=db,
        user=role,
        password=password,
        connect_timeout=5,
    )


@pytest.mark.parametrize("db", sorted(SERVICE_DBS))
def test_service_role_owns_its_database(db: str) -> None:
    role, password_env = SERVICE_DBS[db]
    with _connect(db, role, os.environ[password_env]) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS _smoke (id int)")
        conn.execute("DROP TABLE _smoke")
        row = conn.execute("SELECT current_user, current_database()").fetchone()
    assert row == (role, db)


def test_service_role_cannot_reach_other_database() -> None:
    with pytest.raises(psycopg.OperationalError, match="permission denied"):
        _connect("prefect", "sqlmesh", os.environ["SQLMESH_DB_PASSWORD"])
