"""ClickHouse checks, including the Phase 0 gate (run `make gate` first).

Gate: ClickHouse reads the Delta table that Spark wrote to the lake.
"""

import os

import pytest
from clickhouse_connect.driver.client import Client

pytestmark = pytest.mark.integration

GATE_PREFIX = "_gate/spark_delta"
GATE_ROWS = 1_000
GATE_SUM_ID = sum(range(GATE_ROWS))


def test_ping(ch: Client) -> None:
    assert ch.command("SELECT 1") == 1


def test_gate_reads_spark_parquet_via_s3(ch: Client) -> None:
    row = ch.query(
        "SELECT count(), sum(id) FROM s3(lake_s3, filename={f:String}, format='Parquet')",
        parameters={"f": f"{GATE_PREFIX}/*.parquet"},
    ).result_rows[0]
    assert tuple(row) == (GATE_ROWS, GATE_SUM_ID)


def test_gate_reads_spark_delta_via_deltalake(ch: Client) -> None:
    url = f"http://seaweedfs:8333/{os.environ['LAKE_BUCKET']}/{GATE_PREFIX}/"
    row = ch.query(
        "SELECT count(), sum(id) FROM deltaLake(lake_s3, url={u:String})",
        parameters={"u": url},
    ).result_rows[0]
    assert tuple(row) == (GATE_ROWS, GATE_SUM_ID)
