"""ops.* tables and the ops_writer user. Requires `make up PROFILES="olap"` (DDL applied)."""

import os
import urllib.error

import pytest
from clickhouse_connect.driver.client import Client

from ops.clickhouse import ClickHouseHTTP
from ops.runs import RunRecorder

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def writer() -> ClickHouseHTTP:
    return ClickHouseHTTP.from_env()


def test_ops_tables_exist(ch: Client) -> None:
    tables = ch.query("SELECT name FROM system.tables WHERE database = 'ops'").result_rows
    assert {"pipeline_runs", "reconciliation"} <= {t[0] for t in tables}


def test_recorder_roundtrip(ch: Client, writer: ClickHouseHTTP) -> None:
    with RunRecorder("_itest", "2013-01", writer) as rec:
        rec.set(rows_in=3, rows_out=2, rows_quarantined=1)
        assert rec.reconcile("_itest", expected=3, actual=2, quarantined=1)
    assert rec.write_errors == []

    run = ch.query(
        "SELECT status, rows_in, rows_out, spill_bytes FROM ops.pipeline_runs "
        "WHERE run_id = {r:UUID}",
        parameters={"r": rec.run_id},
    ).result_rows
    assert run == [("success", 3, 2, None)]
    rec_row = ch.query(
        "SELECT reconciled FROM ops.reconciliation WHERE run_id = {r:UUID}",
        parameters={"r": rec.run_id},
    ).result_rows
    assert rec_row == [(True,)]

    ch.command("DELETE FROM ops.pipeline_runs WHERE job = '_itest'")
    ch.command("DELETE FROM ops.reconciliation WHERE stage = '_itest'")


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE default._ops_probe (x UInt8) ENGINE = Memory",
        "DROP TABLE ops.pipeline_runs",
    ],
)
def test_ops_writer_is_denied(writer: ClickHouseHTTP, sql: str) -> None:
    with pytest.raises(urllib.error.HTTPError) as err:
        writer.query(sql)
    assert "ACCESS_DENIED" in err.value.read().decode()


def test_ops_writer_env_points_at_compose_clickhouse() -> None:
    assert os.environ["CLICKHOUSE_URL"] == f"http://localhost:{os.environ['CLICKHOUSE_HTTP_PORT']}"
