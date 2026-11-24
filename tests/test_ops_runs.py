from collections.abc import Mapping, Sequence
from typing import Any

import pytest

from ops.runs import RunRecorder


class FakeSink:
    def __init__(self, fail: bool = False) -> None:
        self.rows: dict[str, list[Mapping[str, Any]]] = {}
        self.fail = fail

    def insert(self, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
        if self.fail:
            raise ConnectionError("clickhouse down")
        self.rows.setdefault(table, []).extend(rows)


def test_success_run_written_with_metrics() -> None:
    sink = FakeSink()
    with RunRecorder("bronze", "2013-01", sink) as rec:
        rec.set(rows_in=10, rows_out=9, rows_quarantined=1, spill_bytes=None)
        rec.details["chunks"] = 4

    (row,) = sink.rows["ops.pipeline_runs"]
    assert row["status"] == "success"
    assert row["job"] == "bronze"
    assert row["month"] == "2013-01"
    assert row["rows_in"] == 10
    assert row["rows_quarantined"] == 1
    assert "spill_bytes" not in row  # None means "not measured", stored as NULL
    assert row["details"] == '{"chunks": 4}'
    assert row["error"] == ""


def test_failure_recorded_and_reraised() -> None:
    sink = FakeSink()
    with pytest.raises(ValueError, match="boom"), RunRecorder("chunk", "2013-01", sink):
        raise ValueError("boom")

    (row,) = sink.rows["ops.pipeline_runs"]
    assert row["status"] == "failed"
    assert row["error"] == "ValueError: boom"


def test_nonzero_systemexit_is_failure_zero_is_success() -> None:
    sink = FakeSink()
    with pytest.raises(SystemExit), RunRecorder("bronze", "m", sink):
        raise SystemExit("reconciliation failed")
    with pytest.raises(SystemExit), RunRecorder("bronze", "m", sink):
        raise SystemExit(0)

    statuses = [r["status"] for r in sink.rows["ops.pipeline_runs"]]
    assert statuses == ["failed", "success"]


def test_reconcile_counts_quarantine() -> None:
    sink = FakeSink()
    with RunRecorder("bronze", "2013-01", sink) as rec:
        assert rec.reconcile("bronze", expected=10, actual=9, quarantined=1)
        assert not rec.reconcile("bronze", expected=10, actual=8, quarantined=1)

    first, second = sink.rows["ops.reconciliation"]
    assert first["reconciled"] is True
    assert second["reconciled"] is False
    assert first["run_id"] == sink.rows["ops.pipeline_runs"][0]["run_id"]


def test_sink_failure_does_not_change_job_outcome() -> None:
    sink = FakeSink(fail=True)
    with RunRecorder("chunk", "2013-01", sink) as rec:
        rec.set(rows_out=5)
    assert len(rec.write_errors) == 1


def test_unknown_metric_rejected() -> None:
    with RunRecorder("x", sink=FakeSink()) as rec, pytest.raises(KeyError):
        rec.set(row_out=1)
