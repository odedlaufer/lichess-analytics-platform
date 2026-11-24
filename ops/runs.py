"""Record pipeline runs and reconciliation checks in ClickHouse (FR-SP8, FR-DL3).

    with RunRecorder("bronze", month) as rec:
        ...
        rec.set(rows_in=..., rows_out=...)
        rec.reconcile("bronze", expected=..., actual=..., quarantined=...)

On exit one ops.pipeline_runs row is written with status success/failed (exceptions,
including SystemExit, are recorded and re-raised). Metrics writes fail open: if
ClickHouse is unreachable the job's own outcome is unchanged and a warning is logged.
"""

import json
import logging
import os
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Literal, Protocol

log = logging.getLogger(__name__)

METRICS = frozenset(
    {
        "rows_in",
        "rows_out",
        "rows_quarantined",
        "bytes_in",
        "bytes_out",
        "shuffle_read_bytes",
        "shuffle_write_bytes",
        "spill_bytes",
    }
)


class Sink(Protocol):
    def insert(self, table: str, rows: Sequence[Mapping[str, Any]]) -> None: ...


def default_sink() -> Sink | None:
    if not os.environ.get("CLICKHOUSE_URL"):
        log.warning("CLICKHOUSE_URL not set: run metrics are NOT recorded")
        return None
    from ops.clickhouse import ClickHouseHTTP

    return ClickHouseHTTP.from_env()


def _now() -> datetime:
    return datetime.now(UTC)


class RunRecorder:
    def __init__(self, job: str, month: str = "", sink: Sink | None = None) -> None:
        self.job = job
        self.month = month
        self.sink = sink
        self.run_id = str(uuid.uuid4())
        self.metrics: dict[str, int] = {}
        self.details: dict[str, Any] = {}
        self.write_errors: list[str] = []
        self._started = _now()
        self._t0 = time.monotonic()

    def __enter__(self) -> "RunRecorder":
        self._started = _now()
        self._t0 = time.monotonic()
        return self

    def set(self, **metrics: int | None) -> None:
        unknown = set(metrics) - METRICS
        if unknown:
            raise KeyError(f"unknown run metrics: {sorted(unknown)}")
        self.metrics.update({k: int(v) for k, v in metrics.items() if v is not None})

    def reconcile(self, stage: str, expected: int, actual: int, quarantined: int = 0) -> bool:
        ok = actual + quarantined == expected
        self._write(
            "ops.reconciliation",
            {
                "month": self.month,
                "stage": stage,
                "expected": expected,
                "actual": actual,
                "quarantined": quarantined,
                "reconciled": ok,
                "run_id": self.run_id,
                "checked_at": _now().isoformat(),
            },
        )
        return ok

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> Literal[False]:
        failed = exc_type is not None and not (
            exc_type is SystemExit and isinstance(exc, SystemExit) and exc.code in (0, None)
        )
        row: dict[str, Any] = {
            "run_id": self.run_id,
            "job": self.job,
            "month": self.month,
            "status": "failed" if failed else "success",
            "started_at": self._started.isoformat(),
            "finished_at": _now().isoformat(),
            "duration_s": round(time.monotonic() - self._t0, 3),
            "error": f"{exc_type.__name__}: {exc}" if failed and exc_type else "",
            "details": json.dumps(self.details, default=str),
            **self.metrics,
        }
        self._write("ops.pipeline_runs", row)
        return False  # never swallow the job's exception

    def _write(self, table: str, row: Mapping[str, Any]) -> None:
        if self.sink is None:
            return
        try:
            self.sink.insert(table, [row])
        except Exception as err:  # fail open: metrics must not change the job's outcome
            self.write_errors.append(f"{table}: {err}")
            log.warning("could not write %s for run %s: %s", table, self.run_id, err)
