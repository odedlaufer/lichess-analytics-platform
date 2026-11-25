"""Data-quality results in ops.dq_results, including DQ-07 header drift."""

import json
from datetime import UTC, datetime
from typing import Any

from ops.clickhouse import ClickHouseHTTP


def header_drift(previous: set[str] | None, current: set[str]) -> dict[str, Any]:
    """Compare header key sets. No previous month -> baseline; any change -> warn."""
    if previous is None:
        return {"status": "baseline", "added": [], "removed": [], "keys": sorted(current)}
    added, removed = sorted(current - previous), sorted(previous - current)
    return {
        "status": "warn" if added or removed else "pass",
        "added": added,
        "removed": removed,
        "keys": sorted(current),
    }


def previous_header_keys(ch: ClickHouseHTTP, month: str) -> set[str] | None:
    """Header keys of the latest recorded month before `month` (backfill order agnostic)."""
    sql = (
        "SELECT details FROM ops.dq_results "
        f"WHERE check_id = 'DQ-07' AND month < '{month}' "
        "ORDER BY month DESC, checked_at DESC LIMIT 1 FORMAT JSONEachRow"
    )
    lines = [line for line in ch.query(sql).splitlines() if line]
    if not lines:
        return None
    keys: list[str] = json.loads(json.loads(lines[0])["details"])["keys"]
    return set(keys)


def dq_row(
    run_id: str, month: str, layer: str, check_id: str, status: str, details: dict[str, Any]
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "month": month,
        "layer": layer,
        "check_id": check_id,
        "status": status,
        "details": json.dumps(details, default=str),
        "checked_at": datetime.now(UTC).isoformat(),
    }
