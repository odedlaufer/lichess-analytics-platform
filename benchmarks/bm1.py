"""BM-1: throughput vs month size (PRD 8.5).

Runs `make month` for each month, then derives per-stage throughput from the
ops.pipeline_runs rows those runs wrote, so the benchmark adds no timers of its own.

    uv run python -m benchmarks.bm1 --variant baseline 2013-01 2016-01
"""

import argparse
import json
import os
import subprocess
import uuid
from datetime import UTC, datetime
from typing import Any

from ingest.download import validate_month
from ops.clickhouse import ClickHouseHTTP

STAGES = ("download", "chunk", "bronze")
PROFILES = "lake spark olap"


def git_sha() -> str:
    out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True)
    dirty = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    return out.stdout.strip() + ("-dirty" if dirty.stdout.strip() else "")


def config() -> dict[str, Any]:
    return {
        "workers": 2,
        "worker_cores": int(os.environ.get("SPARK_WORKER_CORES", "2")),
        "worker_memory": os.environ.get("SPARK_WORKER_MEMORY", "2560m"),
        "executor_memory": "2g",
        "chunk_target_mb": 256,
        "git_sha": git_sha(),
    }


def runs_since(ch: ClickHouseHTTP, month: str, since: datetime) -> dict[str, dict[str, Any]]:
    sql = (
        "SELECT job, status, duration_s, rows_out, details FROM ops.pipeline_runs "
        # SQL literals are not parsed best-effort, so parse the ISO timestamp explicitly.
        f"WHERE month = '{validate_month(month)}' "
        f"AND started_at >= parseDateTime64BestEffort('{since.isoformat()}', 3, 'UTC') "
        "ORDER BY started_at FORMAT JSONEachRow"
    )
    rows = [json.loads(line) for line in ch.query(sql).splitlines() if line]
    return {r["job"]: r for r in rows}  # latest run per job wins


def measure(ch: ClickHouseHTTP, month: str, variant: str, bench_id: str) -> list[dict[str, Any]]:
    since = datetime.now(UTC)
    subprocess.run(["make", "month", f"M={month}", f"PROFILES={PROFILES}"], check=True)
    runs = runs_since(ch, month, since)

    missing = [s for s in STAGES if runs.get(s, {}).get("status") != "success"]
    if missing:
        raise SystemExit(f"{month}: no successful run recorded for {missing}")

    chunk = json.loads(runs["chunk"]["details"])
    games = int(runs["chunk"]["rows_out"])
    raw = int(chunk["uncompressed_bytes"])
    peak = chunk["peak_local_bytes"] / chunk["source_bytes"] if chunk.get("source_bytes") else None
    cfg = json.dumps(config())
    now = datetime.now(UTC).isoformat()

    def row(stage: str, seconds: float, peak_ratio: float | None = None) -> dict[str, Any]:
        return {
            "bench_id": bench_id,
            "experiment": "BM-1",
            "variant": variant,
            "month": month,
            "stage": stage,
            "games": games,
            "raw_bytes": raw,
            "seconds": round(seconds, 2),
            "games_per_s": round(games / seconds, 1),
            "raw_mib_per_s": round(raw / 2**20 / seconds, 2),
            "peak_disk_ratio": peak_ratio,
            "config": cfg,
            "measured_at": now,
        }

    secs = {s: float(runs[s]["duration_s"]) for s in STAGES}
    # PRD "month processing time": download complete -> last stage done.
    processing = secs["chunk"] + secs["bronze"]
    return [
        row("download", secs["download"]),
        row("chunk", secs["chunk"], peak),
        row("bronze", secs["bronze"]),
        row("month", processing, peak),
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("months", nargs="+")
    parser.add_argument("--variant", default="baseline")
    args = parser.parse_args()

    ch = ClickHouseHTTP.from_env()
    bench_id = str(uuid.uuid4())
    results: list[dict[str, Any]] = []
    for month in args.months:
        rows = measure(ch, month, args.variant, bench_id)
        ch.insert("ops.benchmark_results", rows)
        results.extend(rows)

    print(f"BM-1 bench_id={bench_id} variant={args.variant}")
    header = ("month", "stage", "games", "raw_MiB", "secs", "games/s", "MiB/s")
    print("{:8} {:9} {:>10} {:>8} {:>7} {:>9} {:>7}".format(*header))
    for r in results:
        print(
            f"{r['month']:8} {r['stage']:9} {r['games']:>10} {r['raw_bytes'] / 2**20:>8.0f} "
            f"{r['seconds']:>7.1f} {r['games_per_s']:>9.0f} {r['raw_mib_per_s']:>7.1f}"
        )


if __name__ == "__main__":
    main()
