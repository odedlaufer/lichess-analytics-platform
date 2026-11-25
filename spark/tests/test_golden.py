"""Golden-file test (PRD 8.4): real odd games from the dumps -> exact expected silver rows.

Fixtures in spark/tests/golden/ come from `make golden-extract` (real records, pseudonymized
players) and were reviewed when committed. Any parser change that alters one of these real
games fails here with the game, column, expected and actual value.
"""

import json
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from spark.jobs import bronze, silver

GOLDEN = Path(__file__).parent / "golden"
SKIP = {"_ingested_at"}


def check(name: str, cond: bool) -> None:
    print(f"TEST {'PASS' if cond else 'FAIL'} {name}")
    if not cond:
        raise AssertionError(name)


def normalise(row: dict[str, Any]) -> dict[str, Any]:
    return {
        k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items() if k not in SKIP
    }


def main() -> None:
    expected: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    categories: dict[str, set[str]] = defaultdict(set)
    for line in (GOLDEN / "expected_silver.jsonl").read_text(encoding="utf-8").splitlines():
        item = json.loads(line)
        expected[item["month"]][item["game_id"]] = item["silver"]
        categories[item["month"]].add(item["category"])
    check("fixtures present", sum(len(v) for v in expected.values()) >= 50)

    spark = SparkSession.builder.master("local[2]").appName("test-golden").getOrCreate()
    tmp = Path(tempfile.mkdtemp())
    root = f"file://{tmp}"
    try:
        for month, games in sorted(expected.items()):
            text = (GOLDEN / f"{month}.pgn").read_text(encoding="utf-8")
            raw = tmp / "raw" / month
            raw.mkdir(parents=True)
            (raw / "chunk-00000.pgn").write_text(text, encoding="utf-8")
            n = sum(1 for line in text.splitlines() if line.startswith("[Event "))
            manifest = {
                "month": month,
                "games_total": n,
                "chunks": [{"key": f"raw/{month}/chunk-00000.pgn", "games": n}],
            }
            (raw / "_manifest.json").write_text(json.dumps(manifest))

            b = bronze.run(spark, month, root)
            s = silver.run(spark, month, root)
            check(f"{month}: {n} records = {len(games)} expected games", n == len(games))
            check(
                f"{month}: bronze reconciled, nothing quarantined",
                b["dq03_reconciled"] and b["quarantine_rows"] == 0,
            )
            check(
                f"{month}: silver reconciled, nothing quarantined",
                s["reconciled"] and s["quarantine_rows"] == 0,
            )
            check(f"{month}: contract passes", s["contract_ok"])

            actual = {
                r["game_id"]: normalise(r.asDict())
                for r in spark.read.format("delta")
                .load(f"{root}/silver/games")
                .where(f"month = '{month}'")
                .collect()
            }
            diffs = [
                f"{gid}.{col}: expected {exp_row[col]!r}, got {actual.get(gid, {}).get(col)!r}"
                for gid, exp_row in sorted(games.items())
                for col in sorted(exp_row)
                if actual.get(gid, {}).get(col) != exp_row[col]
            ]
            for d in diffs[:20]:
                print("DIFF", d)
            check(f"{month}: every silver column matches ({len(games)} games)", not diffs)
            print(f"INFO {month} categories: {sorted(categories[month])}")
        print("TEST ALL PASSED")
    finally:
        spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
