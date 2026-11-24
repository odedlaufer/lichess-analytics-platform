"""Bronze job tests. Run inside the Spark image: `make test-spark`.

Uses local[2] Spark and the local filesystem: real record splitting, real Delta writes,
real reconciliation, on hand-built odd games.
"""

import json
import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.jobs import bronze

MONTH = "2013-01"


def game(
    site_id: str | None, white: str = "alice", extra: str = "", moves: str = "1. e4 e5 1-0"
) -> str:
    site = f'[Site "https://lichess.org/{site_id}"]\n' if site_id else ""
    return (
        f'[Event "Rated Blitz game"]\n{site}[White "{white}"]\n[Black "bob"]\n'
        f'[Result "1-0"]\n{extra}\n{moves}\n\n'
    )


CHUNK0 = (
    game("aaaaaaaa")
    + game("bbbbbbbb", white="Ñandú 王")  # unicode name
    + game(None)  # missing Site -> missing_game_id
    + game("cccccccc", extra='[White "dup"]\n')  # duplicate header key
    + game("dddddddd", moves="1. e4 { [%clk 0:03:00] } e5 { [%clk 0:02:59] } 0-1")
    + game("eeeeeeee", moves="0-1")  # no moves (abandoned)
)
CHUNK1 = game("ffffffff") + game("aaaaaaaa", white="second-copy")  # dup id across chunks


def check(name: str, cond: bool) -> None:
    print(f"TEST {'PASS' if cond else 'FAIL'} {name}")
    if not cond:
        raise AssertionError(name)


def main() -> None:
    spark = SparkSession.builder.master("local[2]").appName("test-bronze").getOrCreate()
    tmp = Path(tempfile.mkdtemp())
    try:
        raw = tmp / "raw" / MONTH
        raw.mkdir(parents=True)
        (raw / "chunk-00000.pgn").write_text(CHUNK0)
        (raw / "chunk-00001.pgn").write_text(CHUNK1)
        manifest = {
            "month": MONTH,
            "games_total": 8,
            "chunks": [
                {"key": f"raw/{MONTH}/chunk-00000.pgn", "games": 6},
                {"key": f"raw/{MONTH}/chunk-00001.pgn", "games": 2},
            ],
        }
        (raw / "_manifest.json").write_text(json.dumps(manifest))
        root = f"file://{tmp}"

        result = bronze.run(spark, MONTH, root)
        print("RESULT", json.dumps(result))

        check("dq03 reconciled", result["dq03_reconciled"])
        check("dq04 unique ids", result["dq04_unique_ids"])
        check("bronze rows = 5", result["bronze_rows"] == 5)
        check(
            "quarantine rules",
            result["quarantine_by_rule"]
            == {"missing_game_id": 1, "duplicate_header_keys": 1, "duplicate_game_id": 1},
        )

        b = spark.read.format("delta").load(f"{root}/bronze/games_raw")
        rows = {r["game_id"]: r for r in b.collect()}
        check("first copy kept", rows["aaaaaaaa"]["headers"]["White"] == "alice")
        check("chunk id parsed", rows["ffffffff"]["chunk_id"] == 1)
        check("unicode preserved", rows["bbbbbbbb"]["headers"]["White"] == "Ñandú 王")
        check("event header restored", rows["bbbbbbbb"]["headers"]["Event"] == "Rated Blitz game")
        check("movetext with clocks", "[%clk 0:03:00]" in rows["dddddddd"]["movetext"])
        check("movetext no moves", rows["eeeeeeee"]["movetext"] == "0-1")
        check("site in headers", rows["aaaaaaaa"]["headers"]["Site"].endswith("aaaaaaaa"))

        q = spark.read.format("delta").load(f"{root}/quarantine/games")
        dup = q.where(F.col("rule_failed") == "duplicate_game_id").first()
        check(
            "dup copy quarantined with raw text",
            dup is not None and "second-copy" in dup["raw_text"],
        )

        again = bronze.run(spark, MONTH, root)  # idempotent rerun
        check("rerun same counts", again["bronze_rows"] == 5 and again["quarantine_rows"] == 3)
        print("TEST ALL PASSED")
    finally:
        spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
