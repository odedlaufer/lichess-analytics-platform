"""Pick real odd games for golden-file tests (PRD 8.4) and print them as JSON lines.

For each month, games are chosen per category in a fixed priority order, deterministically
(ordered by game_id, a game is used once). For each game: the exact raw PGN record from the
chunks and its current silver row. `tools/write_golden.py` turns the output into fixtures.
"""

import argparse
import json
from datetime import datetime
from typing import Any

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from spark.jobs.bronze import RECORD_SEP, SITE_GAME_ID_RE
from spark.jobs.lake import DEFAULT_LAKE_ROOT, read_manifest, table_path

PER_CATEGORY = 3


def categories(month: str) -> list[tuple[str, Column]]:
    c = F.col
    return [
        ("elo_unknown", c("white_elo").isNull() | c("black_elo").isNull()),
        ("titled", c("white_title").isNotNull() | c("black_title").isNotNull()),
        ("correspondence", c("speed") == "correspondence"),
        ("abandoned", c("termination") == "Abandoned"),
        ("rules_infraction", c("termination") == "Rules infraction"),
        ("time_forfeit", c("termination") == "Time forfeit"),
        ("draw", c("result") == "1/2-1/2"),
        ("unicode_name", c("white").rlike(r"[^\x00-\x7F]") | c("black").rlike(r"[^\x00-\x7F]")),
        ("with_eval", c("has_eval")),
        ("prev_day_start", c("played_at") < F.to_timestamp(F.lit(f"{month}-01"))),
        ("no_rating_diff", c("white_rating_diff").isNull()),
        ("tournament", c("tournament_id").isNotNull()),
        ("short_game", c("ply_count") <= 2),
    ]


def jsonable(row: dict[str, Any]) -> dict[str, Any]:
    return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in row.items()}


def extract(spark: SparkSession, month: str, root: str) -> list[dict[str, Any]]:
    silver = (
        spark.read.format("delta")
        .load(table_path(root, "silver", "games"))
        .where(F.col("month") == month)
    )
    picked: dict[str, str] = {}
    for name, cond in categories(month):
        rows = (
            silver.where(cond & ~F.col("game_id").isin(list(picked) or [""]))
            .select("game_id")
            .orderBy("game_id")
            .limit(PER_CATEGORY)
            .collect()
        )
        picked.update({r["game_id"]: name for r in rows})

    manifest = read_manifest(spark, root, month)
    raw = (
        spark.read.option("lineSep", RECORD_SEP)
        .text([f"{root}/{c['key']}" for c in manifest["chunks"]])
        .withColumn(
            "pgn",
            F.when(F.col("value").startswith("[Event "), F.col("value")).otherwise(
                F.concat(F.lit("[Event "), F.col("value"))
            ),
        )
        .withColumn("game_id", F.regexp_extract("pgn", SITE_GAME_ID_RE, 1))
        .where(F.col("game_id").isin(list(picked)))
    )
    pgn = {r["game_id"]: r["pgn"].rstrip() + "\n\n" for r in raw.collect()}
    rows = silver.where(F.col("game_id").isin(list(picked))).drop("_ingested_at").collect()
    return [
        {
            "month": month,
            "category": picked[r["game_id"]],
            "game_id": r["game_id"],
            "pgn": pgn[r["game_id"]],
            "silver": jsonable(r.asDict()),
        }
        for r in sorted(rows, key=lambda r: r["game_id"])
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", required=True)
    parser.add_argument("--lake-root", default=DEFAULT_LAKE_ROOT)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("extract-golden").getOrCreate()
    for month in args.months:
        for item in extract(spark, month, args.lake_root):
            print("GOLDEN " + json.dumps(item, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
