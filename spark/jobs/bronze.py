"""Bronze: raw chunks -> bronze.games_raw (Delta), one row per game. FR-SP2, DQ-03, DQ-04.

Reads exactly the chunks listed in ``raw/{month}/_manifest.json``. Records are split on
``\\n[Event `` (one record per game; Spark decompresses zstd natively). Nothing is
interpreted beyond structure: headers become a map, movetext stays a string.

Structural rejects go to quarantine.games (layer='bronze') with a rule name:
  no_headers, duplicate_header_keys, missing_game_id, duplicate_game_id (DQ-04: the first
  copy by chunk is kept, later copies are quarantined).

Reconciliation (DQ-03): bronze rows + bronze quarantine rows must equal the manifest's
games_total, read back from the written Delta tables; otherwise the job exits non-zero.
Writes replace only this month (replaceWhere), so reruns never duplicate (FR-DL5).
"""

import argparse
import json
import time
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark.jobs.lake import DEFAULT_LAKE_ROOT, read_manifest, table_path

RECORD_SEP = "\n[Event "
HEADER_RE = r'(?m)^\[([A-Za-z0-9_]+) "(.*)"\]$'
SITE_GAME_ID_RE = r'(?m)^\[Site "https?://lichess\.org/([A-Za-z0-9]{8})"\]$'
MOVETEXT_RE = r"(?s)\n[ \t]*\n(.*?)\s*$"
CHUNK_ID_RE = r"chunk-(\d+)"


def read_raw(spark: SparkSession, paths: list[str]) -> DataFrame:
    return (
        spark.read.option("lineSep", RECORD_SEP)
        .text(paths)
        .select("value", F.col("_metadata.file_name").alias("file_name"))
    )


def parse_games(raw: DataFrame, month: str) -> DataFrame:
    """One row per record with structure extracted and a reject reason (null = valid)."""
    value = F.col("value")
    pgn = F.when(value.startswith("[Event "), value).otherwise(F.concat(F.lit("[Event "), value))
    df = raw.select(
        F.regexp_extract("file_name", CHUNK_ID_RE, 1).cast("int").alias("chunk_id"),
        pgn.alias("pgn"),
    )
    keys = F.regexp_extract_all("pgn", F.lit(HEADER_RE), F.lit(1))
    vals = F.regexp_extract_all("pgn", F.lit(HEADER_RE), F.lit(2))
    unique_keys: Column = F.size(F.array_distinct(keys)) == F.size(keys)
    game_id = F.regexp_extract("pgn", SITE_GAME_ID_RE, 1)

    return df.select(
        F.when(game_id != "", game_id).alias("game_id"),
        F.lit(month).alias("month"),
        "chunk_id",
        F.when(unique_keys, F.map_from_arrays(keys, vals)).alias("headers"),
        F.regexp_extract("pgn", MOVETEXT_RE, 1).alias("movetext"),
        "pgn",
        F.when(F.size(keys) == 0, "no_headers")
        .when(~unique_keys, "duplicate_header_keys")
        .when(game_id == "", "missing_game_id")
        .alias("reject_reason"),
    )


def split_bronze_quarantine(parsed: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = parsed.where(F.col("reject_reason").isNull())
    invalid = parsed.where(F.col("reject_reason").isNotNull())

    # Duplicates are rare: find their ids (small), then rank only those rows.
    dup_ids = (
        valid.groupBy("game_id")
        .count()
        .where("count > 1")
        .select("game_id", F.lit(True).alias("_dup"))
    )
    flagged = valid.join(F.broadcast(dup_ids), "game_id", "left")
    unique_rows = flagged.where(F.col("_dup").isNull()).drop("_dup")
    ranked = flagged.where(F.col("_dup")).withColumn(
        "_rn", F.row_number().over(Window.partitionBy("game_id").orderBy("chunk_id", "pgn"))
    )

    now = F.current_timestamp()
    bronze_cols = ["game_id", "month", "chunk_id", "headers", "movetext"]
    bronze = (
        unique_rows.select(*bronze_cols)
        .unionByName(ranked.where("_rn = 1").select(*bronze_cols))
        .withColumn("_ingested_at", now)
    )

    q_cols: list[Column | str] = [
        "month",
        F.lit("bronze").alias("layer"),
        F.col("reject_reason").alias("rule_failed"),
        "game_id",
        "chunk_id",
        F.col("pgn").alias("raw_text"),
    ]
    quarantine = (
        invalid.select(*q_cols)
        .unionByName(
            ranked.where("_rn > 1")
            .withColumn("reject_reason", F.lit("duplicate_game_id"))
            .select(*q_cols)
        )
        .withColumn("_ingested_at", now)
    )
    return bronze, quarantine


def run(spark: SparkSession, month: str, root: str = DEFAULT_LAKE_ROOT) -> dict[str, Any]:
    started = time.monotonic()
    manifest = read_manifest(spark, root, month)
    paths = [f"{root}/{c['key']}" for c in manifest["chunks"]]
    bronze_path = table_path(root, "bronze", "games_raw")
    quarantine_path = table_path(root, "quarantine", "games")

    bronze, quarantine = split_bronze_quarantine(parse_games(read_raw(spark, paths), month))

    (
        bronze.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"month = '{month}'")
        .partitionBy("month")
        .save(bronze_path)
    )
    (
        quarantine.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"layer = 'bronze' AND month = '{month}'")
        .partitionBy("layer", "month")
        .save(quarantine_path)
    )

    # DQ-03 / DQ-04 on what was actually written.
    b = spark.read.format("delta").load(bronze_path).where(F.col("month") == month)
    q = (
        spark.read.format("delta")
        .load(quarantine_path)
        .where((F.col("layer") == "bronze") & (F.col("month") == month))
    )
    bronze_rows = b.count()
    distinct_ids = b.select("game_id").distinct().count()
    rules = {r["rule_failed"]: r["count"] for r in q.groupBy("rule_failed").count().collect()}
    quarantine_rows = sum(rules.values())
    expected = int(manifest["games_total"])

    result: dict[str, Any] = {
        "month": month,
        "manifest_games": expected,
        "bronze_rows": bronze_rows,
        "quarantine_rows": quarantine_rows,
        "quarantine_by_rule": rules,
        "chunks": len(paths),
        "dq03_reconciled": bronze_rows + quarantine_rows == expected,
        "dq04_unique_ids": distinct_ids == bronze_rows,
        "seconds": round(time.monotonic() - started, 1),
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    parser.add_argument("--lake-root", default=DEFAULT_LAKE_ROOT)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"bronze-{args.month}").getOrCreate()
    result = run(spark, args.month, args.lake_root)
    print("BRONZE " + json.dumps(result))
    spark.stop()
    if not (result["dq03_reconciled"] and result["dq04_unique_ids"]):
        raise SystemExit(f"bronze {args.month}: reconciliation failed")


if __name__ == "__main__":
    main()
