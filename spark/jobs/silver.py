"""Silver: bronze.games_raw -> silver.games (typed, validated). FR-SP3, DQ-05, DQ-06, DQ-08.

Rules come from profiling real months (ADR-008), not from guesses:
- TimeControl "-" is correspondence (no clock), a valid speed, not an error.
- Elo "?" is an unknown rating (those games never carry a rating diff): stored as NULL.
- UTCDate is the game's start; a month's file holds games that ended in that month, so a
  start on the previous month's days is valid.

Rows failing a rule go to quarantine.games (layer='silver') with the first failing rule.
Reconciliation: silver + silver quarantine = bronze rows. DQ-08: silver / bronze must be
>= 99.99%, else the job exits non-zero (downstream publish is blocked).
Spark 4 runs in ANSI mode, so every conversion uses try_* functions.
"""

import argparse
import json
import time
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ops.runs import RunRecorder, default_sink
from spark.jobs.lake import DEFAULT_LAKE_ROOT, stage_metrics, table_path

PARSE_SUCCESS_MIN = 0.9999
ELO_MIN, ELO_MAX = 400, 3500
RESULTS = ("1-0", "0-1", "1/2-1/2")
ECO_RE = r"^([A-E]\d{2}|\?)$"
TIME_CONTROL_RE = r"^(\d+)\+(\d+)$"
TOURNAMENT_RE = r"tournament/([A-Za-z0-9]+)$"
COMMENT_RE = r"\{[^}]*\}"
MOVE_NUMBER_RE = r"\d+\.+"
RESULT_TOKEN_RE = r"(1-0|0-1|1/2-1/2|\*)\s*$"


def speed_from(base_s: Column, increment_s: Column, tc: Column) -> Column:
    """Lichess rule: estimated duration = base + 40 * increment (seconds)."""
    est = base_s + F.lit(40) * increment_s
    return (
        F.when(tc == "-", "correspondence")
        .when(est <= 29, "ultrabullet")
        .when(est <= 179, "bullet")
        .when(est <= 479, "blitz")
        .when(est <= 1499, "rapid")
        .when(est.isNotNull(), "classical")
    )


def ply_count(movetext: Column) -> Column:
    cleaned = F.trim(
        F.regexp_replace(
            F.regexp_replace(F.regexp_replace(movetext, COMMENT_RE, " "), MOVE_NUMBER_RE, " "),
            RESULT_TOKEN_RE,
            "",
        )
    )
    return F.when(cleaned == "", F.lit(0)).otherwise(F.size(F.split(cleaned, r"\s+")))


def elo(raw: Column) -> tuple[Column, Column]:
    """(value, rule): '?' is unknown -> NULL; non-numeric or out of range -> rule name."""
    num = raw.try_cast("int")
    value = F.when(raw == "?", F.lit(None).cast("int")).otherwise(num)
    rule = (
        F.when(raw.isNull() | (raw == "?"), F.lit(None).cast("string"))
        .when(num.isNull(), "elo_not_numeric")
        .when((num < ELO_MIN) | (num > ELO_MAX), "elo_out_of_range")
    )
    return value, rule


def transform(bronze: DataFrame, month: str) -> DataFrame:
    """Typed silver columns plus `reject_reason` (null = valid) and raw text for quarantine."""
    h = F.col("headers")
    tc = h["TimeControl"]
    base_s = F.regexp_extract(tc, TIME_CONTROL_RE, 1).try_cast("int")
    inc_s = F.regexp_extract(tc, TIME_CONTROL_RE, 2).try_cast("int")
    played_at = F.try_to_timestamp(
        F.concat_ws(" ", h["UTCDate"], h["UTCTime"]), F.lit("yyyy.MM.dd HH:mm:ss")
    )
    month_start = F.to_timestamp(F.lit(f"{month}-01"))
    window_start = F.add_months(month_start, -1)
    window_end = F.add_months(month_start, 1)
    white_elo, white_elo_rule = elo(h["WhiteElo"])
    black_elo, black_elo_rule = elo(h["BlackElo"])
    opening = F.when(h["Opening"] != "?", h["Opening"])

    reject = F.coalesce(
        F.when(F.trim(F.coalesce(h["White"], F.lit(""))) == "", "missing_player"),
        F.when(F.trim(F.coalesce(h["Black"], F.lit(""))) == "", "missing_player"),
        F.when(~F.coalesce(h["Result"].isin(*RESULTS), F.lit(False)), "invalid_result"),
        F.when(~F.coalesce(h["ECO"].rlike(ECO_RE), F.lit(False)), "invalid_eco"),
        F.when(
            ~(F.coalesce(tc.rlike(TIME_CONTROL_RE), F.lit(False)) | (tc == "-")),
            "time_control_unparseable",
        ),
        white_elo_rule,
        black_elo_rule,
        F.when(played_at.isNull(), "played_at_unparseable"),
        F.when((played_at < window_start) | (played_at >= window_end), "played_at_out_of_window"),
    )

    return bronze.select(
        "game_id",
        F.lit(month).alias("month"),
        played_at.alias("played_at"),
        speed_from(base_s, inc_s, tc).alias("speed"),
        base_s.alias("base_s"),
        inc_s.alias("increment_s"),
        h["White"].alias("white"),
        h["Black"].alias("black"),
        white_elo.alias("white_elo"),
        black_elo.alias("black_elo"),
        h["WhiteRatingDiff"].try_cast("int").alias("white_rating_diff"),
        h["BlackRatingDiff"].try_cast("int").alias("black_rating_diff"),
        h["WhiteTitle"].alias("white_title"),
        h["BlackTitle"].alias("black_title"),
        h["Result"].alias("result"),
        h["Termination"].alias("termination"),
        h["Event"].alias("event"),
        F.when(
            F.regexp_extract(h["Event"], TOURNAMENT_RE, 1) != "",
            F.regexp_extract(h["Event"], TOURNAMENT_RE, 1),
        ).alias("tournament_id"),
        h["ECO"].alias("eco"),
        F.trim(F.split(opening, ":").getItem(0)).alias("opening_family"),
        F.when(opening.contains(":"), F.trim(F.substring_index(opening, ":", -1))).alias(
            "opening_variation"
        ),
        ply_count(F.col("movetext")).alias("ply_count"),
        F.col("movetext").contains("[%clk").alias("has_clock"),
        F.col("movetext").contains("[%eval").alias("has_eval"),
        reject.alias("reject_reason"),
        F.to_json(h).alias("_raw_headers"),
        "chunk_id",
    )


def split_silver_quarantine(t: DataFrame) -> tuple[DataFrame, DataFrame]:
    now = F.current_timestamp()
    silver = (
        t.where(F.col("reject_reason").isNull())
        .drop("reject_reason", "_raw_headers", "chunk_id")
        .withColumn("_ingested_at", now)
    )
    quarantine = t.where(F.col("reject_reason").isNotNull()).select(
        "month",
        F.lit("silver").alias("layer"),
        F.col("reject_reason").alias("rule_failed"),
        "game_id",
        "chunk_id",
        F.col("_raw_headers").alias("raw_text"),
        now.alias("_ingested_at"),
    )
    return silver, quarantine


def run(spark: SparkSession, month: str, root: str = DEFAULT_LAKE_ROOT) -> dict[str, Any]:
    started = time.monotonic()
    bronze_path = table_path(root, "bronze", "games_raw")
    silver_path = table_path(root, "silver", "games")
    quarantine_path = table_path(root, "quarantine", "games")

    bronze = spark.read.format("delta").load(bronze_path).where(F.col("month") == month)
    silver, quarantine = split_silver_quarantine(transform(bronze, month))

    (
        silver.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"month = '{month}'")
        .partitionBy("month")
        .save(silver_path)
    )
    (
        quarantine.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"layer = 'silver' AND month = '{month}'")
        .partitionBy("layer", "month")
        .save(quarantine_path)
    )

    bronze_rows = bronze.count()
    s = spark.read.format("delta").load(silver_path).where(F.col("month") == month)
    q = (
        spark.read.format("delta")
        .load(quarantine_path)
        .where((F.col("layer") == "silver") & (F.col("month") == month))
    )
    silver_rows = s.count()
    rules = {r["rule_failed"]: r["count"] for r in q.groupBy("rule_failed").count().collect()}
    quarantine_rows = sum(rules.values())
    speeds = {r["speed"]: r["count"] for r in s.groupBy("speed").count().collect()}
    success = silver_rows / bronze_rows if bronze_rows else 0.0

    return {
        "month": month,
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "quarantine_rows": quarantine_rows,
        "quarantine_by_rule": rules,
        "speeds": speeds,
        "parse_success": round(success, 6),
        "reconciled": silver_rows + quarantine_rows == bronze_rows,
        "dq08_ok": success >= PARSE_SUCCESS_MIN,
        "seconds": round(time.monotonic() - started, 1),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    parser.add_argument("--lake-root", default=DEFAULT_LAKE_ROOT)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"silver-{args.month}").getOrCreate()
    with RunRecorder("silver", args.month, default_sink()) as rec:
        try:
            result = run(spark, args.month, args.lake_root)
            print("SILVER " + json.dumps(result))
            rec.set(
                rows_in=result["bronze_rows"],
                rows_out=result["silver_rows"],
                rows_quarantined=result["quarantine_rows"],
                **stage_metrics(spark),
            )
            rec.details.update(
                quarantine_by_rule=result["quarantine_by_rule"],
                speeds=result["speeds"],
                parse_success=result["parse_success"],
                app_id=spark.sparkContext.applicationId,
            )
            rec.reconcile(
                "silver",
                expected=result["bronze_rows"],
                actual=result["silver_rows"],
                quarantined=result["quarantine_rows"],
            )
        finally:
            spark.stop()
        if not result["reconciled"]:
            raise SystemExit(f"silver {args.month}: reconciliation failed")
        if not result["dq08_ok"]:
            raise SystemExit(
                f"silver {args.month}: DQ-08 parse success {result['parse_success']} "
                f"< {PARSE_SUCCESS_MIN}"
            )


if __name__ == "__main__":
    main()
