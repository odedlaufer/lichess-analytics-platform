"""Profile bronze headers for one month: the evidence silver rules are designed from.

Prints one JSON line prefixed with PROFILE. Also the basis for header-drift (DQ-07).
"""

import argparse
import json
from typing import Any

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from spark.jobs.lake import DEFAULT_LAKE_ROOT, table_path

TIME_CONTROL_RE = r"^\d+\+\d+$"
ECO_RE = r"^([A-E]\d{2}|\?)$"
ELO_MIN, ELO_MAX = 400, 3500


def top(df: DataFrame, col: str, n: int = 20) -> dict[str, int]:
    rows = df.groupBy(col).count().orderBy(F.desc("count")).limit(n).collect()
    return {str(r[col]): int(r["count"]) for r in rows}


def profile(spark: SparkSession, month: str, root: str) -> dict[str, Any]:
    b = (
        spark.read.format("delta")
        .load(table_path(root, "bronze", "games_raw"))
        .where(F.col("month") == month)
    )
    h = F.col("headers")
    df = b.select(
        h,
        "movetext",
        h["Result"].alias("result"),
        h["Termination"].alias("termination"),
        h["Event"].alias("event"),
        h["TimeControl"].alias("tc"),
        h["ECO"].alias("eco"),
        h["UTCDate"].alias("utc_date"),
        h["WhiteElo"].alias("white_elo"),
        h["BlackElo"].alias("black_elo"),
    ).cache()

    def elo_stats(col: str) -> list[Column]:
        num = F.col(col).try_cast("int")
        return [
            F.sum(F.when(F.col(col).isNull(), 1).otherwise(0)).alias(f"{col}_missing"),
            F.sum(F.when(F.col(col).isNotNull() & num.isNull(), 1).otherwise(0)).alias(
                f"{col}_non_numeric"
            ),
            F.sum(F.when((num < ELO_MIN) | (num > ELO_MAX), 1).otherwise(0)).alias(
                f"{col}_out_of_range"
            ),
            F.min(num).alias(f"{col}_min"),
            F.max(num).alias(f"{col}_max"),
        ]

    agg = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("tc").rlike(TIME_CONTROL_RE), 1).otherwise(0)).alias("tc_n_plus_n"),
        F.sum(F.when(F.col("tc") == "-", 1).otherwise(0)).alias("tc_dash"),
        F.sum(F.when(F.col("tc").isNull(), 1).otherwise(0)).alias("tc_missing"),
        F.sum(F.when(~F.col("eco").rlike(ECO_RE), 1).otherwise(0)).alias("eco_invalid"),
        F.sum(F.when(F.col("eco").isNull(), 1).otherwise(0)).alias("eco_missing"),
        F.sum(
            F.when(F.substring("utc_date", 1, 7) != month.replace("-", "."), 1).otherwise(0)
        ).alias("utc_date_outside_month"),
        F.sum(F.when(F.col("movetext").contains("[%clk"), 1).otherwise(0)).alias("with_clock"),
        F.sum(F.when(F.col("movetext").contains("[%eval"), 1).otherwise(0)).alias("with_eval"),
        *elo_stats("white_elo"),
        *elo_stats("black_elo"),
    ).first()
    assert agg is not None

    keys = df.select(F.explode(F.map_keys("headers")).alias("key"))
    odd_tc = df.where(~F.col("tc").rlike(TIME_CONTROL_RE) & (F.col("tc") != "-"))
    odd_elo = df.where(F.col("white_elo").isNotNull() & F.col("white_elo").try_cast("int").isNull())
    odd_eco = df.where(~F.col("eco").rlike(ECO_RE))

    result: dict[str, Any] = {
        "month": month,
        **agg.asDict(),
        "header_keys": top(keys, "key", 50),
        "result_values": top(df, "result"),
        "termination_values": top(df, "termination"),
        "event_values": top(df, "event", 15),
        "odd_time_controls": top(odd_tc, "tc", 10),
        "odd_white_elo": top(odd_elo, "white_elo", 10),
        "odd_eco": top(odd_eco, "eco", 10),
    }
    df.unpersist()
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True)
    parser.add_argument("--lake-root", default=DEFAULT_LAKE_ROOT)
    args = parser.parse_args()
    spark = SparkSession.builder.appName(f"profile-bronze-{args.month}").getOrCreate()
    print("PROFILE " + json.dumps(profile(spark, args.month, args.lake_root)))
    spark.stop()


if __name__ == "__main__":
    main()
