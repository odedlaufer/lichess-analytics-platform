"""Silver job tests. Run inside the Spark image: `make test-spark`.

Builds a small bronze Delta table from hand-made headers (one game per rule and per
speed), runs the real silver job on local files and checks every column and rule.
"""

import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from spark.jobs import silver

MONTH = "2016-01"
BASE = {
    "Event": "Rated Blitz game",
    "Site": "https://lichess.org/xxxxxxxx",
    "White": "alice",
    "Black": "bob",
    "Result": "1-0",
    "UTCDate": "2016.01.15",
    "UTCTime": "12:00:00",
    "WhiteElo": "1500",
    "BlackElo": "1600",
    "WhiteRatingDiff": "+12",
    "BlackRatingDiff": "-12",
    "ECO": "B90",
    "Opening": "Sicilian Defense: Najdorf Variation",
    "TimeControl": "300+3",
    "Termination": "Normal",
}

# game_id -> (header overrides, movetext)
GAMES: dict[str, tuple[dict[str, Any], str]] = {
    "blitz001": ({}, "1. e4 { [%eval 0.2] } 1... c5 2. Nf3 1-0"),
    "corresp1": ({"TimeControl": "-", "Event": "Rated Correspondence game"}, "1. d4 d5 1/2-1/2"),
    "unknelo1": ({"WhiteElo": "?", "WhiteRatingDiff": None}, "1. e4 0-1"),
    "ultrab01": ({"TimeControl": "15+0"}, "1. e4 e5 1-0"),
    "bullet01": ({"TimeControl": "60+1"}, "1-0"),
    "rapid001": ({"TimeControl": "600+5"}, "1. e4 1-0"),
    "classic1": (
        {
            "TimeControl": "1800+0",
            "Event": "Rated Classical tournament https://lichess.org/tournament/AbC123xy",
        },
        "1. e4 1-0",
    ),
    "prevday1": ({"UTCDate": "2015.12.31", "UTCTime": "23:59:00"}, "1. e4 1-0"),
    "noopvar1": ({"Opening": "Van't Kruijs Opening", "WhiteTitle": "GM"}, "1. e3 1-0"),
    # rejects
    "badres01": ({"Result": "*"}, "1. e4 *"),
    "badeco01": ({"ECO": "Z99"}, "1. e4 1-0"),
    "badtc001": ({"TimeControl": "abc"}, "1. e4 1-0"),
    "eloout01": ({"BlackElo": "99999"}, "1. e4 1-0"),
    "elonan01": ({"WhiteElo": "abc"}, "1. e4 1-0"),
    "olddate1": ({"UTCDate": "2015.10.01"}, "1. e4 1-0"),
    "baddate1": ({"UTCDate": "garbage"}, "1. e4 1-0"),
    "noplayr1": ({"White": ""}, "1. e4 1-0"),
}

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("game_id", T.StringType()),
        T.StructField("month", T.StringType()),
        T.StructField("chunk_id", T.IntegerType()),
        T.StructField("headers", T.MapType(T.StringType(), T.StringType())),
        T.StructField("movetext", T.StringType()),
        T.StructField("_ingested_at", T.TimestampType()),
    ]
)


def check(name: str, cond: bool) -> None:
    print(f"TEST {'PASS' if cond else 'FAIL'} {name}")
    if not cond:
        raise AssertionError(name)


def main() -> None:
    spark = SparkSession.builder.master("local[2]").appName("test-silver").getOrCreate()
    tmp = Path(tempfile.mkdtemp())
    root = f"file://{tmp}"
    try:
        rows = []
        for gid, (over, moves) in GAMES.items():
            headers = {k: v for k, v in {**BASE, **over}.items() if v is not None}
            rows.append((gid, MONTH, 0, headers, moves, datetime(2016, 2, 1)))
        spark.createDataFrame(rows, BRONZE_SCHEMA).write.format("delta").partitionBy("month").save(
            f"{root}/bronze/games_raw"
        )

        result = silver.run(spark, MONTH, root)
        print("RESULT", result)

        check("reconciled", result["reconciled"])
        check("silver rows = 9", result["silver_rows"] == 9)
        check(
            "quarantine rules",
            result["quarantine_by_rule"]
            == {
                "invalid_result": 1,
                "invalid_eco": 1,
                "time_control_unparseable": 1,
                "elo_out_of_range": 1,
                "elo_not_numeric": 1,
                "played_at_out_of_window": 1,
                "played_at_unparseable": 1,
                "missing_player": 1,
            },
        )
        check("dq08 fails on this tiny sample", not result["dq08_ok"])

        s = {
            r["game_id"]: r
            for r in spark.read.format("delta").load(f"{root}/silver/games").collect()
        }
        check(
            "speeds",
            {
                g: s[g]["speed"]
                for g in ("blitz001", "corresp1", "ultrab01", "bullet01", "rapid001", "classic1")
            }
            == {
                "blitz001": "blitz",
                "corresp1": "correspondence",
                "ultrab01": "ultrabullet",
                "bullet01": "bullet",
                "rapid001": "rapid",
                "classic1": "classical",
            },
        )
        check(
            "time control parsed",
            (s["blitz001"]["base_s"], s["blitz001"]["increment_s"]) == (300, 3),
        )
        check("correspondence has no clock", s["corresp1"]["base_s"] is None)
        check(
            "unknown elo is null",
            s["unknelo1"]["white_elo"] is None and s["unknelo1"]["black_elo"] == 1600,
        )
        check(
            "rating diff signed",
            (s["blitz001"]["white_rating_diff"], s["blitz001"]["black_rating_diff"]) == (12, -12),
        )
        check("prev-day start kept", s["prevday1"]["played_at"] == datetime(2015, 12, 31, 23, 59))
        check(
            "opening split",
            (s["blitz001"]["opening_family"], s["blitz001"]["opening_variation"])
            == ("Sicilian Defense", "Najdorf Variation"),
        )
        check(
            "opening without variation",
            (s["noopvar1"]["opening_family"], s["noopvar1"]["opening_variation"])
            == ("Van't Kruijs Opening", None),
        )
        check("title", s["noopvar1"]["white_title"] == "GM")
        check(
            "tournament id",
            s["classic1"]["tournament_id"] == "AbC123xy" and s["blitz001"]["tournament_id"] is None,
        )
        check("ply count with eval comments", s["blitz001"]["ply_count"] == 3)
        check("ply count no moves", s["bullet01"]["ply_count"] == 0)
        check("has_eval / has_clock", s["blitz001"]["has_eval"] and not s["blitz001"]["has_clock"])

        again = silver.run(spark, MONTH, root)
        check("rerun idempotent", (again["silver_rows"], again["quarantine_rows"]) == (9, 8))
        print("TEST ALL PASSED")
    finally:
        spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
