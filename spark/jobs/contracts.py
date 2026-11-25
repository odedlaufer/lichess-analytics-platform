"""Pandera contract for silver.games, validated on the month as written.

Second line of defence after the per-row rules in silver.py: if a bug ever lets a bad
value through, the contract fails the job instead of publishing it.
"""

from typing import Any

import pyspark.sql.types as T
from pandera.pyspark import Check, Column, DataFrameSchema
from pyspark.sql import DataFrame

SPEEDS = ["correspondence", "ultrabullet", "bullet", "blitz", "rapid", "classical"]
RESULTS = ["1-0", "0-1", "1/2-1/2"]
ECO_RE = r"^([A-E]\d{2}|\?)$"
GAME_ID_RE = r"^[A-Za-z0-9]{8}$"
ELO_MIN, ELO_MAX = 400, 3500


def _str(nullable: bool = True, *checks: Check) -> Column:
    return Column(T.StringType(), list(checks), nullable=nullable)


def _int(nullable: bool = True, *checks: Check) -> Column:
    return Column(T.IntegerType(), list(checks), nullable=nullable)


SILVER_CONTRACT = DataFrameSchema(
    {
        "game_id": _str(False, Check.str_matches(GAME_ID_RE)),
        "month": _str(False),
        "played_at": Column(T.TimestampType(), nullable=False),
        "speed": _str(False, Check.isin(SPEEDS)),
        "base_s": _int(True, Check.greater_than_or_equal_to(0)),
        "increment_s": _int(True, Check.greater_than_or_equal_to(0)),
        "white": _str(False),
        "black": _str(False),
        "white_elo": _int(True, Check.in_range(ELO_MIN, ELO_MAX)),
        "black_elo": _int(True, Check.in_range(ELO_MIN, ELO_MAX)),
        "white_rating_diff": _int(),
        "black_rating_diff": _int(),
        "white_title": _str(),
        "black_title": _str(),
        "result": _str(False, Check.isin(RESULTS)),
        "termination": _str(),
        "event": _str(),
        "tournament_id": _str(),
        "eco": _str(False, Check.str_matches(ECO_RE)),
        "opening_family": _str(),
        "opening_variation": _str(),
        "ply_count": _int(False, Check.greater_than_or_equal_to(0)),
        "has_clock": Column(T.BooleanType(), nullable=False),
        "has_eval": Column(T.BooleanType(), nullable=False),
        "_ingested_at": Column(T.TimestampType(), nullable=False),
    },
    name="silver.games",
    strict=True,
)


def contract_errors(df: DataFrame) -> list[dict[str, Any]]:
    """Validate and flatten Pandera's report to [{column, check, error}, ...]; empty = pass."""
    validated: Any = SILVER_CONTRACT.validate(df)  # adds the `.pandera` accessor
    report = validated.pandera.errors
    flat: list[dict[str, Any]] = []
    for _level, by_type in dict(report).items():
        for check_type, errors in dict(by_type).items():
            for e in errors:
                flat.append(
                    {
                        "type": check_type,
                        "column": e.get("column"),
                        "check": e.get("check"),
                        "error": str(e.get("error"))[:300],
                    }
                )
    return flat
