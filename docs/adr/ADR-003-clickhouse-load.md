# ADR-003: ClickHouse load method (PRD Q2)

- Status: Proposed (decided in Phase 3)
- Date: 2026-11-19

## Question
Load ClickHouse from gold Parquet with `s3()`, or read Delta directly with `deltaLake()`?

## Evidence so far (Phase 0 gate)
`make gate` writes a 1,000-row Delta table with Spark and reads it from ClickHouse 26.8.40.7 both ways:

| Method | Result |
| --- | --- |
| `s3(lake_s3, filename='…/*.parquet', format='Parquet')` | count and sum match |
| `deltaLake(lake_s3, url='…')` | count and sum match |

Both use the `lake_s3` named collection, so no credentials appear in SQL.

Access notes:
- Using a named collection requires `named_collection_control` for the user.
- `deltaLake()` additionally requires `show_named_collections_secrets` (it passes the
  credentials to the Delta kernel). Granted to the single admin user in
  `clickhouse/users.d/lichess.xml`.

Caveat: a plain Parquet glob ignores the Delta log, so it is only correct for a table
with a single version (or for an exported gold snapshot). `deltaLake()` reads the
current snapshot.

## To decide in Phase 3
Compare on a real month: load time, correctness after OPTIMIZE/VACUUM, partition pruning.
