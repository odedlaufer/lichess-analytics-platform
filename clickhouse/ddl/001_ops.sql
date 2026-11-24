-- Operational tables (FR-SP8, FR-DL3, DQ-03). Idempotent; applied by `make ch-ddl`.
CREATE DATABASE IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs
(
    run_id              UUID,
    job                 LowCardinality(String),
    month               LowCardinality(String),
    status              Enum8('success' = 1, 'failed' = 2),
    started_at          DateTime64(3, 'UTC'),
    finished_at         DateTime64(3, 'UTC'),
    duration_s          Float64,
    rows_in             Nullable(UInt64),
    rows_out            Nullable(UInt64),
    rows_quarantined    Nullable(UInt64),
    bytes_in            Nullable(UInt64),
    bytes_out           Nullable(UInt64),
    shuffle_read_bytes  Nullable(UInt64),
    shuffle_write_bytes Nullable(UInt64),
    spill_bytes         Nullable(UInt64),
    error               String DEFAULT '',
    details             String DEFAULT ''  -- JSON
)
ENGINE = MergeTree
ORDER BY (job, started_at);

-- Latest check per (month, stage) wins.
CREATE TABLE IF NOT EXISTS ops.reconciliation
(
    month        LowCardinality(String),
    stage        LowCardinality(String),  -- source | bronze | silver | clickhouse
    expected     UInt64,
    actual       UInt64,
    quarantined  UInt64,
    reconciled   Bool,
    run_id       UUID,
    checked_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(checked_at)
ORDER BY (month, stage);
