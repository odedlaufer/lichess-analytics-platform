-- Data-quality check results (DQ-07 header drift, silver contract, ...). Idempotent.
CREATE TABLE IF NOT EXISTS ops.dq_results
(
    run_id      UUID,
    month       LowCardinality(String),
    layer       LowCardinality(String),
    check_id    LowCardinality(String),        -- e.g. DQ-07, silver_contract
    status      Enum8('pass' = 1, 'warn' = 2, 'fail' = 3, 'baseline' = 4),
    details     String,                        -- JSON
    checked_at  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (check_id, month, checked_at);
