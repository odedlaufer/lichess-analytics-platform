-- Benchmark results (PRD 8.5). One row per (bench run, month, stage).
CREATE TABLE IF NOT EXISTS ops.benchmark_results
(
    bench_id      UUID,
    experiment    LowCardinality(String),   -- BM-1 .. BM-6
    variant       LowCardinality(String),   -- e.g. baseline
    month         LowCardinality(String),
    stage         LowCardinality(String),   -- download | chunk | bronze | month
    games         UInt64,
    raw_bytes     UInt64,                   -- uncompressed PGN
    seconds       Float64,
    games_per_s   Float64,
    raw_mib_per_s Float64,
    peak_disk_ratio Nullable(Float64),
    config        String,                   -- JSON: cores, memory, chunk size, git sha
    measured_at   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (experiment, variant, month, stage, measured_at);
