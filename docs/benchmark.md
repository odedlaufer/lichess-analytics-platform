# Benchmarks

Results are stored in `ops.benchmark_results` (ClickHouse). Stage timings come from the
pipeline's own `ops.pipeline_runs` rows. Run with `make bench MONTHS="..."`.

## BM-1 baseline (Phase 1)

Laptop: Docker VM 11.7 GB, Spark standalone 2 workers × 2 cores, executor 2 GiB,
chunk target 256 MiB, nothing else running on Docker.

| Month | Stage | Games | Raw MiB | Seconds | Games/s | Raw MiB/s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2013-01 | chunk | 121,332 | 89 | 0.5 | 250,686 | 182.9 |
| 2013-01 | bronze | 121,332 | 89 | 22.2 | 5,472 | 4.0 |
| 2013-01 | month (chunk + bronze) | 121,332 | 89 | 22.7 | 5,355 | 3.9 |
| 2016-01 | chunk | 4,770,357 | 4,351 | 20.6 | 231,458 | 211.1 |
| 2016-01 | bronze | 4,770,357 | 4,351 | 127.5 | 37,402 | 34.1 |
| 2016-01 | month (chunk + bronze) | 4,770,357 | 4,351 | 148.2 | 32,199 | 29.4 |

Download is network-bound and excluded from processing time (2016-01: 47 s and 254 s on
two days for the same file).

**Reading the numbers.** From the two months, bronze costs ≈ 20 s fixed per run (Spark
startup, manifest, reconciliation reads) plus ≈ 44,000 games/s marginal. Small months are
dominated by the fixed part; that is the first Phase 5 lever.

## Targets (PRD section 3, set at the end of Phase 1)

| Metric | Baseline | Target |
| --- | --- | --- |
| Batch throughput (bronze, 2016-01) | 37,400 games/s | ≥ 75,000 games/s by Phase 5 (2×) |
| Month processing time (2016-01, download complete → bronze) | 148 s | report per stage as silver/gold land |
| Month processing time (recent ~95M-game month) | extrapolated ≈ 50 min | measured in Phase 5 |
| Peak disk | 1.09× (2016-01) | < 1.5× compressed |
| Row-count reconciliation | exact (2013-01, 2016-01) | exact, every month |

The recent-month figure is a linear extrapolation (chunker at ~210 MiB/s over ~200 GB raw,
bronze at the marginal rate); Phase 5 replaces it with a measurement.
