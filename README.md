# Lichess Analytics Platform

Large-scale batch (Spark + Delta on MinIO) and real-time OLAP (Kafka + ClickHouse) analytics on public Lichess data.

> Status: Phase 1 complete: download → chunk → bronze with exact reconciliation; baseline 37,400 games/s (see `docs/benchmark.md`). Next: Phase 2, silver.

## Phase 0 gate
| Check | Result |
| --- | --- |
| `make up-all`: 10 services healthy | ✅ ~2.9 GB idle memory |
| Spark writes Delta to the lake (`make gate`) | ✅ 1,000 rows in 12 s |
| ClickHouse reads it via `s3()` and `deltaLake()` | ✅ count and sum match |
| `make test-integration` | ✅ 20 passed |

## Quickstart
Requires: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker.

```bash
cp .env.example .env            # then set S3_SECRET_KEY
make setup
make lint typecheck test        # unit tests, no services needed
make up-all                     # every service
make up PROFILES=lake           # object store only
make test-integration           # tests against running services
make build PROFILES="lake spark"        # build the Spark image
make up PROFILES="lake spark"           # + Spark cluster
make spark-smoke PROFILES="lake spark"  # Delta round-trip on the cluster
make up PROFILES="lake spark olap"      # + ClickHouse
make gate PROFILES="lake spark olap"    # Phase 0 gate: Spark -> Delta -> ClickHouse
make up PROFILES="lake stream"          # + Kafka (creates topics)
```

## Local services
| Service | Profile | URL |
| --- | --- | --- |
| SeaweedFS S3 API | lake | http://localhost:28333 |
| SeaweedFS filer UI | lake | http://localhost:28888 |
| SeaweedFS master UI | lake | http://localhost:29333 |
| Spark master UI | spark | http://localhost:28080 |
| Spark worker UIs | spark | http://localhost:28081, :28082 |
| Spark History Server | spark | http://localhost:28180 |
| ClickHouse HTTP | olap | http://localhost:28123 (play UI: /play) |
| ClickHouse native | olap | localhost:29000 |
| Kafka (host clients) | stream | localhost:29092 |
| Postgres (metadata) | olap, orch, bi | localhost:25432 |
| Prefect UI / API | orch | http://localhost:24200 |
| Superset | bi (+ olap) | http://localhost:28088 |

## Layout
See `docs/adr/` for decisions. Full structure and roadmap tracked in the PRD.

## License
MIT. Lichess data is CC0.
