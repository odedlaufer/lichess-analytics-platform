# Lichess Analytics Platform

Large-scale batch (Spark + Delta on MinIO) and real-time OLAP (Kafka + ClickHouse) analytics on public Lichess data.

> Status: Phase 0 — setup.

## Quickstart
Requires: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker.

```bash
cp .env.example .env            # then set S3_SECRET_KEY
make setup
make lint typecheck test        # unit tests, no services needed
make up PROFILES=lake           # object store
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

## Layout
See `docs/adr/` for decisions. Full structure and roadmap tracked in the PRD.

## License
MIT. Lichess data is CC0.
