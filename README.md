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

## Layout
See `docs/adr/` for decisions. Full structure and roadmap tracked in the PRD.

## License
MIT. Lichess data is CC0.
