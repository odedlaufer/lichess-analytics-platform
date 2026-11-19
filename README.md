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
```

## Local services
| Service | Profile | URL |
| --- | --- | --- |
| SeaweedFS S3 API | lake | http://localhost:18333 |
| SeaweedFS filer UI | lake | http://localhost:18888 |
| SeaweedFS master UI | lake | http://localhost:19333 |

## Layout
See `docs/adr/` for decisions. Full structure and roadmap tracked in the PRD.

## License
MIT. Lichess data is CC0.
