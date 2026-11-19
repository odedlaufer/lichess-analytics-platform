# Lichess Analytics Platform

Large-scale batch (Spark + Delta on MinIO) and real-time OLAP (Kafka + ClickHouse) analytics on public Lichess data.

> Status: Phase 0 — setup.

## Quickstart
Requires: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker.

```bash
cp .env.example .env
make setup
make lint typecheck test
```

## Layout
See `docs/adr/` for decisions. Full structure and roadmap tracked in the PRD.

## License
MIT. Lichess data is CC0.
