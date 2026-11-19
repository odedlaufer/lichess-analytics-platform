# ADR-002: Version matrix

- Status: Proposed (filled in as each service lands in Phase 0)
- Date: 2026-11-19

Images are pinned by tag **and** digest in `docker-compose.yml`. Python packages are pinned by `uv.lock`.

| Component | Version | Pin | Added in |
| --- | --- | --- | --- |
| Python | 3.12 | `.python-version` | 0.1 |
| SeaweedFS | 4.54 | `chrislusf/seaweedfs:4.54@sha256:4b42c212…1222d15` | 0.2 |
| Spark | TBD | | 0.2 |
| Delta Lake | TBD | | 0.2 |
| hadoop-aws | TBD | | 0.2 |
| Java | TBD | | 0.2 |
| Kafka (KRaft) | TBD | | 0.2 |
| ClickHouse | TBD | | 0.2 |
| Postgres | TBD | | 0.2 |
| Prefect | TBD | | 0.2 |
| Superset | TBD | | 0.2 |
