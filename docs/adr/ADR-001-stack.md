# ADR-001: Stack choice

- Status: Accepted
- Date: 2026-11-19

## Context
Portfolio project on real Lichess data. Must differ from prior projects (Databricks/Delta; Flink/Iceberg/dbt) and expose engineering that managed services hide.

## Decision
| Layer | Choice |
| --- | --- |
| Batch compute | Apache Spark standalone (Docker) |
| Table format | Delta Lake OSS on MinIO |
| Transport | Apache Kafka (KRaft) |
| Real-time + OLAP | ClickHouse (Kafka engine + MVs) |
| Transformations | SQLMesh (state in Postgres) |
| Orchestration | Prefect 3 OSS |
| Data quality | Pandera (PySpark), SQLMesh audits, ClickHouse constraints |
| BI | Apache Superset |
| Shared | Python + uv, Docker Compose, GitHub Actions, Slack webhook |

## Consequences
- Cluster sizing, partitioning, AQE and skew are handled explicitly.
- Version compatibility (Spark / Delta / hadoop-aws / Java) is a real risk; pinned in ADR-002.
- Everything runs on one ~16 GB laptop; design is chunk-process-delete.
