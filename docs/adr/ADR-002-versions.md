# ADR-002: Version matrix

- Status: Proposed (filled in as each service lands in Phase 0)
- Date: 2026-11-19

Images are pinned by tag **and** digest in `docker-compose.yml`. Python packages are pinned by `uv.lock`.

| Component | Version | Pin | Added in |
| --- | --- | --- | --- |
| Python | 3.12 | `.python-version` | 0.1 |
| SeaweedFS | 4.54 | `chrislusf/seaweedfs:4.54@sha256:4b42c212…1222d15` | 0.2 |
| Spark | 4.1.1 (Scala 2.13) | `spark:4.1.1-scala2.13-java17-python3-ubuntu@sha256:6383b289…dca0b1ad` | 0.3 |
| Java | 17.0.19 | from Spark base image | 0.3 |
| Hadoop (client) | 3.4.2 | from Spark base image | 0.3 |
| hadoop-aws | 3.4.2 | must equal Hadoop client version; `spark/Dockerfile` | 0.3 |
| AWS SDK v2 bundle | 2.29.52 | Hadoop 3.4.2's `aws-java-sdk-v2.version`; `spark/Dockerfile` | 0.3 |
| Delta Lake | 4.4.0 (`delta-spark_4.1_2.13`) | supports Spark 4.0.1 / 4.1.0 / 4.2.0; `spark/Dockerfile` | 0.3 |
| Python in Spark image | 3.12 (deadsnakes) | matches repo; driver and executors must match | 0.3 |
| PySpark (dev, typing/tests) | 4.1.1 | `pyproject.toml`, equals cluster version | 0.3 |

Why Spark 4.1.1 and not 4.2: Delta 4.4.0 supports 4.2.0, but the official Spark image is published up to 4.1.1.

## Laptop memory budget (Docker ≈ 9.7 GB)
| Service | Limit |
| --- | --- |
| seaweedfs | 1 GB |
| spark-master | 768 MB |
| spark-history | 768 MB |
| spark-worker × 2 | 3.5 GB each (2.5 GB executor pool + 512 MB daemon + Python workers) |
| spark-submit (driver, on demand) | 2 GB |
| clickhouse | 3 GB |
| Kafka (KRaft) | TBD | | 0.2 |
| ClickHouse | 26.8.40.7 (26.8 LTS) | `clickhouse/clickhouse-server:26.8.40.7@sha256:6d462904…2330d59` | 0.4 |
| Postgres | TBD | | 0.2 |
| Prefect | TBD | | 0.2 |
| Superset | TBD | | 0.2 |
