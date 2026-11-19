# ADR-006: Object store — SeaweedFS instead of MinIO

- Status: Accepted
- Date: 2026-11-19
- Amends: ADR-001 (object store layer)

## Context
The PRD assumed MinIO as the local S3-compatible store. MinIO stopped publishing
community Docker images and binaries in October 2025 and archived its community
repository in 2026. `minio/minio` no longer pulls, and there is no patched
community release channel.

## Options
| Option | Notes |
| --- | --- |
| Community MinIO fork | Drop-in, but depends on a single third-party maintainer |
| RustFS | MinIO-like UX, Apache 2.0, younger project |
| Garage | Lightweight, but a narrower S3 feature set |
| **SeaweedFS** | Apache 2.0, maintained since 2015, S3 API gateway, single container |

## Decision
Use SeaweedFS (`weed server -s3`) as a single container in the `lake` profile.

- S3 API: `http://localhost:18333` (host ports in the 1xxxx range to avoid clashing with other local projects)
- Auth: one admin identity generated at startup from `S3_ACCESS_KEY` / `S3_SECRET_KEY` in `.env`
- Bucket `lake` created idempotently by `make buckets` (called by `make up`)
- Clients use path-style addressing and SigV4

## Consequences
- Spark (s3a) and ClickHouse (s3 table function) must be verified against SeaweedFS
  in their own sub-tasks; any incompatibility gets recorded here.
- Delta on S3 is single-writer safe without a LogStore DynamoDB setup; this project
  has one writer per table, so that holds.
- Integration tests (`make test-integration`) cover bucket existence, object
  round-trip and credential rejection.
