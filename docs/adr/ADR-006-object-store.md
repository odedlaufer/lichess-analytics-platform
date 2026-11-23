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

- S3 API: `http://localhost:28333` (host ports in the 2xxxx range to avoid clashing with other local projects)
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

## Incident: OOM kill during chunk upload (2026-11-23)
Chunking 2016-01 (831 MiB, 256 MiB chunks) killed SeaweedFS mid-upload. The kernel log
showed a memory-cgroup OOM kill of `weed` at ~890 MiB RSS against the 1 GB limit (Docker's
`OOMKilled` flag read false; `dmesg` is authoritative). After the restart the master took
~16 s to regain leadership, so requests in that window failed.

Causes: one `weed server` process runs master + volume + filer + S3, so uploads are buffered
in several layers; the Go GC is unaware of the cgroup limit; the client sent 10 parallel parts.

Fix:
- container limit 2 GB and `GOMEMLIMIT=1536MiB`
- `-s3/-filer/-volume.concurrentUploadLimitMB=64`
- chunk uploads with `max_concurrency=2`, 16 MiB parts
- `-master.telemetry=false` (it reports usage once 10 GiB are stored)
