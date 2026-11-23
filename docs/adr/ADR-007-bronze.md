# ADR-007: Bronze ingestion design

- Status: Accepted
- Date: 2026-11-23

## Evidence (probes on the cluster image, 2013-01)
| Question | Result |
| --- | --- |
| Can Spark read our `.pgn.zst` chunks without native libs? | Yes: `spark.read.text` decompresses zstd |
| Does `lineSep="\n[Event "` give one record per game? | Yes: 121,332 records = manifest = Lichess |
| Can Spark's file sources read `_manifest.json`? | No: names starting with `_`/`.` are treated as hidden |

## Decisions
- **Pure Spark, no Python UDFs** (FR-SP5): records split by `lineSep`, headers and movetext
  via `regexp_extract_all` / `map_from_arrays`.
- **Manifest is the contract**: the job reads `_manifest.json` through the Hadoop FS API and
  processes exactly the chunk paths it lists (no globbing).
- **Bronze schema**: `game_id, month, chunk_id, headers map<string,string>, movetext,
  _ingested_at`; partitioned by `month`; path `lake/bronze/games_raw`.
- **One quarantine table** for all layers: `lake/quarantine/games`, partitioned by
  `(layer, month)`, with `rule_failed` and `raw_text`.
- **Structural rules only at bronze**: `no_headers`, `duplicate_header_keys` (also avoids
  `map_from_arrays` failing on duplicate keys), `missing_game_id`, `duplicate_game_id`
  (DQ-04: first copy by chunk kept).
- **Idempotent writes**: `replaceWhere` on the month (and layer) partition.
- **Reconciliation on written data** (DQ-03): bronze + quarantine rows read back from Delta
  must equal `games_total`; the job exits non-zero otherwise.

## Trade-off
No `persist()`: the source is read once per output (bronze, quarantine) plus once to find
duplicate ids. Caching ~200 GB of raw text would spill to the laptop disk; re-reading
compressed chunks from the lake is cheaper. Revisit in the Phase 5 benchmark.
