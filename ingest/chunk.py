"""Split one month's .pgn.zst into game-aligned, independently compressed chunks.

FR-DL2..DL5, DQ-02. The source is stream-decompressed block by block and never
materialised. A new chunk starts at the first game boundary after the current chunk
reaches the target size, so every chunk starts with ``[Event `` and holds whole games.

Chunk sizes overshoot the target by at most one read block (8 MiB vs 256 MiB).

A game boundary is a line starting with ``[Event ``. Lichess movetext is a single line
and comments are inline, so no other line can start with that tag.

Output in the lake (idempotent: the month prefix is wiped first; the manifest is
written last and is the commit marker downstream jobs wait for):

    raw/{month}/chunk-00000.pgn.zst ...
    raw/{month}/_manifest.json      games per chunk, total games (source of truth)
"""

import argparse
import json
import logging
import os
import shutil
import time
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO, Protocol

import zstandard
from boto3.s3.transfer import TransferConfig

from ingest.download import file_name, sha256_file, validate_month
from ops.runs import RunRecorder, default_sink

GAME_START = b"[Event "
NL_GAME_START = b"\n" + GAME_START
DEFAULT_TARGET_BYTES = 256 << 20
READ_BLOCK_BYTES = 8 << 20
ZSTD_LEVEL = 3
# Few parallel parts: SeaweedFS buffers in-flight uploads in memory (ADR-006).
UPLOAD_CONFIG = TransferConfig(multipart_chunksize=16 << 20, max_concurrency=2)

log = logging.getLogger(__name__)


class ChunkSink(Protocol):
    """Where chunks go. S3 in production, a local directory in tests."""

    def delete_prefix(self, prefix: str) -> int: ...
    def upload_file(self, local: Path, key: str) -> None: ...
    def put_json(self, key: str, obj: dict[str, object]) -> None: ...


@dataclass
class ChunkInfo:
    key: str
    games: int
    uncompressed_bytes: int
    compressed_bytes: int


@dataclass
class Manifest:
    month: str
    source_file: str
    source_sha256: str
    source_bytes: int
    target_chunk_bytes: int
    games_total: int = 0
    uncompressed_bytes_total: int = 0
    peak_local_bytes: int = 0
    duration_s: float = 0.0
    created_at: str = ""
    chunks: list[ChunkInfo] = field(default_factory=list)


def count_game_starts(data: bytes, at_line_start: bool) -> int:
    """Games starting in ``data``; ``at_line_start`` says whether data[0] begins a line."""
    n = data.count(NL_GAME_START)
    if at_line_start and data.startswith(GAME_START):
        n += 1
    return n


def first_game_start(data: bytes) -> int:
    """Offset of the first game start in ``data`` (which begins at a line start), or -1."""
    if data.startswith(GAME_START):
        return 0
    idx = data.find(NL_GAME_START)
    return idx + 1 if idx >= 0 else -1


def iter_complete_lines(stream: BinaryIO, block_bytes: int) -> Iterator[bytes]:
    """Yield blocks that start at a line start and end with a newline (except the tail)."""
    carry = b""
    while block := stream.read(block_bytes):
        buf = carry + block
        cut = buf.rfind(b"\n")
        if cut < 0:
            carry = buf
            continue
        carry = buf[cut + 1 :]
        yield buf[: cut + 1]
    if carry:
        yield carry


def dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


class _ChunkWriter:
    def __init__(self, tmp_dir: Path, month: str, index: int) -> None:
        self.key = f"raw/{month}/chunk-{index:05d}.pgn.zst"
        self.path = tmp_dir / f"chunk-{index:05d}.pgn.zst"
        self._fh = self.path.open("wb")
        self._zw = zstandard.ZstdCompressor(level=ZSTD_LEVEL, threads=-1).stream_writer(
            self._fh, closefd=False
        )
        self.games = 0
        self.uncompressed = 0

    def write(self, data: bytes, at_line_start: bool) -> None:
        if not data:
            return
        if self.uncompressed == 0 and not data.startswith(GAME_START):
            raise ValueError(f"{self.key} would not start at a game boundary")
        self._zw.write(data)
        self.games += count_game_starts(data, at_line_start)
        self.uncompressed += len(data)

    def close(self) -> ChunkInfo:
        self._zw.close()  # type: ignore[no-untyped-call]  # zstandard stub gap
        self._fh.close()
        return ChunkInfo(self.key, self.games, self.uncompressed, self.path.stat().st_size)


def chunk_month(
    month: str,
    source: Path,
    sink: ChunkSink,
    *,
    work_dir: Path,
    target_bytes: int = DEFAULT_TARGET_BYTES,
    block_bytes: int = READ_BLOCK_BYTES,
) -> Manifest:
    validate_month(month)
    started = time.monotonic()
    tmp_dir = work_dir / "tmp" / f"chunks-{month}"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    tmp_dir.mkdir(parents=True)

    manifest = Manifest(
        month=month,
        source_file=source.name,
        source_sha256=sha256_file(source),
        source_bytes=source.stat().st_size,
        target_chunk_bytes=target_bytes,
    )
    removed = sink.delete_prefix(f"raw/{month}/")
    if removed:
        log.info("removed %d existing objects under raw/%s/", removed, month)

    def finish(writer: _ChunkWriter) -> None:
        info = writer.close()
        # This month's footprint: the source file plus its pending chunk (FR-DL4).
        local = manifest.source_bytes + dir_size(tmp_dir)
        manifest.peak_local_bytes = max(manifest.peak_local_bytes, local)
        sink.upload_file(writer.path, info.key)
        writer.path.unlink()
        manifest.chunks.append(info)
        log.info(
            "%s: %d games, %.0f MiB raw", info.key, info.games, info.uncompressed_bytes / 2**20
        )

    try:
        index = 0
        writer = _ChunkWriter(tmp_dir, month, index)
        with source.open("rb") as fh, zstandard.ZstdDecompressor().stream_reader(fh) as reader:
            for block in iter_complete_lines(reader, block_bytes):
                if writer.uncompressed >= target_bytes:
                    pos = first_game_start(block)
                    if pos >= 0:
                        writer.write(block[:pos], at_line_start=True)
                        finish(writer)
                        index += 1
                        writer = _ChunkWriter(tmp_dir, month, index)
                        block = block[pos:]
                writer.write(block, at_line_start=True)
        if writer.uncompressed:
            finish(writer)
        else:
            writer.close()
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    manifest.games_total = sum(c.games for c in manifest.chunks)
    manifest.uncompressed_bytes_total = sum(c.uncompressed_bytes for c in manifest.chunks)
    manifest.duration_s = round(time.monotonic() - started, 1)
    manifest.created_at = datetime.now(UTC).isoformat(timespec="seconds")
    sink.put_json(f"raw/{month}/_manifest.json", asdict(manifest))
    return manifest


class S3ChunkSink:
    def __init__(self, bucket: str) -> None:
        from ingest.s3 import make_s3_client

        self.bucket = bucket
        self.s3 = make_s3_client()

    def delete_prefix(self, prefix: str) -> int:
        removed = 0
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys})  # type: ignore[typeddict-item]
                removed += len(keys)
        return removed

    def upload_file(self, local: Path, key: str) -> None:
        self.s3.upload_file(str(local), self.bucket, key, Config=UPLOAD_CONFIG)

    def put_json(self, key: str, obj: dict[str, object]) -> None:
        body = json.dumps(obj, indent=2).encode()
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")


def main() -> None:
    parser = argparse.ArgumentParser(description="Chunk one month into the lake (FR-DL2).")
    parser.add_argument("--month", required=True)
    parser.add_argument("--target-mb", type=int, default=DEFAULT_TARGET_BYTES >> 20)
    parser.add_argument(
        "--keep-download",
        action="store_true",
        help="keep the downloaded file (default: delete after success, FR-DL4)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    data_dir = Path(os.environ.get("DATA_DIR", "data"))
    source = data_dir / "downloads" / file_name(args.month)
    if not source.exists():
        raise SystemExit(f"{source} not found; run `make download M={args.month}` first")

    with RunRecorder("chunk", args.month, default_sink()) as rec:
        m = chunk_month(
            args.month,
            source,
            S3ChunkSink(os.environ["LAKE_BUCKET"]),
            work_dir=data_dir,
            target_bytes=args.target_mb << 20,
        )
        rec.set(
            rows_out=m.games_total,
            bytes_in=m.source_bytes,
            bytes_out=sum(c.compressed_bytes for c in m.chunks),
        )
        rec.details.update(
            chunks=len(m.chunks),
            uncompressed_bytes=m.uncompressed_bytes_total,
            peak_local_bytes=m.peak_local_bytes,
            source_bytes=m.source_bytes,
            source_sha256=m.source_sha256,
        )
        # FR-DL3: the chunker's count is the source of truth for every later stage.
        rec.reconcile("source", expected=m.games_total, actual=m.games_total)
    if not args.keep_download:
        source.unlink()
    print(
        f"CHUNK month={m.month} games={m.games_total} chunks={len(m.chunks)} "
        f"raw_mib={m.uncompressed_bytes_total / 2**20:.0f} "
        f"source_mib={m.source_bytes / 2**20:.1f} peak_local_mib={m.peak_local_bytes / 2**20:.1f} "
        f"peak_ratio={m.peak_local_bytes / m.source_bytes:.2f} seconds={m.duration_s}"
    )


if __name__ == "__main__":
    main()
