"""Chunker tests on synthetic PGN (no network, no S3)."""

import json
import random
from pathlib import Path

import pytest
import zstandard

from ingest.chunk import (
    GAME_START,
    chunk_month,
    count_game_starts,
    first_game_start,
    iter_complete_lines,
)

MONTH = "2013-01"


def make_pgn(n_games: int, seed: int = 7) -> bytes:
    rnd = random.Random(seed)
    games = []
    for i in range(n_games):
        moves = " ".join(f"{m}. e4 e5" for m in range(1, rnd.randint(1, 60)))
        games.append(
            f'[Event "Rated Blitz game"]\n[Site "https://lichess.org/g{i:07d}"]\n'
            f'[White "w{i}"]\n[Black "b{i}"]\n[Result "1-0"]\n\n{moves} 1-0\n\n'
        )
    return "".join(games).encode()


class LocalSink:
    def __init__(self, root: Path) -> None:
        self.root = root

    def delete_prefix(self, prefix: str) -> int:
        files = [
            p
            for p in self.root.rglob("*")
            if p.is_file() and str(p.relative_to(self.root)).startswith(prefix)
        ]
        for p in files:
            p.unlink()
        return len(files)

    def upload_file(self, local: Path, key: str) -> None:
        out = self.root / key
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(local.read_bytes())

    def put_json(self, key: str, obj: dict[str, object]) -> None:
        out = self.root / key
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(obj))


@pytest.fixture
def source(tmp_path: Path) -> tuple[Path, bytes]:
    raw = make_pgn(500)
    path = tmp_path / "downloads" / "src.pgn.zst"
    path.parent.mkdir()
    path.write_bytes(zstandard.ZstdCompressor().compress(raw))
    return path, raw


def _decompress(path: Path) -> bytes:
    return zstandard.ZstdDecompressor().decompressobj().decompress(path.read_bytes())


@pytest.mark.parametrize("block_bytes", [7, 64, 4096, 1 << 20])
@pytest.mark.parametrize("target_bytes", [1, 2_000, 50_000, 10 << 20])
def test_chunks_are_game_aligned_and_lossless(
    tmp_path: Path, source: tuple[Path, bytes], block_bytes: int, target_bytes: int
) -> None:
    src, raw = source
    lake = tmp_path / "lake"
    m = chunk_month(
        MONTH,
        src,
        LocalSink(lake),
        work_dir=tmp_path,
        target_bytes=target_bytes,
        block_bytes=block_bytes,
    )

    chunks = [_decompress(lake / c.key) for c in m.chunks]
    assert b"".join(chunks) == raw  # nothing lost, nothing duplicated, order kept
    assert all(c.startswith(GAME_START) for c in chunks)  # DQ-02
    assert [c.games for c in m.chunks] == [c.count(b"[Event ") for c in chunks]
    assert m.games_total == 500
    assert m.uncompressed_bytes_total == len(raw)
    if target_bytes >= len(raw):
        assert len(m.chunks) == 1


def test_manifest_written_last_and_complete(tmp_path: Path, source: tuple[Path, bytes]) -> None:
    src, _ = source
    lake = tmp_path / "lake"
    m = chunk_month(
        MONTH, src, LocalSink(lake), work_dir=tmp_path, target_bytes=10_000, block_bytes=1024
    )

    manifest = json.loads((lake / f"raw/{MONTH}/_manifest.json").read_text())
    assert manifest["games_total"] == 500
    assert len(manifest["chunks"]) == len(m.chunks) > 1
    assert manifest["source_sha256"] == m.source_sha256


def test_rerun_replaces_previous_chunks(tmp_path: Path, source: tuple[Path, bytes]) -> None:
    src, _ = source
    lake = tmp_path / "lake"
    chunk_month(MONTH, src, LocalSink(lake), work_dir=tmp_path, target_bytes=1_000, block_bytes=512)
    m = chunk_month(MONTH, src, LocalSink(lake), work_dir=tmp_path, target_bytes=50_000)

    on_disk = sorted(p.name for p in (lake / f"raw/{MONTH}").glob("chunk-*"))
    assert on_disk == sorted(Path(c.key).name for c in m.chunks)  # FR-DL5: no leftovers


def test_temp_files_cleaned_up(tmp_path: Path, source: tuple[Path, bytes]) -> None:
    src, _ = source
    chunk_month(MONTH, src, LocalSink(tmp_path / "lake"), work_dir=tmp_path, target_bytes=1_000)
    assert not any((tmp_path / "tmp").rglob("*.zst"))


def test_count_game_starts() -> None:
    assert count_game_starts(b'[Event "a"]\nx\n[Event "b"]\n', at_line_start=True) == 2
    assert count_game_starts(b'[Event "a"]\n', at_line_start=False) == 0
    assert count_game_starts(b'1. e4 {[Event "x"]}\n', at_line_start=True) == 0


def test_first_game_start() -> None:
    assert first_game_start(b'[Event "a"]') == 0
    assert first_game_start(b'1. e4\n[Event "a"]') == 6
    assert first_game_start(b"1. e4\n") == -1


def test_iter_complete_lines_never_splits_a_line() -> None:
    import io

    data = b"aa\nbbbb\nc\n\ndddddd\ntail"
    blocks = list(iter_complete_lines(io.BytesIO(data), 3))
    assert b"".join(blocks) == data
    assert all(b.endswith(b"\n") for b in blocks[:-1])


def test_peak_local_counts_only_this_month(tmp_path: Path, source: tuple[Path, bytes]) -> None:
    src, _ = source
    (tmp_path / "downloads" / "other-month.pgn.zst").write_bytes(b"x" * 1_000_000)
    m = chunk_month(MONTH, src, LocalSink(tmp_path / "lake"), work_dir=tmp_path)

    assert m.source_bytes < m.peak_local_bytes < m.source_bytes + 1_000_000
