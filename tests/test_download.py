"""Downloader tests against an in-memory HTTP server (no network)."""

import hashlib
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from ingest.download import (
    BASE_URL,
    ChecksumMismatchError,
    download_month,
    file_name,
    parse_sha256sums,
    validate_month,
)

MONTH = "2013-01"
NAME = file_name(MONTH)
PAYLOAD = bytes(range(256)) * 4096  # 1 MiB of deterministic bytes
DIGEST = hashlib.sha256(PAYLOAD).hexdigest()


class FakeServer:
    """Serves sha256sums.txt and the month file; supports Range like a real server."""

    def __init__(
        self,
        payload: bytes = PAYLOAD,
        digest: str = DIGEST,
        honor_range: bool = True,
        fail_first: int = 0,
        fail_status: int = 503,
    ) -> None:
        self.payload, self.digest, self.honor_range = payload, digest, honor_range
        self.fail_first, self.fail_status = fail_first, fail_status
        self.file_requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == f"{BASE_URL}/sha256sums.txt":
            return httpx.Response(200, text=f"{self.digest}  {NAME}\nffff  other.pgn.zst\n")
        if url == f"{BASE_URL}/{NAME}":
            self.file_requests.append(request)
            if len(self.file_requests) <= self.fail_first:
                return httpx.Response(self.fail_status)
            rng = request.headers.get("Range")
            if rng and self.honor_range:
                start = int(rng.removeprefix("bytes=").removesuffix("-"))
                if start >= len(self.payload):
                    return httpx.Response(416)
                return httpx.Response(206, content=self.payload[start:])
            return httpx.Response(200, content=self.payload)
        return httpx.Response(404)

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self.handler))


def _no_sleep() -> tuple[list[float], Callable[[float], None]]:
    waits: list[float] = []
    return waits, waits.append


def test_full_download_verifies_and_renames(tmp_path: Path) -> None:
    server = FakeServer()
    result = download_month(MONTH, tmp_path, client=server.client())

    assert result.path == tmp_path / NAME
    assert result.path.read_bytes() == PAYLOAD
    assert result.sha256 == DIGEST
    assert not result.skipped
    assert not (tmp_path / f"{NAME}.part").exists()


def test_resume_requests_only_missing_bytes(tmp_path: Path) -> None:
    half = len(PAYLOAD) // 2
    (tmp_path / f"{NAME}.part").write_bytes(PAYLOAD[:half])
    server = FakeServer()

    result = download_month(MONTH, tmp_path, client=server.client())

    assert server.file_requests[0].headers["Range"] == f"bytes={half}-"
    assert result.path.read_bytes() == PAYLOAD


def test_server_ignoring_range_restarts_cleanly(tmp_path: Path) -> None:
    (tmp_path / f"{NAME}.part").write_bytes(PAYLOAD[:1000])
    server = FakeServer(honor_range=False)

    result = download_month(MONTH, tmp_path, client=server.client())

    assert result.path.read_bytes() == PAYLOAD


def test_checksum_mismatch_fails_and_removes_partial(tmp_path: Path) -> None:
    server = FakeServer(digest="0" * 64)

    with pytest.raises(ChecksumMismatchError):
        download_month(MONTH, tmp_path, client=server.client())

    assert not (tmp_path / NAME).exists()
    assert not (tmp_path / f"{NAME}.part").exists()


def test_verified_file_is_not_downloaded_again(tmp_path: Path) -> None:
    (tmp_path / NAME).write_bytes(PAYLOAD)
    server = FakeServer()

    result = download_month(MONTH, tmp_path, client=server.client())

    assert result.skipped
    assert server.file_requests == []


def test_corrupt_existing_file_is_replaced(tmp_path: Path) -> None:
    (tmp_path / NAME).write_bytes(b"corrupt")
    server = FakeServer()

    result = download_month(MONTH, tmp_path, client=server.client())

    assert not result.skipped
    assert result.path.read_bytes() == PAYLOAD


def test_retries_server_errors_with_backoff(tmp_path: Path) -> None:
    server = FakeServer(fail_first=2, fail_status=503)
    waits, sleep = _no_sleep()

    result = download_month(MONTH, tmp_path, client=server.client(), sleep=sleep)

    assert result.path.read_bytes() == PAYLOAD
    assert waits == [2.0, 4.0]


def test_rate_limit_waits_at_least_60s(tmp_path: Path) -> None:
    server = FakeServer(fail_first=1, fail_status=429)
    waits, sleep = _no_sleep()

    download_month(MONTH, tmp_path, client=server.client(), sleep=sleep)

    assert waits == [60.0]


def test_client_errors_are_not_retried(tmp_path: Path) -> None:
    server = FakeServer(fail_first=1, fail_status=404)
    waits, sleep = _no_sleep()

    with pytest.raises(httpx.HTTPStatusError):
        download_month(MONTH, tmp_path, client=server.client(), sleep=sleep)
    assert waits == []


def test_month_missing_from_checksums_fails(tmp_path: Path) -> None:
    server = FakeServer()
    with pytest.raises(LookupError):
        download_month("2013-02", tmp_path, client=server.client())


def test_parse_sha256sums() -> None:
    text = "ABC123  a.pgn.zst\ndef456 *b.pgn.zst\n\nnot a valid line here\n"
    assert parse_sha256sums(text) == {"a.pgn.zst": "abc123", "b.pgn.zst": "def456"}


@pytest.mark.parametrize("bad", ["2012-12", "2013-13", "2013-1", "13-01", "2013/01", ""])
def test_validate_month_rejects(bad: str) -> None:
    with pytest.raises(ValueError, match="YYYY-MM"):
        validate_month(bad)


def test_file_name() -> None:
    assert file_name("2016-07") == "lichess_db_standard_rated_2016-07.pgn.zst"
