"""Download one month of Lichess standard rated games (FR-DL1, DQ-01).

- Resumable: an interrupted download continues from the ``.part`` file via HTTP Range.
- Verified: SHA-256 must match the published ``sha256sums.txt``; a mismatch deletes the
  partial file and fails.
- Idempotent: an already verified file is not downloaded again.
- Fair use: one connection, exponential backoff, at least 60 s after HTTP 429.
"""

import argparse
import hashlib
import logging
import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx

BASE_URL = "https://database.lichess.org/standard"
FIRST_MONTH = "2013-01"
MONTH_RE = re.compile(r"^20\d{2}-(0[1-9]|1[0-2])$")
CHUNK_BYTES = 1 << 20
PROGRESS_EVERY_BYTES = 256 << 20
RATE_LIMIT_WAIT_S = 60.0
USER_AGENT = "lichess-analytics-platform (portfolio; github.com/odedlaufer)"

log = logging.getLogger(__name__)


class ChecksumMismatchError(RuntimeError):
    pass


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    sha256: str
    size_bytes: int
    skipped: bool


def validate_month(month: str) -> str:
    if not MONTH_RE.match(month) or month < FIRST_MONTH:
        raise ValueError(f"month must be YYYY-MM and >= {FIRST_MONTH}, got {month!r}")
    return month


def file_name(month: str) -> str:
    return f"lichess_db_standard_rated_{validate_month(month)}.pgn.zst"


def parse_sha256sums(text: str) -> dict[str, str]:
    """Parse ``<sha256>  <file name>`` lines (sha256sum format)."""
    sums: dict[str, str] = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 2:
            digest, name = parts
            sums[name.lstrip("*")] = digest.lower()
    return sums


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(CHUNK_BYTES):
            h.update(chunk)
    return h.hexdigest()


def fetch_expected_sha256(client: httpx.Client, month: str) -> str:
    resp = client.get(f"{BASE_URL}/sha256sums.txt")
    resp.raise_for_status()
    name = file_name(month)
    sums = parse_sha256sums(resp.text)
    if name not in sums:
        raise LookupError(f"{name} is not in the published sha256sums.txt")
    return sums[name]


def _download_to_part(client: httpx.Client, url: str, part: Path) -> None:
    offset = part.stat().st_size if part.exists() else 0
    headers = {"Range": f"bytes={offset}-"} if offset else {}
    with client.stream("GET", url, headers=headers) as resp:
        if resp.status_code == 416:  # nothing left to fetch; the checksum decides
            return
        resp.raise_for_status()
        if offset and resp.status_code != 206:
            log.warning("server ignored Range; restarting from byte 0")
            offset = 0
        if offset:
            log.info("resuming at %.1f MiB", offset / 2**20)
        written, next_report = offset, offset + PROGRESS_EVERY_BYTES
        with part.open("ab" if offset else "wb") as fh:
            for chunk in resp.iter_bytes(CHUNK_BYTES):
                fh.write(chunk)
                written += len(chunk)
                if written >= next_report:
                    log.info("downloaded %.0f MiB", written / 2**20)
                    next_report += PROGRESS_EVERY_BYTES


def _retry_wait(exc: Exception, attempt: int, backoff_s: float) -> float | None:
    """Seconds to wait before retrying, or None if the error is not retryable."""
    backoff: float = backoff_s * 2 ** (attempt - 1)
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 429:
            return max(RATE_LIMIT_WAIT_S, backoff)
        if status < 500:
            return None
    return backoff


def download_month(
    month: str,
    dest: Path,
    *,
    client: httpx.Client | None = None,
    max_attempts: int = 5,
    backoff_s: float = 2.0,
    sleep: Callable[[float], None] = time.sleep,
) -> DownloadResult:
    dest.mkdir(parents=True, exist_ok=True)
    final = dest / file_name(month)
    part = final.with_name(final.name + ".part")
    url = f"{BASE_URL}/{final.name}"

    own_client = client is None
    http = client or httpx.Client(
        timeout=httpx.Timeout(30.0, read=120.0),
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    )
    try:
        expected = fetch_expected_sha256(http, month)

        if final.exists():
            if sha256_file(final) == expected:
                log.info("%s already downloaded and verified", final.name)
                return DownloadResult(final, expected, final.stat().st_size, skipped=True)
            log.warning("%s exists but fails checksum; downloading again", final.name)
            final.unlink()

        for attempt in range(1, max_attempts + 1):
            try:
                _download_to_part(http, url, part)
                break
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                wait = _retry_wait(exc, attempt, backoff_s)
                if wait is None or attempt == max_attempts:
                    raise
                log.warning(
                    "attempt %d/%d failed (%s); retry in %.0fs", attempt, max_attempts, exc, wait
                )
                sleep(wait)

        actual = sha256_file(part)
        if actual != expected:
            part.unlink()
            raise ChecksumMismatchError(f"{final.name}: expected {expected}, got {actual}")
        part.rename(final)
        return DownloadResult(final, actual, final.stat().st_size, skipped=False)
    finally:
        if own_client:
            http.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--month", required=True, help="YYYY-MM, e.g. 2013-01")
    parser.add_argument(
        "--dest",
        type=Path,
        default=Path(os.environ.get("DATA_DIR", "data")) / "downloads",
        help="target directory (default: $DATA_DIR/downloads)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    started = time.monotonic()
    result = download_month(args.month, args.dest)
    print(
        f"DOWNLOAD month={args.month} file={result.path} "
        f"mib={result.size_bytes / 2**20:.1f} sha256={result.sha256} "
        f"skipped={result.skipped} seconds={time.monotonic() - started:.1f}"
    )


if __name__ == "__main__":
    main()
