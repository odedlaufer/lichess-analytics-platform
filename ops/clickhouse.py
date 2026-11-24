"""Minimal ClickHouse HTTP client. Standard library only, so it also runs in the Spark image."""

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Any


class ClickHouseError(RuntimeError):
    """ClickHouse rejected a request; the message is ClickHouse's own error text."""


class ClickHouseHTTP:
    def __init__(self, url: str, user: str, password: str, timeout_s: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.user = user
        self.password = password
        self.timeout_s = timeout_s

    @classmethod
    def from_env(cls) -> "ClickHouseHTTP":
        return cls(
            os.environ["CLICKHOUSE_URL"],
            os.environ.get("CLICKHOUSE_OPS_USER", "ops_writer"),
            os.environ["CLICKHOUSE_OPS_PASSWORD"],
        )

    def _post(self, query: str, body: bytes = b"") -> str:
        params = urllib.parse.urlencode({"query": query, "date_time_input_format": "best_effort"})
        req = urllib.request.Request(
            f"{self.url}/?{params}",
            data=body,
            method="POST",
            headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                return str(resp.read().decode())
        except urllib.error.HTTPError as err:
            detail = err.read().decode(errors="replace").strip()
            raise ClickHouseError(f"HTTP {err.code}: {detail[:800]}") from err

    def insert(self, table: str, rows: Sequence[Mapping[str, Any]]) -> None:
        body = "\n".join(json.dumps(r, default=str) for r in rows).encode()
        self._post(f"INSERT INTO {table} FORMAT JSONEachRow", body)

    def query(self, sql: str) -> str:
        return self._post(sql)
