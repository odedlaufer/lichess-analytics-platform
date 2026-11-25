"""Write golden fixtures from `extract_golden.py` output (stdin) with pseudonymized players.

Player names become `player-<hash>` (or a non-ASCII pseudonym when the original name was
non-ASCII, so the unicode case stays covered). Same name -> same pseudonym.

    make golden-extract
"""

import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

OUT = Path("spark/tests/golden")
PLAYER_LINE = re.compile(r'^\[(White|Black) "(.*)"\]$', re.MULTILINE)


def pseudonym(name: str) -> str:
    h = hashlib.sha256(name.encode()).hexdigest()[:8]
    return f"player-{h}" if name.isascii() else f"игрок-{h}"


def anonymize(item: dict[str, Any]) -> dict[str, Any]:
    pgn = PLAYER_LINE.sub(lambda m: f'[{m[1]} "{pseudonym(m[2])}"]', item["pgn"])
    silver = {**item["silver"]}
    silver["white"] = pseudonym(silver["white"])
    silver["black"] = pseudonym(silver["black"])
    return {**item, "pgn": pgn, "silver": silver}


def main() -> None:
    items = [anonymize(json.loads(line[7:])) for line in sys.stdin if line.startswith("GOLDEN ")]
    if not items:
        raise SystemExit("no GOLDEN lines on stdin")
    OUT.mkdir(parents=True, exist_ok=True)
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for it in items:
        by_month[it["month"]].append(it)
    for month, games in sorted(by_month.items()):
        (OUT / f"{month}.pgn").write_text("".join(g["pgn"] for g in games), encoding="utf-8")
    with (OUT / "expected_silver.jsonl").open("w", encoding="utf-8") as fh:
        for it in items:
            row = {k: it[k] for k in ("month", "category", "game_id", "silver")}
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    counts = Counter((it["month"], it["category"]) for it in items)
    print(f"GOLDEN games={len(items)} months={sorted(by_month)}")
    for (month, cat), n in sorted(counts.items()):
        print(f"  {month} {cat:17} {n}")


if __name__ == "__main__":
    main()
