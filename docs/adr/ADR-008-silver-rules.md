# ADR-008: Silver rules from profiled data

- Status: Accepted
- Date: 2026-11-24

## Evidence (`make profile-bronze`, 2013-01 and 2016-01)
| Field | Finding | 2013-01 | 2016-01 |
| --- | --- | ---: | ---: |
| TimeControl | `-` on exactly the correspondence games | 266 | 20,924 |
| WhiteElo / BlackElo | `?` (no rating diff on any of these games) | 78 / 140 | 11 / 26 |
| Elo numeric range | min / max | 782 / 2,403 | 739 / 2,998 |
| UTCDate | outside the file's month; all on the previous month's last day | 135 | 5,124 |
| Result | only `1-0`, `0-1`, `1/2-1/2` | ✓ | ✓ |
| ECO | all match `[A-E]\d{2}` | ✓ | ✓ |
| Termination | Normal, Time forfeit, Abandoned, Rules infraction | 2 values | 4 values |
| `%clk` / `%eval` | games with annotations | 0 / 518 | 0 / 702,897 |

Applying DQ-05 literally (quarantine `-` time controls, `?` Elo, out-of-month dates) would
quarantine 0.2–0.5% of every month and fail DQ-08 (≥ 99.99%) by design.

## Decisions
- **Correspondence** (`TimeControl = "-"`) is a speed; `base_s`/`increment_s` are NULL.
- **Speed** is derived with Lichess's rule: base + 40 × increment ≤ 29 ultrabullet,
  ≤ 179 bullet, ≤ 479 blitz, ≤ 1499 rapid, else classical. Consistent across years
  (2013–2016 `Event` names say "Classical" for what is rapid today); `event` is kept raw.
- **Elo `?`** → NULL (unknown rating). Non-numeric other values → `elo_not_numeric`;
  numeric outside 400–3500 → `elo_out_of_range`.
- **played_at** = UTCDate + UTCTime (game start, UTC). Valid window: from the first day of
  the previous month to the end of the file's month (a file holds games that *ended* in it).
  Outside → `played_at_out_of_window`; unparseable → `played_at_unparseable`.
- Other rules: `missing_player`, `invalid_result`, `invalid_eco`, `time_control_unparseable`.
  The first failing rule is recorded.
- **ANSI mode** (Spark 4 default): all conversions use `try_cast` / `try_to_timestamp`.
- Silver does not store movetext; annotation jobs read it from bronze by `game_id`.
- Pandera schema contract on the silver output and DQ-07 header drift: Phase 2.2.
