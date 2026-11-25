# ADR-009: Pandera contract on silver, DQ-07 header drift

- Status: Accepted
- Date: 2026-11-25
- Answers PRD Q4 in part (performance at full scale is measured in Phase 5)

## Evidence (probe in the Spark image)
| Question | Result |
| --- | --- |
| Pandera 0.35.0 with PySpark 4.1.1? | Works; its PySpark backend needs pandas ≥ 2.2 and PyArrow on the driver |
| Catches value violations? | Yes: `isin`, `in_range` violations reported per column and check |
| Unknown columns? | Yes with `strict=True` (`COLUMN_NOT_IN_SCHEMA`) |
| Row-level routing? | No: it reports failures, it does not split rows |

## Decisions
- **Two lines of defence.** Per-row rules stay in Spark SQL (they route rows to quarantine
  with a rule name, ADR-008). Pandera validates the month *as written* against a strict
  contract (types, nullability, allowed values, ranges, patterns); any violation fails the job.
- **Spark image Python env**: `/opt/pyenv` virtualenv used by driver and executors;
  `spark/requirements.in` locked with hashes into `spark/requirements.txt`
  (`make spark-lock`, only releases older than the cutoff date); installed with
  `pip --require-hashes`.
- **DQ-07**: each month's header key set is stored in `ops.dq_results` and compared with
  the latest earlier month. First month = `baseline`; any added/removed key = `warn`
  (logged, not failing). The contract outcome is stored there too (`silver_contract`).
