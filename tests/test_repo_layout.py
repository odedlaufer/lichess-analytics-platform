from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

EXPECTED_DIRS = [
    "ingest",
    "spark/jobs",
    "spark/schemas",
    "spark/conf",
    "live",
    "clickhouse",
    "sqlmesh",
    "flows",
    "superset",
    "benchmarks",
    "docs/adr",
]


def test_expected_directories_exist() -> None:
    missing = [d for d in EXPECTED_DIRS if not (ROOT / d).is_dir()]
    assert not missing, f"missing directories: {missing}"


def test_env_example_present_and_env_ignored() -> None:
    assert (ROOT / ".env.example").is_file()
    assert ".env" in (ROOT / ".gitignore").read_text().splitlines()
