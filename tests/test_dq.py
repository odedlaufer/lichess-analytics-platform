from ops.dq import dq_row, header_drift


def test_first_month_is_baseline() -> None:
    d = header_drift(None, {"Event", "Site"})
    assert d["status"] == "baseline"
    assert d["keys"] == ["Event", "Site"]


def test_same_keys_pass() -> None:
    assert header_drift({"Event", "Site"}, {"Site", "Event"})["status"] == "pass"


def test_added_and_removed_keys_warn() -> None:
    d = header_drift({"Event", "Site", "WhiteTitle"}, {"Event", "Site", "WhiteFideId"})
    assert d["status"] == "warn"
    assert d["added"] == ["WhiteFideId"]
    assert d["removed"] == ["WhiteTitle"]


def test_dq_row_serialises_details() -> None:
    row = dq_row("r1", "2016-01", "bronze", "DQ-07", "pass", {"keys": ["a"]})
    assert row["details"] == '{"keys": ["a"]}'
    assert row["check_id"] == "DQ-07"
