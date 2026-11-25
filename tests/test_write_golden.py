from tools.write_golden import anonymize, pseudonym


def test_pseudonym_is_deterministic_and_keeps_script() -> None:
    assert pseudonym("alice") == pseudonym("alice")
    assert pseudonym("alice").startswith("player-")
    assert not pseudonym("Ñandú").isascii()


def test_anonymize_replaces_names_in_pgn_and_silver_consistently() -> None:
    item = {
        "pgn": '[Event "x"]\n[White "alice"]\n[Black "Ñandú"]\n[WhiteElo "1500"]\n\n1. e4 1-0\n\n',
        "silver": {"white": "alice", "black": "Ñandú", "white_elo": 1500},
    }
    out = anonymize(item)
    assert "alice" not in out["pgn"]
    assert "Ñandú" not in out["pgn"]
    assert f'[White "{out["silver"]["white"]}"]' in out["pgn"]
    assert f'[Black "{out["silver"]["black"]}"]' in out["pgn"]
    assert '[WhiteElo "1500"]' in out["pgn"]
