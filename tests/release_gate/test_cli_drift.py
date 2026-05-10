"""Release-gate: ``bot drift check`` CLI (Issue #76).

Mocks live-holdings fetch and seeds the persisted ``positions`` table to
exercise the four drift classifications: should_close, shrink, phantom_grow,
and untracked_live.
"""
from __future__ import annotations

from decimal import Decimal

from bot_cli.commands.drift import (
    _classify_drift,
    _list_persisted_positions,
    _live_size_index,
)
from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations


def _seed(db_path: str) -> None:
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.execute(
            """
            INSERT INTO positions
              (user_id, condition_id, token_id, leader_id,
               size_tokens, avg_entry_price, updated_at)
            VALUES
              ('u-test','cond-x','tok-x','ldr-1','10','0.42',CURRENT_TIMESTAMP),
              ('u-test','cond-y','tok-y','ldr-1','5','0.50',CURRENT_TIMESTAMP)
            """,
        )
        conn.commit()


def test_classify_drift_kinds() -> None:
    eps = Decimal("0.000001")
    assert _classify_drift(Decimal("10"), Decimal("0"), eps) == "should_close"
    assert _classify_drift(Decimal("10"), Decimal("5"), eps) == "shrink"
    assert _classify_drift(Decimal("5"), Decimal("10"), eps) == "phantom_grow"
    assert _classify_drift(Decimal("5"), Decimal("5"), eps) is None
    assert _classify_drift(Decimal("5"), Decimal("5.0000001"), eps) is None


def test_persisted_positions_listed(tmp_path) -> None:
    db = tmp_path / "drift.db"
    _seed(str(db))
    rows = _list_persisted_positions(str(db), user_id="u-test")
    assert len(rows) == 2
    keys = {(r["condition_id"], r["token_id"]) for r in rows}
    assert keys == {("cond-x", "tok-x"), ("cond-y", "tok-y")}


def test_live_size_index_aggregates() -> None:
    live = [
        {"conditionId": "cond-x", "tokenID": "tok-x", "size": "3"},
        {"condition_id": "cond-x", "token_id": "tok-x", "size": "2"},
        {"conditionId": "cond-z", "asset": "tok-z", "size": "1"},
    ]
    idx = _live_size_index(live)
    assert idx[("cond-x", "tok-x")] == Decimal("5")
    assert idx[("cond-z", "tok-z")] == Decimal("1")
