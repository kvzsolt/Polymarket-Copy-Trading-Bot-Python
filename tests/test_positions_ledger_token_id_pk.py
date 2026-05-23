"""Regression: positions_ledger must key on (user_id, condition_id, token_id).

Pre-fix bug: PRIMARY KEY (user_id, condition_id) silently overwrote the
YES outcome row when a NO row for the same condition was upserted (and
vice-versa). Loss of cost basis + size on binary markets.

This test pins the new 3-column PK by inserting both outcomes for one
condition and verifying both rows persist after upsert.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations


@pytest.fixture()
def fresh_db(tmp_path):
    db_path = str(tmp_path / "ledger_pk.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()
    return db_path


def test_positions_ledger_pk_includes_token_id(fresh_db):
    """Inserting YES then NO for same condition must produce two distinct rows."""
    with create_connection(fresh_db) as conn:
        cur = conn.execute("PRAGMA table_info(positions_ledger)")
        rows = cur.fetchall()
        pk_cols = sorted(
            row["name"] if hasattr(row, "keys") else row[1]
            for row in rows
            if (row["pk"] if hasattr(row, "keys") else row[5]) > 0
        )
        assert pk_cols == ["condition_id", "token_id", "user_id"], (
            f"positions_ledger PK must be (user_id, condition_id, token_id); got {pk_cols}"
        )


def test_yes_and_no_outcomes_persist_independently(fresh_db, monkeypatch):
    """Both YES (token_a) and NO (token_b) rows survive concurrent upserts."""
    import services.accountingService as acct_mod
    from services.accountingService import AccountingService, PositionLot

    monkeypatch.setattr(acct_mod, "get_database", lambda: create_connection(fresh_db))

    svc = AccountingService()
    yes_lot = PositionLot(
        user_id="u1",
        condition_id="cond_x",
        token_id="token_yes",
        size_tokens=Decimal("10"),
        avg_entry_price=Decimal("0.30"),
        leader_id="leader1",
        category="copy",
    )
    no_lot = PositionLot(
        user_id="u1",
        condition_id="cond_x",
        token_id="token_no",
        size_tokens=Decimal("5"),
        avg_entry_price=Decimal("0.70"),
        leader_id="leader1",
        category="copy",
    )
    svc.upsert_position(yes_lot)
    svc.upsert_position(no_lot)

    with create_connection(fresh_db) as conn:
        rows = conn.execute(
            "SELECT token_id, size_tokens, avg_entry_price FROM positions_ledger "
            "WHERE user_id = ? AND condition_id = ? ORDER BY token_id",
            ("u1", "cond_x"),
        ).fetchall()
    assert len(rows) == 2, f"expected 2 rows (YES + NO), got {len(rows)}: {rows}"
    token_ids = sorted(r[0] for r in rows)
    assert token_ids == ["token_no", "token_yes"]


def test_close_position_requires_token_id(fresh_db, monkeypatch):
    """close_position must accept token_id and only delete the matching token row."""
    import services.accountingService as acct_mod
    from services.accountingService import AccountingService, PositionLot

    monkeypatch.setattr(acct_mod, "get_database", lambda: create_connection(fresh_db))

    svc = AccountingService()
    for tok in ("token_yes", "token_no"):
        svc.upsert_position(
            PositionLot(
                user_id="u2",
                condition_id="cond_y",
                token_id=tok,
                size_tokens=Decimal("1"),
                avg_entry_price=Decimal("0.5"),
                leader_id="L",
                category="copy",
            )
        )
    # close only YES — NO must survive
    svc.close_position(user_id="u2", condition_id="cond_y", token_id="token_yes")
    with create_connection(fresh_db) as conn:
        rows = conn.execute(
            "SELECT token_id FROM positions_ledger WHERE user_id = ? AND condition_id = ?",
            ("u2", "cond_y"),
        ).fetchall()
    assert [r[0] for r in rows] == ["token_no"], f"only token_no should remain: {rows}"
