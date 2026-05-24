"""Regression: FillsRepo.append_fill is idempotent on duplicate inserts.

Pre-fix: bare ``INSERT INTO fills`` raised ``sqlite3.IntegrityError``
on the second call with the same (user_id, order_id, condition_id,
side, fill_ts) tuple — the persist_execution_atomically path caught
it but direct ``FillsRepo`` callers (e.g. reconciliation_service)
did not, so a benign duplicate fill could crash the reconciliation
cycle.

Post-fix: ``INSERT OR IGNORE`` with rowcount check. First call
returns lastrowid; second call on the same fill returns 0.
"""
from __future__ import annotations

import time
from decimal import Decimal

import pytest

from storage.sqlite.connection import create_connection
from storage.sqlite.fills_repo import FillRecord, FillsRepo
from storage.sqlite.migrations import apply_migrations


@pytest.fixture()
def repo(tmp_path):
    db_path = str(tmp_path / "fills.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()
    return FillsRepo(db_path)


def _record() -> FillRecord:
    return FillRecord(
        user_id="u",
        leader_id="L",
        order_id="venue-1",
        condition_id="c",
        token_id="1" * 64,
        side="BUY",
        size_tokens=Decimal("5"),
        price=Decimal("0.42"),
        fee=Decimal("0.01"),
        fill_ts=int(time.time()),
    )


def test_first_insert_returns_lastrowid(repo):
    rid = repo.append_fill(_record())
    assert rid > 0


def test_duplicate_insert_returns_zero(repo):
    rec = _record()
    first = repo.append_fill(rec)
    second = repo.append_fill(rec)
    assert first > 0
    assert second == 0, (
        f"duplicate insert must return 0 (not raise, not insert new row); got {second}"
    )


def test_duplicate_insert_does_not_create_second_row(repo):
    rec = _record()
    repo.append_fill(rec)
    repo.append_fill(rec)
    rows = repo.list_fills_for_user("u")
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}"
