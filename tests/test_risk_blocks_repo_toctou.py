"""Regression: RiskBlocksRepo.clear_risk_block must pin the row by triggered_at.

Pre-fix the clear UPDATE only filtered by (user, condition, token) and
``cleared_at IS NULL``. The TOCTOU window:
  1. Caller reads block A via get_active_risk_block (captures triggered_at=t1).
  2. Concurrent fill triggers a fresh block via mark_risk_block — UPSERT
     resets cleared_at=NULL, writes triggered_at=t2.
  3. Caller's clear_risk_block fires, sees ``cleared_at IS NULL`` for
     the NEW block, and clears it — exposing a position that should
     have stayed blocked.

The fix accepts an optional ``triggered_at`` arg and adds
``AND triggered_at = ?`` so the UPDATE only clears the exact row the
caller meant to clear. The expires_at filter also lands here so a
stale clear cannot resurrect a window-expired block.
"""
from __future__ import annotations

import pytest

from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo


@pytest.fixture()
def repo(tmp_path):
    db_path = str(tmp_path / "blocks.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()
    return RiskBlocksRepo(db_path)


def test_clear_with_wrong_triggered_at_is_noop(repo):
    """Stale clear (caller saw block t1, current row is t2) must NOT clear."""
    repo.mark_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        reason="stop_loss",
        source="risk_monitor",
    )
    active = repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t")
    assert active is not None
    t1 = active["triggered_at"]

    # Simulate a concurrent re-trigger landing between read + clear.
    repo.mark_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        reason="stop_loss_reentry",
        source="risk_monitor",
    )

    # Caller's stale clear referencing the OLD triggered_at must not affect
    # the NEW block.
    cleared = repo.clear_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        clear_reason="test_stale",
        triggered_at=t1,
    )
    assert cleared is False, "stale-pinned clear must be a no-op"

    still_active = repo.get_active_risk_block(
        user_id="u", condition_id="c", token_id="t"
    )
    assert still_active is not None, (
        "active re-trigger block must survive stale-pinned clear"
    )


def test_clear_with_matching_triggered_at_succeeds(repo):
    repo.mark_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        reason="stop_loss",
        source="risk_monitor",
    )
    active = repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t")
    assert active is not None

    cleared = repo.clear_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        clear_reason="ok",
        triggered_at=active["triggered_at"],
    )
    assert cleared is True


def test_clear_without_triggered_at_keeps_backward_compat(repo):
    """Callers that don't pass triggered_at (legacy paths) keep old behaviour."""
    repo.mark_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        reason="stop_loss",
        source="risk_monitor",
    )
    cleared = repo.clear_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        clear_reason="ok",
    )
    assert cleared is True
