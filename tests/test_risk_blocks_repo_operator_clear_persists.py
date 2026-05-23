"""Regression: mark_risk_block UPSERT preserves operator-initiated clears.

Pre-fix the UPSERT's DO UPDATE clause unconditionally reset
``cleared_at = NULL`` and ``clear_reason = NULL``. If an operator
explicitly cleared a block (e.g. after manual reconciliation) and the
position later re-triggered the risk path, the UPSERT silently
reactivated what the operator had cleared — with no audit trail
showing the override was reversed.

Fix: the UPSERT preserves cleared_at when the existing clear_reason
starts with the documented operator marker ``operator:``. System
triggers (``risk_monitor:*``, ``reconciler:*``, …) still reset for
the normal re-trigger semantics.
"""
from __future__ import annotations

import pytest

from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations
from storage.sqlite.risk_blocks_repo import (
    OPERATOR_CLEAR_PREFIX,
    RiskBlocksRepo,
)


@pytest.fixture()
def repo(tmp_path):
    db_path = str(tmp_path / "blocks.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()
    return RiskBlocksRepo(db_path)


def test_operator_clear_survives_re_trigger(repo):
    """Operator clear must NOT be reset by a subsequent re-trigger UPSERT."""
    repo.mark_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        reason="stop_loss",
        source="risk_monitor",
    )
    # Operator clears the block via the documented prefix.
    cleared = repo.clear_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        clear_reason=f"{OPERATOR_CLEAR_PREFIX}manual_unlock",
    )
    assert cleared is True
    assert repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t") is None

    # Position re-triggers the risk path — the UPSERT must respect the
    # operator's override.
    repo.mark_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        reason="stop_loss",
        source="risk_monitor",
    )
    assert repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t") is None, (
        "operator clear must survive re-trigger UPSERT"
    )


def test_system_clear_does_get_reset_on_re_trigger(repo):
    """System clears (no operator prefix) keep the existing re-trigger semantics."""
    repo.mark_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        reason="stop_loss",
        source="risk_monitor",
    )
    # System clear with a non-operator reason.
    repo.clear_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        clear_reason="reconciler:full_close_confirmed",
    )
    assert repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t") is None

    repo.mark_risk_block(
        user_id="u",
        condition_id="c",
        token_id="t",
        reason="stop_loss",
        source="risk_monitor",
    )
    assert repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t") is not None, (
        "system-cleared block must be re-armed by a fresh trigger"
    )
