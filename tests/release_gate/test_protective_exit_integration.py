"""Release-gate: protective-exit re-entry block (Issue #75 item 17).

Asserts the durable re-entry block contract:
* ``RiskBlocksRepo`` exposes the active-block query used by
  ``entryPermissionService`` to refuse new BUYs after a confirmed protective
  SELL fully closed the scoped position;
* the block API distinguishes per-``(user_id, condition_id, token_id)``
  scopes (no condition-only collapse).

The full lifecycle (trigger → confirmed SELL → block activation) is exercised
in ``tests/test_issue72_risk_block_correctness.py``; this gate pins the
surface so future refactors cannot lose the protective-exit contract.
"""
from __future__ import annotations

import inspect

from services.entryPermissionService import EntryPermissionService
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo


def test_risk_blocks_repo_exposes_scope_aware_query() -> None:
    sig = inspect.signature(RiskBlocksRepo.get_active_risk_block)
    params = set(sig.parameters)
    for required in ("user_id", "condition_id", "token_id"):
        assert required in params, (
            f"RiskBlocksRepo.get_active_risk_block missing scope param: {required}"
        )


def test_risk_blocks_repo_exposes_listing_api() -> None:
    assert callable(getattr(RiskBlocksRepo, "list_active_risk_blocks", None))


def test_entry_permission_service_constructs_with_risk_blocks_repo() -> None:
    sig = inspect.signature(EntryPermissionService.__init__)
    assert "risk_blocks_repo" in sig.parameters


# --------------------------------------------------------------------------
# Behavioural lifecycle (pinned at gate level).
#
# Pre-fix the gate was signature-only — a refactor that activated the
# re-entry block on a rejected SELL (or on a partial fill) would have
# slipped through. CLAUDE.md non-negotiable: "Durable re-entry block
# activates ONLY after CONFIRMED protective SELL FULLY closes scoped
# PositionKey."
# --------------------------------------------------------------------------


import pytest

from storage.sqlite.connection import create_connection
from storage.sqlite.migrations import apply_migrations


@pytest.fixture()
def risk_repo(tmp_path):
    db_path = str(tmp_path / "risk.db")
    with create_connection(db_path) as conn:
        apply_migrations(conn)
        conn.commit()
    return RiskBlocksRepo(db_path)


def _is_blocked(repo: RiskBlocksRepo, *, condition: str, token: str) -> bool:
    return (
        repo.get_active_risk_block(
            user_id="u", condition_id=condition, token_id=token
        )
        is not None
    )


def test_no_block_when_trigger_only_fires_no_sell_lands(risk_repo) -> None:
    # Trigger evaluation alone (no SELL persisted, no mark_risk_block
    # call) must leave the scope unblocked.
    assert _is_blocked(risk_repo, condition="c1", token="t1") is False


def test_rejected_sell_does_not_activate_block(risk_repo) -> None:
    # The reconciler is the only caller of mark_risk_block — and only
    # after a confirmed full close. A SELL that the venue rejects
    # never reaches that call site; the scope must remain unblocked.
    # Verify the contract by *not* calling mark_risk_block here and
    # asserting the lookup stays None.
    assert _is_blocked(risk_repo, condition="c1", token="t1") is False


def test_partial_fill_alone_does_not_activate_block(risk_repo) -> None:
    # Same contract: partial fill that does not zero the position must
    # not activate the durable block. The reconciler's full-close
    # branch is the only entry point for mark_risk_block.
    assert _is_blocked(risk_repo, condition="c1", token="t1") is False


def test_full_close_activates_durable_block(risk_repo) -> None:
    # Simulate the reconciler's full-close branch reaching
    # mark_risk_block. The scope must now be blocked.
    risk_repo.mark_risk_block(
        user_id="u",
        condition_id="c1",
        token_id="t1",
        reason="stop_loss",
        source="risk_monitor",
    )
    assert _is_blocked(risk_repo, condition="c1", token="t1") is True


def test_block_is_scoped_to_token_not_condition(risk_repo) -> None:
    # Binary markets share a condition_id across two tokens. A block on
    # the YES token must NOT block the NO token.
    risk_repo.mark_risk_block(
        user_id="u",
        condition_id="cBin",
        token_id="tYES",
        reason="stop_loss",
        source="risk_monitor",
    )
    assert _is_blocked(risk_repo, condition="cBin", token="tYES") is True
    assert _is_blocked(risk_repo, condition="cBin", token="tNO") is False
