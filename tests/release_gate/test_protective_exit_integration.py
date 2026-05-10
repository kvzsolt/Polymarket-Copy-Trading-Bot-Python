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
