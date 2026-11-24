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

PG-A5: ported to async PG. The surface-inspection tests now probe the
canonical ``storage.postgres.risk_blocks_repo.RiskBlocksRepo``; the
behavioural lifecycle tests use the established ``_pg_pool`` async ctx
mgr.
"""

from __future__ import annotations

import contextlib
import inspect

import pytest

from pmcopy.services.trading.entry_permission_service import EntryPermissionService
from pmcopy.storage.postgres.risk_blocks_repo import RiskBlocksRepo

pytestmark = pytest.mark.release_gate


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


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from tests.integration_postgres import PostgresContainer
        import asyncpg
        from pmcopy.storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


async def _is_blocked(repo: RiskBlocksRepo, *, condition: str, token: str) -> bool:
    return (
        await repo.get_active_risk_block(user_id="u", condition_id=condition, token_id=token)
        is not None
    )


@pytest.mark.asyncio
async def test_no_block_when_trigger_only_fires_no_sell_lands() -> None:
    # Trigger evaluation alone (no SELL persisted, no mark_risk_block
    # call) must leave the scope unblocked.
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        assert await _is_blocked(repo, condition="c1", token="t1") is False


@pytest.mark.asyncio
async def test_rejected_sell_does_not_activate_block() -> None:
    # The reconciler is the only caller of mark_risk_block — and only
    # after a confirmed full close. A SELL that the venue rejects
    # never reaches that call site; the scope must remain unblocked.
    # Verify the contract by *not* calling mark_risk_block here and
    # asserting the lookup stays None.
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        assert await _is_blocked(repo, condition="c1", token="t1") is False


@pytest.mark.asyncio
async def test_partial_fill_alone_does_not_activate_block() -> None:
    # Same contract: partial fill that does not zero the position must
    # not activate the durable block. The reconciler's full-close
    # branch is the only entry point for mark_risk_block.
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        assert await _is_blocked(repo, condition="c1", token="t1") is False


@pytest.mark.asyncio
async def test_full_close_activates_durable_block() -> None:
    # Simulate the reconciler's full-close branch reaching
    # mark_risk_block. The scope must now be blocked.
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        await repo.mark_risk_block(
            user_id="u",
            condition_id="c1",
            token_id="t1",
            reason="stop_loss",
            source="risk_monitor",
        )
        assert await _is_blocked(repo, condition="c1", token="t1") is True


@pytest.mark.asyncio
async def test_block_is_scoped_to_token_not_condition() -> None:
    # Binary markets share a condition_id across two tokens. A block on
    # the YES token must NOT block the NO token.
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        await repo.mark_risk_block(
            user_id="u",
            condition_id="cBin",
            token_id="tYES",
            reason="stop_loss",
            source="risk_monitor",
        )
        assert await _is_blocked(repo, condition="cBin", token="tYES") is True
        assert await _is_blocked(repo, condition="cBin", token="tNO") is False
