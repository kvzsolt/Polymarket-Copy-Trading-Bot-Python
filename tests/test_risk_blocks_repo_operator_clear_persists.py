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

import contextlib

import pytest

from pmcopy.storage.postgres.risk_blocks_repo import (
    OPERATOR_CLEAR_PREFIX,
    RiskBlocksRepo,
)

pytestmark = pytest.mark.asyncio


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
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


async def test_operator_clear_survives_re_trigger():
    """Operator clear must NOT be reset by a subsequent re-trigger UPSERT."""
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        await repo.mark_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            reason="stop_loss",
            source="risk_monitor",
        )
        cleared = await repo.clear_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            clear_reason=f"{OPERATOR_CLEAR_PREFIX}manual_unlock",
        )
        assert cleared is True
        assert await repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t") is None

        await repo.mark_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            reason="stop_loss",
            source="risk_monitor",
        )
        assert (
            await repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t") is None
        ), "operator clear must survive re-trigger UPSERT"


async def test_system_clear_does_get_reset_on_re_trigger():
    """System clears (no operator prefix) keep re-trigger semantics."""
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        await repo.mark_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            reason="stop_loss",
            source="risk_monitor",
        )
        await repo.clear_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            clear_reason="reconciler:full_close_confirmed",
        )
        assert await repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t") is None

        await repo.mark_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            reason="stop_loss",
            source="risk_monitor",
        )
        assert (
            await repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t")
            is not None
        ), "system-cleared block must be re-armed by a fresh trigger"
