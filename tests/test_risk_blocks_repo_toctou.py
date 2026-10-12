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
``AND triggered_at = $N`` so the UPDATE only clears the exact row the
caller meant to clear. The expires_at filter also lands here so a
stale clear cannot resurrect a window-expired block.
"""

from __future__ import annotations

import contextlib

import pytest

from pmcopy.storage.postgres.risk_blocks_repo import RiskBlocksRepo

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


async def test_clear_with_wrong_triggered_at_is_noop():
    """Stale clear (caller saw block t1, current row is t2) must NOT clear."""
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        await repo.mark_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            reason="stop_loss",
            source="risk_monitor",
        )
        active = await repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t")
        assert active is not None
        t1 = active["triggered_at"]

        # Simulate a concurrent re-trigger landing between read + clear.
        await repo.mark_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            reason="stop_loss_reentry",
            source="risk_monitor",
        )

        # Caller's stale clear referencing the OLD triggered_at must not
        # affect the NEW block.
        cleared = await repo.clear_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            clear_reason="test_stale",
            triggered_at=t1,
        )
        assert cleared is False, "stale-pinned clear must be a no-op"

        still_active = await repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t")
        assert still_active is not None, "active re-trigger block must survive stale-pinned clear"


async def test_clear_with_matching_triggered_at_succeeds():
    async with _pg_pool() as pool:
        repo = RiskBlocksRepo(pool)
        await repo.mark_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            reason="stop_loss",
            source="risk_monitor",
        )
        active = await repo.get_active_risk_block(user_id="u", condition_id="c", token_id="t")
        assert active is not None

        cleared = await repo.clear_risk_block(
            user_id="u",
            condition_id="c",
            token_id="t",
            clear_reason="ok",
            triggered_at=active["triggered_at"],
        )
        assert cleared is True


async def test_clear_without_triggered_at_keeps_backward_compat():
    """Callers that don't pass triggered_at (legacy paths) keep old behaviour."""
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
            clear_reason="ok",
        )
        assert cleared is True
