"""Discovery refresh is DB-only and must not start monitors."""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock

import pytest


def _make_leaders_repo(tiers: dict[str, list[str]]):
    repo = MagicMock()
    repo.get_leaders_by_tier = AsyncMock(side_effect=lambda t: list(tiers.get(t, [])))
    return repo


class _FakeRegistry:
    def __init__(self):
        self.refresh_count = 0

    async def refresh(self, _leaders):
        self.refresh_count += 1


@pytest.mark.asyncio
async def test_discovery_loop_does_not_callback_for_tier_leaders():
    from app import _discovery_refresh_loop

    registry = _FakeRegistry()
    repo = _make_leaders_repo({"live_follow": ["0xAAA"], "paper_follow": ["0xBBB"]})
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xAAA"],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert registry.refresh_count > 0
    assert repo.get_leaders_by_tier.call_count == 0
    assert callback.call_count == 0


@pytest.mark.asyncio
async def test_discovery_loop_refresh_exception_resilience():
    from app import _discovery_refresh_loop

    registry = MagicMock()
    call_count = 0

    async def _failing_refresh(_leaders):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError("API down")

    registry.refresh = _failing_refresh
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xFFF"],
            refresh_interval=0,
            leaders_repo=_make_leaders_repo({"live_follow": ["0xFFF"]}),
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.08)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert call_count >= 3
    assert callback.call_count == 0


@contextlib.asynccontextmanager
async def _pg_pool():
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    ctr = PostgresContainer("postgres:16-alpine")
    try:
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


@pytest.mark.asyncio
async def test_get_leaders_by_tier():
    from storage.postgres.leaders_repo import LeadersRepo

    async with _pg_pool() as pool:
        async with pool.acquire() as conn:
            for wallet, tier in [
                ("0x111", "live_follow"),
                ("0x222", "paper_follow"),
                ("0x333", "watchlist"),
                ("0x444", "live_follow"),
            ]:
                await conn.execute(
                    "INSERT INTO discovered_leaders "
                    "(wallet, active, follow_tier, updated_at) "
                    "VALUES ($1, TRUE, $2, 1000.0)",
                    wallet,
                    tier,
                )

        repo = LeadersRepo(pool)
        live = await repo.get_leaders_by_tier("live_follow")
        paper = await repo.get_leaders_by_tier("paper_follow")
        watchlist = await repo.get_leaders_by_tier("watchlist")
        none = await repo.get_leaders_by_tier("nonexistent")

        assert sorted(live) == ["0x111", "0x444"]
        assert paper == ["0x222"]
        assert watchlist == ["0x333"]
        assert none == []

