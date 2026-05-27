"""Tests for Phase 10: discovery → live monitoring wiring.

PG-A6 port:
* Mock leaders_repo.get_leaders_by_tier uses AsyncMock now that the
  underlying PG repo method is a coroutine.
* The schema-touching ``test_get_leaders_by_tier`` runs against a
  per-test asyncpg testcontainer pool.
"""
from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock

import pytest


# ---------------------------------------------------------------------------
# _discovery_refresh_loop
# ---------------------------------------------------------------------------

def _make_leaders_repo(tiers: dict[str, list[str]]):
    """Return a mock leaders_repo whose get_leaders_by_tier (async) returns from *tiers*."""
    repo = MagicMock()
    repo.get_leaders_by_tier = AsyncMock(side_effect=lambda t: list(tiers.get(t, [])))
    return repo


class _FakeRegistry:
    """Minimal stand-in for LeaderRegistry."""
    def __init__(self):
        self.refresh_count = 0

    async def refresh(self, _leaders):
        self.refresh_count += 1


@pytest.mark.asyncio
async def test_discovery_loop_detects_new_leaders():
    """When leaders_repo returns new tier-promoted leaders, callback fires."""
    from app import _discovery_refresh_loop

    registry = _FakeRegistry()
    tiers: dict[str, list[str]] = {"live_follow": ["0xAAA"], "paper_follow": []}
    repo = _make_leaders_repo(tiers)
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
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert callback.call_count >= 1
    first_call_wallets = callback.call_args_list[0][0][0]
    assert "0xAAA" in first_call_wallets


@pytest.mark.asyncio
async def test_discovery_loop_no_duplicate_callbacks():
    """Once a leader is tracked, it should not trigger callback again."""
    from app import _discovery_refresh_loop

    registry = _FakeRegistry()
    tiers = {"live_follow": ["0xBBB"], "paper_follow": []}
    repo = _make_leaders_repo(tiers)
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xBBB"],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.08)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert callback.call_count == 1


@pytest.mark.asyncio
async def test_discovery_loop_already_monitored_skipped():
    """Leaders in already_monitored should not trigger callback."""
    from app import _discovery_refresh_loop

    registry = _FakeRegistry()
    tiers = {"live_follow": ["0xCCC"], "paper_follow": []}
    repo = _make_leaders_repo(tiers)
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xCCC"],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored={"0xCCC"},
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert callback.call_count == 0


@pytest.mark.asyncio
async def test_discovery_loop_paper_follow_detected():
    """Paper-follow leaders should also trigger monitoring."""
    from app import _discovery_refresh_loop

    registry = _FakeRegistry()
    tiers = {"live_follow": [], "paper_follow": ["0xDDD"]}
    repo = _make_leaders_repo(tiers)
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xDDD"],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert callback.call_count >= 1
    assert "0xDDD" in callback.call_args_list[0][0][0]


@pytest.mark.asyncio
async def test_discovery_loop_no_callback_without_repo():
    """When leaders_repo is None, callback is never called."""
    from app import _discovery_refresh_loop

    registry = _FakeRegistry()
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xEEE"],
            refresh_interval=0,
            leaders_repo=None,
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert callback.call_count == 0


@pytest.mark.asyncio
async def test_discovery_loop_refresh_exception_resilience():
    """If registry.refresh raises, the loop continues without crashing."""
    from app import _discovery_refresh_loop

    registry = MagicMock()
    call_count = 0

    async def _failing_refresh(_leaders):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise RuntimeError("API down")

    registry.refresh = _failing_refresh
    tiers = {"live_follow": ["0xFFF"], "paper_follow": []}
    repo = _make_leaders_repo(tiers)
    callback = AsyncMock()

    task = asyncio.create_task(
        _discovery_refresh_loop(
            registry=registry,
            configured_leaders=["0xFFF"],
            refresh_interval=0,
            leaders_repo=repo,
            on_new_leaders=callback,
            already_monitored=set(),
        )
    )
    await asyncio.sleep(0.08)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert call_count >= 3
    assert callback.call_count >= 1


# ---------------------------------------------------------------------------
# get_leaders_by_tier (PG)
# ---------------------------------------------------------------------------

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
    """Verify get_leaders_by_tier returns correct wallets for a given tier."""
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
