"""Tests for Phase 10: discovery → live monitoring wiring."""
from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

# ---------------------------------------------------------------------------
# _discovery_refresh_loop
# ---------------------------------------------------------------------------

# We need to import the function under test.
# It lives at module-level in app.py which has heavy side-effects at import
# time.  To avoid that, we import it carefully.  If that proves fragile we
# test indirectly via the callback contract.

def _make_leaders_repo(tiers: dict[str, list[str]]):
    """Return a mock leaders_repo whose get_leaders_by_tier returns from *tiers*."""
    repo = MagicMock()
    repo.get_leaders_by_tier = MagicMock(side_effect=lambda t: list(tiers.get(t, [])))
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

    async def _run():
        task = asyncio.create_task(
            _discovery_refresh_loop(
                registry=registry,
                configured_leaders=["0xAAA"],
                refresh_interval=0,  # instant
                leaders_repo=repo,
                on_new_leaders=callback,
                already_monitored=set(),
            )
        )
        # Let two iterations run
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    await _run()
    # Callback should have been called exactly once with ["0xAAA"]
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

    # Should only be called once (first detection), not on subsequent iterations
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
            already_monitored={"0xCCC"},  # already being monitored
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

    # Should have survived failures and eventually called callback
    assert call_count >= 3
    assert callback.call_count >= 1


# ---------------------------------------------------------------------------
# get_leaders_by_tier (sqlite)
# ---------------------------------------------------------------------------
def test_get_leaders_by_tier():
    """Verify get_leaders_by_tier returns correct wallets for a given tier."""
    import tempfile
    import os
    from storage.sqlite.leaders_repo import LeadersRepo
    from storage.sqlite.migrations import apply_migrations as run_migrations
    from storage.sqlite.connection import create_connection

    db_path = os.path.join(tempfile.mkdtemp(), "test.db")
    with create_connection(db_path) as conn:
        run_migrations(conn)
        # Insert some test leaders with different tiers
        for wallet, tier in [("0x111", "live_follow"), ("0x222", "paper_follow"),
                             ("0x333", "watchlist"), ("0x444", "live_follow")]:
            conn.execute(
                "INSERT INTO discovered_leaders (wallet, active, follow_tier, updated_at) "
                "VALUES (?, 1, ?, 1000.0)",
                (wallet, tier),
            )
        conn.commit()

    repo = LeadersRepo(db_path)
    live = repo.get_leaders_by_tier("live_follow")
    paper = repo.get_leaders_by_tier("paper_follow")
    watchlist = repo.get_leaders_by_tier("watchlist")
    none = repo.get_leaders_by_tier("nonexistent")

    assert sorted(live) == ["0x111", "0x444"]
    assert paper == ["0x222"]
    assert watchlist == ["0x333"]
    assert none == []

    os.unlink(db_path)
