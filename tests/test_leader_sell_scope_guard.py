"""Durable fail-closed guard for degraded leader SELL-fraction scopes."""

from __future__ import annotations

import pytest

from services.onchain_detection.leader_sell_scope_guard import LeaderSellScopeGuard


class _ScopeStore:
    """Record scope mutations with optional deterministic failures."""

    def __init__(self) -> None:
        self.paused: set[tuple[str, str]] = set()
        self.fail_read = False
        self.fail_write = False

    async def set_paused(self, leader_wallet: str, token_id: str, reason: str) -> None:
        """Persist a normalized pause unless configured to fail."""

        if self.fail_write:
            raise RuntimeError("write unavailable")
        self.paused.add((leader_wallet, token_id))

    async def clear_paused(self, leader_wallet: str, token_id: str) -> None:
        """Delete a normalized pause unless configured to fail."""

        if self.fail_write:
            raise RuntimeError("write unavailable")
        self.paused.discard((leader_wallet, token_id))

    async def is_paused(self, leader_wallet: str, token_id: str) -> bool:
        """Read a normalized pause unless configured to fail."""

        if self.fail_read:
            raise RuntimeError("read unavailable")
        return (leader_wallet, token_id) in self.paused


@pytest.mark.asyncio
async def test_pause_is_shared_by_new_guard_instances_and_can_be_cleared() -> None:
    """Durable state survives service reconstruction and clears explicitly."""

    store = _ScopeStore()
    first = LeaderSellScopeGuard(store=store)
    await first.pause_buys(
        "0xABC",
        "123",
        reason="authoritative_sources_unavailable",
    )
    restarted = LeaderSellScopeGuard(store=store)

    assert await restarted.is_buy_paused("0xabc", "123") is True
    assert await restarted.clear_buys_pause("0xABC", "123") is True
    assert await first.is_buy_paused("0xabc", "123") is False


@pytest.mark.asyncio
async def test_store_failures_keep_buy_scope_paused_fail_closed() -> None:
    """A failed pause write/read/clear never re-enables a BUY."""

    store = _ScopeStore()
    guard = LeaderSellScopeGuard(store=store)
    store.fail_write = True

    await guard.pause_buys("0xABC", "123", reason="provider outage")

    assert await guard.is_buy_paused("0xabc", "123") is True
    assert await guard.clear_buys_pause("0xabc", "123") is False
    assert await guard.is_buy_paused("0xabc", "123") is True

    fresh = LeaderSellScopeGuard(store=store)
    store.fail_read = True
    assert await fresh.is_buy_paused("0xabc", "123") is True
