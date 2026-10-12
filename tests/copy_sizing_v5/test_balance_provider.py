"""Contracts for the cached pUSD balance provider."""

from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.services.copy_sizing_v5.balance_provider import CachedPusdBalanceProvider


@pytest.mark.asyncio
async def test_balance_provider_caches_follower_and_each_leader_independently() -> None:
    calls = {"follower": 0, "leader": 0}

    async def follower() -> Decimal:
        calls["follower"] += 1
        return Decimal("500")

    async def leader(wallet: str) -> Decimal:
        calls["leader"] += 1
        return Decimal("10000") if wallet.endswith("1") else Decimal("20000")

    provider = CachedPusdBalanceProvider(
        follower_balance=follower,
        leader_balance=leader,
        follower_ttl_seconds=60,
        leader_ttl_seconds=60,
    )
    assert await provider.follower_pusd_balance() == Decimal("500")
    assert await provider.follower_pusd_balance() == Decimal("500")
    assert await provider.leader_pusd_balance("0x1") == Decimal("10000")
    assert await provider.leader_pusd_balance("0x1") == Decimal("10000")
    assert await provider.leader_pusd_balance("0x2") == Decimal("20000")
    assert calls == {"follower": 1, "leader": 2}


@pytest.mark.asyncio
async def test_balance_provider_rejects_non_positive_or_non_finite_payloads() -> None:
    async def follower() -> str:
        return "nan"

    async def leader(_wallet: str) -> Decimal:
        return Decimal("100")

    provider = CachedPusdBalanceProvider(
        follower_balance=follower,
        leader_balance=leader,
    )
    with pytest.raises(ValueError, match="follower pUSD balance"):
        await provider.follower_pusd_balance()
