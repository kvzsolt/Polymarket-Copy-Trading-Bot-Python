"""Contracts for the V5 runtime sizing composition."""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.copy_sizing_v5.addon import CopySizingV5Addon
from services.copy_sizing_v5.balance_provider import CachedPusdBalanceProvider
from services.copy_sizing_v5.domain import CopySizingRequest
from services.copy_sizing_v5.engine import CopySizingEngine
from services.copy_sizing_v5.policy import CopySizingPolicy
from services.copy_sizing_v5.runtime import CopySizingV5Runtime


@pytest.mark.asyncio
async def test_runtime_hydrates_wallet_balances_before_sizing() -> None:
    async def follower() -> Decimal:
        return Decimal("500")

    async def leader(_wallet: str) -> Decimal:
        return Decimal("10000")

    policy = CopySizingPolicy(
        mode="proportional",
        percentage=Decimal("5"),
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("0.01"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
    )
    engine = CopySizingEngine(policy)
    runtime = CopySizingV5Runtime(
        policy=policy,
        engine=engine,
        addon=CopySizingV5Addon(engine),
        balances=CachedPusdBalanceProvider(
            follower_balance=follower,
            leader_balance=leader,
        ),
    )
    decision = await runtime.size_buy(
        CopySizingRequest(
            leader_wallet="0x" + "a" * 40,
            leader_trade_notional=Decimal("200"),
            follower_pusd_balance=Decimal("1"),  # replaced by provider authority
        )
    )
    assert decision.final_notional == Decimal("7.50")


@pytest.mark.asyncio
async def test_runtime_balance_failure_returns_fail_closed_decision() -> None:
    async def follower() -> Decimal:
        return Decimal("0")

    async def leader(_wallet: str) -> Decimal:
        return Decimal("10000")

    policy = CopySizingPolicy(
        mode="proportional",
        percentage=Decimal("5"),
        copyable_balance_percent=Decimal("75"),
        buy_min_amount=Decimal("0.01"),
        buy_max_amount=Decimal("0"),
        max_position_pct=Decimal("1"),
    )
    engine = CopySizingEngine(policy)
    runtime = CopySizingV5Runtime(
        policy=policy,
        engine=engine,
        addon=CopySizingV5Addon(engine),
        balances=CachedPusdBalanceProvider(
            follower_balance=follower,
            leader_balance=leader,
        ),
    )

    decision = await runtime.size_buy(
        CopySizingRequest(
            leader_wallet="0x" + "c" * 40,
            leader_trade_notional=Decimal("200"),
            follower_pusd_balance=Decimal("1"),
        )
    )

    assert decision.allowed is False
    assert decision.reason == "balance_unavailable:ValueError"
    assert decision.final_notional == Decimal("0")