"""Regression: BalanceRegime daily cap holds under concurrent BUYs.

Pre-fix the trade_pipeline called ``regime.check_buy(...)`` (no lock)
to gate the BUY and then ``regime.record_buy(...)`` only AFTER the
venue ack came back. Two concurrent BUY coroutines could both observe
the same ``spent_today`` snapshot, both pass the cap check, both
submit, and only then both call ``record_buy`` — effectively doubling
the daily cap. The docstring on ``BalanceRegime.reserve`` warns
against this exact race.

This test pins the atomic ``reserve()`` API: two concurrent reserves
totalling more than the daily cap must NOT both succeed.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from services.balance_regime import BalanceRegime


@pytest.mark.asyncio
async def test_concurrent_reserves_respect_daily_cap():
    # Daily cap = $10. Two concurrent BUYs of $6 each — one must lose.
    regime = BalanceRegime(max_daily_spend_usdc=Decimal("10"))

    async def _try(order_id: str):
        return await regime.reserve(
            order_id=order_id,
            notional_usdc=Decimal("6"),
            free_cash_usdc=Decimal("1000"),
        )

    results = await asyncio.gather(_try("a"), _try("b"))
    allowed = [r for r in results if r.allow]

    assert len(allowed) == 1, (
        "exactly one of two concurrent $6 reserves must pass on a $10 cap; "
        f"got {len(allowed)} allowed: {results}"
    )
    # And the ledger reflects only the one that passed.
    assert regime.daily_spent_usdc == Decimal("6"), (
        f"ledger must show only the winning reserve; got {regime.daily_spent_usdc}"
    )


@pytest.mark.asyncio
async def test_release_returns_reservation_to_budget():
    regime = BalanceRegime(max_daily_spend_usdc=Decimal("10"))
    decision = await regime.reserve(
        order_id="rel-1",
        notional_usdc=Decimal("7"),
        free_cash_usdc=Decimal("1000"),
    )
    assert decision.allow
    assert regime.daily_spent_usdc == Decimal("7")

    await regime.release(order_id="rel-1")
    assert regime.daily_spent_usdc == Decimal("0"), (
        "release() must roll the reservation back in full; "
        f"ledger still shows {regime.daily_spent_usdc}"
    )


@pytest.mark.asyncio
async def test_commit_with_unfilled_returns_remainder():
    regime = BalanceRegime(max_daily_spend_usdc=Decimal("10"))
    await regime.reserve(
        order_id="part-1",
        notional_usdc=Decimal("7"),
        free_cash_usdc=Decimal("1000"),
    )
    assert regime.daily_spent_usdc == Decimal("7")

    # Only $3 actually filled.
    await regime.commit(order_id="part-1", actual_filled_usdc=Decimal("3"))
    assert regime.daily_spent_usdc == Decimal("3"), (
        "commit() with actual < reserved must return the unfilled remainder; "
        f"ledger shows {regime.daily_spent_usdc}"
    )
