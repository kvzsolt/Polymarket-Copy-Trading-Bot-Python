"""Sell-fraction annotation resilience.

The trade monitor reconstructs each leader SELL's fraction from a venue
positions snapshot. During a data-api outage that fetch fails; the annotation
step must retry briefly and, if still failing, stamp an explicit
``_leader_sell_fraction_unavailable`` sentinel so the pipeline's
``sell_fraction_missing_policy`` can fail closed instead of silently treating
the trade as a legacy unannotated feed (which used to mean: full close).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from services.tradeMonitor import _annotate_sell_fractions

_LEADER = "0x" + "d" * 40


def _sell_trade(**overrides):
    trade = {
        "side": "SELL",
        "conditionId": "cond1",
        "tokenID": "tok1",
        "size": 5.0,
        "timestamp": 1_000,
    }
    trade.update(overrides)
    return trade


def _position(size: float = 5.0):
    return {"conditionId": "cond1", "tokenID": "tok1", "size": size}


@pytest.mark.asyncio
async def test_annotation_retries_then_succeeds():
    trades = [_sell_trade()]
    fetch = AsyncMock(
        side_effect=[RuntimeError("503"), RuntimeError("503"), [_position(5.0)]]
    )
    with (
        patch("services.tradeMonitor._guarded_leader_fetch", fetch),
        patch("services.tradeMonitor.asyncio.sleep", new_callable=AsyncMock),
    ):
        await _annotate_sell_fractions(trades, _LEADER)

    assert fetch.await_count == 3
    # Leader held 5 after selling 5 -> pre-sell size 10 -> fraction 0.5.
    assert trades[0]["_leader_sell_fraction"] == pytest.approx(0.5)
    assert "_leader_sell_fraction_unavailable" not in trades[0]


@pytest.mark.asyncio
async def test_annotation_all_retries_fail_sets_sentinel():
    trades = [_sell_trade(), _sell_trade(tokenID="tok2"), {"side": "BUY"}]
    fetch = AsyncMock(side_effect=RuntimeError("503"))
    with (
        patch("services.tradeMonitor._guarded_leader_fetch", fetch),
        patch("services.tradeMonitor.asyncio.sleep", new_callable=AsyncMock),
    ):
        await _annotate_sell_fractions(trades, _LEADER)

    assert fetch.await_count == 3
    assert trades[0].get("_leader_sell_fraction_unavailable") is True
    assert trades[1].get("_leader_sell_fraction_unavailable") is True
    assert "_leader_sell_fraction" not in trades[0]
    # Non-SELL trades are untouched.
    assert "_leader_sell_fraction_unavailable" not in trades[2]


@pytest.mark.asyncio
async def test_annotation_non_list_payload_sets_sentinel():
    trades = [_sell_trade()]
    fetch = AsyncMock(return_value={"error": "rate limited"})
    with patch("services.tradeMonitor._guarded_leader_fetch", fetch):
        await _annotate_sell_fractions(trades, _LEADER)

    assert fetch.await_count == 1
    assert trades[0].get("_leader_sell_fraction_unavailable") is True
