from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from services.discovery.copyability_replay import (
    CopyabilityReplayService,
    StaticBalanceProvider,
)
from services.discovery.signal_grouper import GroupedSignal


def _config(balance_headroom: float = 0):
    return SimpleNamespace(
        discovery=SimpleNamespace(discovery_min_group_notional_usdc=5),
        sizing=SimpleNamespace(
            mode="proportional",
            percentage=100,
            fixed_amount=5,
            buy_min_amount=5,
            buy_max_amount=20,
            trade_multiplier=1,
            tiered_trade_multipliers="",
            adaptive_basis="trade_size",
            adaptive_min_percent=0,
            adaptive_max_percent=0,
            adaptive_threshold_usdc=500,
        ),
        execution=SimpleNamespace(
            max_position_size_usdc=0,
            min_balance_headroom_usdc=balance_headroom,
        ),
        risk=SimpleNamespace(max_open_positions=10),
    )


def _signal(
    idx: int,
    notional: str = "10",
    *,
    side: str = "BUY",
    size: str = "10",
    condition_id: str | None = None,
    token_id: str | None = None,
):
    return GroupedSignal(
        wallet="0x1111111111111111111111111111111111111111",
        condition_id=condition_id or f"cond-{idx}",
        token_id=token_id or f"token-{idx}",
        side=side,
        first_ts=100 + idx,
        last_ts=100 + idx,
        total_size=Decimal(size),
        total_usdc_size=Decimal(notional),
        vwap_price=Decimal("1"),
        tx_hashes=(f"0x{idx}",),
        trade_ids=(str(idx),),
        title="NBA",
        market_slug="nba",
        event_slug="",
        outcome="Yes",
        raw_count=1,
    )


@pytest.mark.asyncio
async def test_replay_reports_copyable_ratios():
    replay = CopyabilityReplayService(
        config=_config(),
        balance_provider=StaticBalanceProvider("100"),
    )

    result = await replay.replay_wallet("0x1111111111111111111111111111111111111111", [_signal(1), _signal(2)])

    assert result.meaningful_signals == 2
    assert result.copyable_signal_ratio == 1.0
    assert result.copyable_notional_ratio == 1.0


@pytest.mark.asyncio
async def test_replay_rejects_when_headroom_would_be_breached():
    replay = CopyabilityReplayService(
        config=_config(balance_headroom=95),
        balance_provider=StaticBalanceProvider("100"),
    )

    result = await replay.replay_wallet("0x1111111111111111111111111111111111111111", [_signal(1)])

    assert result.copyable_signals == 0
    assert result.signal_results[0].reason == "insufficient_balance_headroom"


@pytest.mark.asyncio
async def test_replay_sell_releases_cash():
    replay = CopyabilityReplayService(
        config=_config(),
        balance_provider=StaticBalanceProvider("10"),
    )

    result = await replay.replay_wallet(
        "0x1111111111111111111111111111111111111111",
        [
            _signal(1, condition_id="cond-a", token_id="token-a"),
            _signal(
                2,
                side="SELL",
                condition_id="cond-a",
                token_id="token-a",
            ),
            _signal(3, condition_id="cond-b", token_id="token-b"),
        ],
    )

    assert result.copyable_signals == 2
    assert [r.reason for r in result.signal_results] == ["ok", "ok"]
    assert result.copyable_signal_ratio == 1.0


@pytest.mark.asyncio
async def test_replay_partial_sell_releases_only_proportional_cash():
    replay = CopyabilityReplayService(
        config=_config(),
        balance_provider=StaticBalanceProvider("10"),
    )

    result = await replay.replay_wallet(
        "0x1111111111111111111111111111111111111111",
        [
            _signal(1, notional="10", size="10", condition_id="cond-a", token_id="token-a"),
            _signal(
                2,
                side="SELL",
                size="5",
                condition_id="cond-a",
                token_id="token-a",
            ),
            _signal(3, notional="6", size="6", condition_id="cond-b", token_id="token-b"),
        ],
    )

    assert result.copyable_signals == 1
    assert [r.reason for r in result.signal_results] == [
        "ok",
        "insufficient_balance_headroom",
    ]


@pytest.mark.asyncio
async def test_replay_closed_positions_do_not_false_exhaust_balance():
    replay = CopyabilityReplayService(
        config=_config(),
        balance_provider=StaticBalanceProvider("10"),
    )

    result = await replay.replay_wallet(
        "0x1111111111111111111111111111111111111111",
        [
            _signal(1, condition_id="cond-a", token_id="token-a"),
            _signal(
                2,
                side="SELL",
                condition_id="cond-a",
                token_id="token-a",
            ),
            _signal(3, condition_id="cond-b", token_id="token-b"),
            _signal(
                4,
                side="SELL",
                condition_id="cond-b",
                token_id="token-b",
            ),
            _signal(5, condition_id="cond-c", token_id="token-c"),
        ],
    )

    assert result.meaningful_signals == 3
    assert result.copyable_signals == 3
    assert all(r.reason == "ok" for r in result.signal_results)
