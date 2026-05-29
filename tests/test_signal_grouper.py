from __future__ import annotations

from decimal import Decimal

from domain.discovery_models import ObservedTrade
from services.discovery.signal_grouper import SignalGrouper


def _trade(ts: int, *, side: str = "BUY", tx: str = "0x1", usdc: str = "5"):
    return ObservedTrade(
        trader_wallet="0x1111111111111111111111111111111111111111",
        condition_id="cond",
        token_id="token",
        side=side,
        size=Decimal("10"),
        price=Decimal(usdc) / Decimal("10"),
        usdc_size=Decimal(usdc),
        tx_hash=tx,
        event_ts=ts,
        trade_id=tx,
        title="NBA finals",
    )


def test_micro_fills_group_into_one_signal():
    grouper = SignalGrouper(
        window_seconds=60,
        max_gap_seconds=10,
        min_group_notional_usdc=Decimal("1"),
    )

    signals = grouper.group([_trade(100, tx="0x1"), _trade(105, tx="0x2")])

    assert len(signals) == 1
    assert signals[0].raw_count == 2
    assert signals[0].total_usdc_size == Decimal("10")


def test_different_side_does_not_group():
    grouper = SignalGrouper(
        window_seconds=60,
        max_gap_seconds=10,
        min_group_notional_usdc=Decimal("1"),
    )

    signals = grouper.group([_trade(100, side="BUY"), _trade(105, side="SELL")])

    assert [s.side for s in signals] == ["BUY", "SELL"]

