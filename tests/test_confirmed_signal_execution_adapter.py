"""Tests for the confirmed-source compatibility signal adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from application.confirmed_signal_execution_adapter import ConfirmedSignalExecutionAdapter
from domain.source_transactions import (
    ConfirmedExecutionClaim,
    ConfirmedSourceFill,
    CopyConsumptionKey,
    LeaderTradeCorrelation,
    SourceSide,
    WatchedRole,
)
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)

_NOW = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
_TX_HASH = "0x" + "ab" * 32
_ORDER_HASH = "0x" + "bc" * 32
_LEADER = "0x" + "11" * 20
_FOLLOWER = "0x" + "22" * 20
_EXCHANGE = "0x" + "33" * 20


class FakeClock:
    """Fixed signal timestamp clock."""

    def now(self) -> datetime:
        """Return the fixed instant."""

        return _NOW


class FakeSignalSink:
    """Capture normalized on-chain signals."""

    def __init__(self) -> None:
        self.signals: list[OnchainTradeSignal] = []

    async def submit(self, signal: OnchainTradeSignal) -> None:
        """Record one signal."""

        self.signals.append(signal)


def _claim(*, role: WatchedRole = WatchedRole.MAKER) -> ConfirmedExecutionClaim:
    fill = ConfirmedSourceFill(
        chain_id=137,
        transaction_hash=_TX_HASH,
        log_index=3,
        order_hash=_ORDER_HASH,
        leader_wallet=_LEADER,
        watched_role=role,
        exchange_address=_EXCHANGE,
        token_id="1001",
        condition_id="0x" + "44" * 32,
        side=SourceSide.BUY,
        shares=Decimal("4"),
        notional=Decimal("2"),
        price=Decimal("0.5"),
        block_number=101,
        block_hash="0x" + "55" * 32,
    )
    correlation = LeaderTradeCorrelation(
        source_event_id="source-event",
        follower_account=_FOLLOWER,
        copy_task_id="task-1",
        chain_id=137,
        transaction_hash=_TX_HASH,
        leader_wallet=_LEADER,
        token_id="1001",
        side=SourceSide.BUY,
        order_hash=_ORDER_HASH,
    )
    return ConfirmedExecutionClaim(
        consumption_key=CopyConsumptionKey(
            follower_account=_FOLLOWER,
            copy_task_id="task-1",
            source_fill=fill.identity,
        ),
        correlation=correlation,
        fill=fill,
    )


@pytest.mark.asyncio
async def test_adapter_emits_only_authoritative_confirmed_signal() -> None:
    """A confirmed claim maps to one non-provisional legacy signal."""

    sink = FakeSignalSink()
    adapter = ConfirmedSignalExecutionAdapter(sink=sink, clock=FakeClock())

    await adapter.execute_confirmed_source(_claim())

    assert len(sink.signals) == 1
    signal = sink.signals[0]
    assert signal.confidence is DetectionConfidence.MINED_AUTHORITATIVE
    assert signal.provisional is False
    assert signal.tx_hash == _TX_HASH
    assert signal.log_index == 3
    assert signal.block_number == 101
    assert signal.block_hash == "0x" + "55" * 32
    assert signal.order_hash == _ORDER_HASH
    assert signal.leader_wallet == _LEADER
    assert signal.size == Decimal("4")
    assert signal.usdc_size == Decimal("2")
    assert signal.price == Decimal("0.5")
    assert signal.exchange == _EXCHANGE
    assert signal.event_ts is None


@pytest.mark.asyncio
async def test_adapter_preserves_taker_role_when_deriving_raw_maker_side() -> None:
    """Watched taker BUY corresponds to a raw maker SELL event."""

    sink = FakeSignalSink()
    adapter = ConfirmedSignalExecutionAdapter(sink=sink, clock=FakeClock())

    await adapter.execute_confirmed_source(_claim(role=WatchedRole.TAKER))

    assert sink.signals[0].watched_role == "taker"
    assert sink.signals[0].maker_side == 1
