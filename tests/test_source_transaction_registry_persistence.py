"""Tests for durable source reservation and consumption recovery."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from pmcopy.domain.source_transactions import (
    ConfirmedSourceFill,
    ConfirmedTransaction,
    LeaderTradeCorrelation,
    PendingSourceMatch,
    PendingTransaction,
    ReservedTransaction,
    SourceSide,
    SourceTransactionSnapshot,
    TransactionState,
    WatchedRole,
)
from pmcopy.services.onchain_detection.source_transaction_gate import SourceTransactionRegistry

_NOW = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
_TX_HASH = "0x" + "ab" * 32
_ORDER_HASH = "0x" + "bc" * 32
_LEADER = "0x" + "11" * 20
_FOLLOWER = "0x" + "22" * 20
_EXCHANGE = "0x" + "33" * 20


class FakeClock:
    """Fixed source-state clock."""

    def now(self) -> datetime:
        """Return the deterministic instant."""

        return _NOW


class FakeSourceTransactionStateStore:
    """Durable-across-instance in-memory snapshot store."""

    def __init__(self) -> None:
        self.snapshot = SourceTransactionSnapshot.empty()
        self.saves = 0

    async def load(self) -> SourceTransactionSnapshot:
        """Return the last saved immutable snapshot."""

        return self.snapshot

    async def save(self, snapshot: SourceTransactionSnapshot) -> None:
        """Replace the last saved snapshot."""

        self.snapshot = snapshot
        self.saves += 1


def _pending() -> PendingTransaction:
    return PendingTransaction(
        chain_id=137,
        transaction_hash=_TX_HASH,
        sender="0x" + "44" * 20,
        nonce=7,
        target=_EXCHANGE,
        observed_at=_NOW,
        expires_at=_NOW + timedelta(seconds=60),
        matches=(
            PendingSourceMatch(
                leader_wallet=_LEADER,
                watched_role=WatchedRole.MAKER,
                exchange_address=_EXCHANGE,
                order_position=0,
                order_hash=_ORDER_HASH,
                token_id="1001",
                condition_id="0x" + "55" * 32,
                side=SourceSide.BUY,
            ),
        ),
    )


def _correlation() -> LeaderTradeCorrelation:
    return LeaderTradeCorrelation(
        source_event_id="feed-1",
        follower_account=_FOLLOWER,
        copy_task_id="task-1",
        chain_id=137,
        transaction_hash=_TX_HASH,
        leader_wallet=_LEADER,
        token_id="1001",
        side=SourceSide.BUY,
        order_hash=_ORDER_HASH,
    )


def _confirmed(reservation: ReservedTransaction) -> ConfirmedTransaction:
    return ConfirmedTransaction(
        reservation=reservation,
        block_number=101,
        block_hash="0x" + "66" * 32,
        fills=(
            ConfirmedSourceFill(
                chain_id=137,
                transaction_hash=_TX_HASH,
                log_index=3,
                order_hash=_ORDER_HASH,
                leader_wallet=_LEADER,
                watched_role=WatchedRole.MAKER,
                exchange_address=_EXCHANGE,
                token_id="1001",
                condition_id="0x" + "55" * 32,
                side=SourceSide.BUY,
                shares=Decimal("4"),
                notional=Decimal("2"),
                price=Decimal("0.5"),
                block_number=101,
                block_hash="0x" + "66" * 32,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_restart_restores_consumed_fill_and_prevents_second_claim() -> None:
    """A recreated registry cannot re-claim a durably consumed fill."""

    store = FakeSourceTransactionStateStore()
    first = SourceTransactionRegistry(clock=FakeClock(), state_store=store)
    await first.restore()
    await first.complete_resync()
    reservation = await first.reserve(_pending())
    assert reservation is not None
    await first.add_correlation(_correlation())
    claims = await first.confirm(_confirmed(reservation))
    assert len(claims) == 1
    await first.complete_execution_claim(claims[0])

    restarted = SourceTransactionRegistry(clock=FakeClock(), state_store=store)
    await restarted.restore()
    replayed = await restarted.add_correlation(_correlation())

    assert replayed == ()
    assert await restarted.state_for(137, _TX_HASH) is TransactionState.CONSUMED
    assert await restarted.reserve(_pending()) is None
    assert store.saves >= 3


@pytest.mark.asyncio
async def test_restart_restores_unfinished_reservation_as_resync() -> None:
    """An interrupted reservation never resumes as consumed or executable."""

    store = FakeSourceTransactionStateStore()
    first = SourceTransactionRegistry(clock=FakeClock(), state_store=store)
    await first.restore()
    await first.reserve(_pending())

    restarted = SourceTransactionRegistry(clock=FakeClock(), state_store=store)
    await restarted.restore()

    assert await restarted.state_for(137, _TX_HASH) is TransactionState.RESYNC
    assert len(await restarted.resync_reservations()) == 1
