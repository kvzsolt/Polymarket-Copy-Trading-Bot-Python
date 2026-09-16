"""Tests for EVM event routing into the source confirmation gate."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from application.source_transaction_pipeline import SourceTransactionPipeline
from domain.source_transactions import (
    ConfirmedTransaction,
    EvmHead,
    LeaderTradeCorrelation,
    PendingSourceMatch,
    PendingTransaction,
    ReservedTransaction,
    SourceSide,
    WatchedRole,
)

_CHAIN_ID = 137
_TX_HASH = "0x" + "ab" * 32
_LEADER = "0x" + "11" * 20
_EXCHANGE = "0x" + "22" * 20
_NOW = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)


class FakePendingBuilder:
    """Return a configured pending candidate and capture watched wallets."""

    def __init__(self, pending: PendingTransaction | None) -> None:
        self._pending = pending
        self.watched_calls: list[frozenset[str]] = []

    def from_notification(
        self,
        notification: dict[str, object],
        watched_wallets: frozenset[str],
    ) -> PendingTransaction | None:
        """Return the configured candidate."""

        del notification
        self.watched_calls.append(watched_wallets)
        return self._pending


class FakeSourceGate:
    """Capture every routed gate operation."""

    def __init__(self) -> None:
        self.pending: list[PendingTransaction] = []
        self.correlations: list[LeaderTradeCorrelation] = []
        self.heads: list[EvmHead] = []
        self.mined: list[tuple[int, str]] = []
        self.disconnects = 0
        self.resyncs = 0
        self.restores = 0

    async def restore(self) -> None:
        """Capture durable state restoration."""

        self.restores += 1

    async def observe_pending(
        self,
        pending: PendingTransaction,
    ) -> ReservedTransaction | None:
        """Capture and reserve the first pending candidate."""

        self.pending.append(pending)
        return ReservedTransaction(
            reservation_id=f"{pending.chain_id}:{pending.transaction_hash}",
            transaction=pending,
            reserved_at=_NOW,
        )

    async def observe_leader_event(self, correlation: LeaderTradeCorrelation) -> None:
        """Capture a generated source correlation."""

        self.correlations.append(correlation)

    async def on_new_head(self, head: EvmHead) -> tuple[ConfirmedTransaction, ...]:
        """Capture one head."""

        self.heads.append(head)
        return ()

    async def on_mined_transaction(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> ConfirmedTransaction | None:
        """Capture one included transaction identity."""

        self.mined.append((chain_id, transaction_hash))
        return None

    async def on_disconnect(self) -> None:
        """Capture fail-closed transport loss."""

        self.disconnects += 1

    async def resynchronize(self) -> tuple[ConfirmedTransaction, ...]:
        """Capture reconnect resynchronization."""

        self.resyncs += 1
        return ()


def _pending() -> PendingTransaction:
    return PendingTransaction(
        chain_id=_CHAIN_ID,
        transaction_hash=_TX_HASH,
        sender="0x" + "33" * 20,
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
                order_hash=None,
                token_id="1001",
                condition_id="0x" + "44" * 32,
                side=SourceSide.BUY,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_pending_event_reserves_and_correlates_without_execution_shortcut() -> None:
    """Qualified pending events create only reservation correlations."""

    builder = FakePendingBuilder(_pending())
    gate = FakeSourceGate()
    pipeline = SourceTransactionPipeline(
        chain_id=_CHAIN_ID,
        watched_wallets=frozenset({_LEADER}),
        pending_builder=builder,
        gate=gate,
    )

    await pipeline.on_pending_transaction({"hash": _TX_HASH})

    assert gate.pending == [_pending()]
    assert len(gate.correlations) == 1
    correlation = gate.correlations[0]
    assert correlation.transaction_hash == _TX_HASH
    assert correlation.leader_wallet == _LEADER


@pytest.mark.asyncio
async def test_pipeline_routes_heads_logs_disconnect_and_resync() -> None:
    """Every EVM lifecycle event reaches the single confirmation gate."""

    builder = FakePendingBuilder(None)
    gate = FakeSourceGate()
    pipeline = SourceTransactionPipeline(
        chain_id=_CHAIN_ID,
        watched_wallets=frozenset({_LEADER}),
        pending_builder=builder,
        gate=gate,
    )
    head = EvmHead(
        chain_id=_CHAIN_ID,
        number=101,
        block_hash="0x" + "55" * 32,
        parent_hash="0x" + "66" * 32,
        observed_at=_NOW,
    )

    await pipeline.on_new_head(head)
    await pipeline.on_log({"transactionHash": _TX_HASH})
    await pipeline.on_disconnect()
    await pipeline.resynchronize()

    assert gate.heads == [head]
    assert gate.mined == [(_CHAIN_ID, _TX_HASH)]
    assert gate.disconnects == 1
    assert gate.resyncs == 1


@pytest.mark.asyncio
async def test_watched_wallet_update_is_used_by_next_pending_event() -> None:
    """Leader hot-add updates qualification without rebuilding the pipeline."""

    builder = FakePendingBuilder(None)
    pipeline = SourceTransactionPipeline(
        chain_id=_CHAIN_ID,
        watched_wallets=frozenset({_LEADER}),
        pending_builder=builder,
        gate=FakeSourceGate(),
    )
    replacement = "0x" + "77" * 20

    pipeline.update_watched_wallets(frozenset({replacement}))
    await pipeline.on_pending_transaction({"hash": _TX_HASH})

    assert builder.watched_calls == [frozenset({replacement})]
