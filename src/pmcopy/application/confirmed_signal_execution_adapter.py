"""Adapt confirmed source claims to the existing on-chain signal boundary."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from pmcopy.domain.source_transactions import (
    ConfirmedExecutionClaim,
    SourceSide,
    WatchedRole,
)
from pmcopy.interfaces.source_transactions import Clock
from pmcopy.interfaces.source_time import BlockTimestampReader
from pmcopy.services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    MAKER_SIDE_BUY,
    MAKER_SIDE_SELL,
    OnchainTradeSignal,
)


class OnchainSignalSink(Protocol):
    """Submit confirmed fills to the legacy leader-signal boundary."""

    async def submit(self, signal: OnchainTradeSignal) -> None:
        """Submit one authoritative, non-provisional on-chain signal."""

        ...


class ConfirmedSignalExecutionAdapter:
    """Publish only atomically claimed, successfully confirmed source fills."""

    def __init__(
        self,
        sink: OnchainSignalSink,
        clock: Clock,
        *,
        block_timestamp_reader: BlockTimestampReader | None = None,
    ) -> None:
        """Initialize the compatibility adapter.

        Args:
            sink: Existing on-chain leader signal boundary.
            clock: Deterministic signal observation clock.
            block_timestamp_reader: Immutable chain time; missing wiring preserves unknown age.
        """

        self._sink = sink
        self._clock = clock
        self._block_timestamp_reader = block_timestamp_reader

    async def execute_confirmed_source(self, claim: ConfirmedExecutionClaim) -> None:
        """Publish one confirmed source execution claim.

        Args:
            claim: Atomic follower/fill claim released by the confirmation gate.
        """

        fill = claim.fill
        event_ts = (
            await self._block_timestamp_reader.timestamp_ms(fill.block_hash)
            if self._block_timestamp_reader is not None
            else None
        )
        signal = OnchainTradeSignal(
            leader_wallet=fill.leader_wallet,
            token_id=fill.token_id,
            side=fill.side.value,
            size=fill.shares,
            price=fill.price,
            usdc_size=fill.notional,
            tx_hash=fill.transaction_hash,
            confidence=DetectionConfidence.MINED_AUTHORITATIVE,
            maker_side=self._maker_side_for(fill.side, fill.watched_role),
            watched_role=fill.watched_role.value,
            event_ts=event_ts,
            log_index=fill.log_index,
            condition_id=fill.condition_id,
            provisional=False,
            exchange=fill.exchange_address,
            order_hash=fill.order_hash,
            block_number=fill.block_number,
            block_hash=fill.block_hash,
            source_receipt=claim.source_receipt,
        )
        await self._sink.submit(signal)

    @staticmethod
    def _maker_side_for(side: SourceSide, role: WatchedRole) -> int:
        maker_side = MAKER_SIDE_BUY if side is SourceSide.BUY else MAKER_SIDE_SELL
        if role is WatchedRole.MAKER:
            return maker_side
        return MAKER_SIDE_SELL if maker_side == MAKER_SIDE_BUY else MAKER_SIDE_BUY


class CallableOnchainSignalSink:
    """Expose an async signal callback through the narrow sink port."""

    def __init__(
        self,
        callback: Callable[[OnchainTradeSignal], Awaitable[None]],
    ) -> None:
        """Initialize the callback adapter.

        Args:
            callback: Existing asynchronous signal consumer.
        """

        self._callback = callback

    async def submit(self, signal: OnchainTradeSignal) -> None:
        """Delegate one authoritative signal to the callback.

        Args:
            signal: Confirmed on-chain leader fill.
        """

        await self._callback(signal)


__all__ = ["CallableOnchainSignalSink", "ConfirmedSignalExecutionAdapter"]
