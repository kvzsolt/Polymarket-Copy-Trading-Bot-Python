"""Map fresh native owner evidence into the existing per-follower copy dispatcher."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Protocol

from domain.source_time import TransportReceipt
from interfaces.grouped_detection import DetectionStateStore
from interfaces.source_time import BlockTimestampReader
from services.onchain_detection.native_owner import NativeOwnerLogDecoder
from services.onchain_detection.trade_detection_models import (
    DetectionConfidence,
    OnchainTradeSignal,
)


class ConditionResolver(Protocol):
    """Resolve market identity without inventing missing evidence."""

    async def resolve(self, token_id: str) -> str:
        """Return an authoritative condition ID or an empty unavailable value."""
        ...


class NativeOwnerObserver:
    """Deduplicate owner evidence before enrichment and existing task dispatch.

    The source ledger records before dispatch. A crash may under-copy; it cannot
    cause a second submission. Actual follower/task claims remain downstream.
    """

    def __init__(
        self,
        *,
        wallets: Sequence[str],
        state: DetectionStateStore,
        condition_resolver: ConditionResolver,
        block_timestamps: BlockTimestampReader,
        sink: Callable[[OnchainTradeSignal], Awaitable[None]],
    ) -> None:
        """Inject all I/O and fix the active roster for this process lifetime."""
        self._wallets = frozenset(wallets)
        self._state = state
        self._conditions = condition_resolver
        self._timestamps = block_timestamps
        self._sink = sink
        self._decoder = NativeOwnerLogDecoder()

    def retain_wallets(self, wallets: Sequence[str]) -> None:
        """Retire owners immediately; additions require a newly acknowledged runtime."""
        self._wallets = self._wallets.intersection(wallets)

    async def observe(
        self, payload: Mapping[str, object], receipt: TransportReceipt, *, recovered: bool
    ) -> str:
        """Record each inclusion but dispatch only new, fully enriched owner actions.

        Args:
            payload: Original log result, never a counterparty-derived synthetic fill.
            receipt: Local transport arrival clock pair preserved through processing.
            recovered: Historical evidence cannot create a fresh entry.

        Returns:
            A bounded classification suitable for operational counters.
        """
        fill = self._decoder.decode(payload)
        if fill.owner not in self._wallets:
            return "unwatched"
        disposition = self._state.observe(fill, recovered=recovered)
        self._state.record_observation(fill.owner, receipt.wall_timestamp_ms)
        if disposition != "new":
            return disposition
        condition = await self._conditions.resolve(fill.token_id)
        timestamp = await self._timestamps.timestamp_ms(fill.block_hash)
        if fill.owner not in self._wallets:
            return "retired"
        if not condition or timestamp is None:
            self._state.set_effective((fill.owner,), "paused", "enrichment_unavailable")
            return "enrichment_unavailable"
        signal = OnchainTradeSignal(
            leader_wallet=fill.owner,
            token_id=fill.token_id,
            side=fill.side,
            size=fill.shares,
            price=fill.price,
            usdc_size=fill.notional,
            tx_hash=fill.transaction_hash,
            confidence=DetectionConfidence.MINED_AUTHORITATIVE,
            maker_side=0 if fill.side == "BUY" else 1,
            watched_role="native_taker" if fill.native_taker else "maker",
            event_ts=timestamp,
            log_index=fill.log_index,
            condition_id=condition,
            provisional=False,
            exchange=fill.exchange,
            order_hash=fill.order_hash,
            block_number=fill.block_number,
            block_hash=fill.block_hash,
            source_receipt=receipt,
            source_action_id=fill.action_id,
        )
        await self._sink(signal)
        return "new"
