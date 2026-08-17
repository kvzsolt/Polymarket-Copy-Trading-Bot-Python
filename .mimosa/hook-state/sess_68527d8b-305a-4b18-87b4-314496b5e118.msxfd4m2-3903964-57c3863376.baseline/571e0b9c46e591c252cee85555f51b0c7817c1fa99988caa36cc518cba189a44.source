"""Route Polygon subscription events through source confirmation state."""

from __future__ import annotations

from domain.source_transactions import (
    EvmHead,
    LeaderTradeCorrelation,
    PendingSourceMatch,
)
from interfaces.source_transactions import (
    PendingTransactionBuilder,
    SourceConfirmationGate,
)


class SourceTransactionPipeline:
    """Convert EVM events into reservations and confirmed-source transitions."""

    _SYSTEM_CONSUMER = "0x" + "00" * 20
    _SYSTEM_COPY_TASK = "onchain-source-detection"

    def __init__(
        self,
        chain_id: int,
        watched_wallets: frozenset[str],
        pending_builder: PendingTransactionBuilder,
        gate: SourceConfirmationGate,
    ) -> None:
        """Initialize the event pipeline.

        Args:
            chain_id: Positive EVM chain identifier.
            watched_wallets: Initial leader wallets eligible for qualification.
            pending_builder: Provider payload qualification boundary.
            gate: Atomic reservation and confirmation service.

        Raises:
            ValueError: If ``chain_id`` is not positive.
        """

        if chain_id <= 0:
            raise ValueError("chain_id must be positive")
        self._chain_id = chain_id
        self._watched_wallets = frozenset(wallet.lower() for wallet in watched_wallets)
        self._pending_builder = pending_builder
        self._gate = gate

    async def restore(self) -> None:
        """Load durable source state before connecting event streams."""

        await self._gate.restore()

    def update_watched_wallets(self, wallets: frozenset[str]) -> None:
        """Replace the watched-wallet snapshot for subsequent notifications.

        Args:
            wallets: Complete current leader wallet set.
        """

        self._watched_wallets = frozenset(wallet.lower() for wallet in wallets)

    async def on_pending_transaction(self, transaction: dict[str, object]) -> None:
        """Qualify and atomically reserve one full pending transaction.

        Args:
            transaction: Full Alchemy pending transaction object.
        """

        pending = self._pending_builder.from_notification(
            transaction,
            self._watched_wallets,
        )
        if pending is None:
            return
        await self._gate.observe_pending(pending)
        for match in pending.matches:
            await self._gate.observe_leader_event(
                self._correlation_for_match(pending.transaction_hash, match)
            )

    async def on_new_head(self, head: EvmHead) -> None:
        """Resolve reservations made visible by a new Polygon head.

        Args:
            head: Normalized ``newHeads`` notification.
        """

        await self._gate.on_new_head(head)

    async def on_log(self, log: dict[str, object]) -> None:
        """Resolve the exact transaction named by an included log.

        Args:
            log: Raw Alchemy log subscription result.
        """

        transaction_hash = str(
            log.get("transactionHash") or log.get("transaction_hash") or ""
        ).lower()
        if not transaction_hash:
            return
        await self._gate.on_mined_transaction(self._chain_id, transaction_hash)

    async def on_disconnect(self) -> None:
        """Fail closed after an unexpected Alchemy socket loss."""

        await self._gate.on_disconnect()

    async def resynchronize(self) -> None:
        """Resolve in-flight source state after reconnect subscriptions recover."""

        await self._gate.resynchronize()

    def _correlation_for_match(
        self,
        transaction_hash: str,
        match: PendingSourceMatch,
    ) -> LeaderTradeCorrelation:
        position = "taker" if match.order_position is None else str(match.order_position)
        source_event_id = "|".join(
            (
                str(self._chain_id),
                transaction_hash,
                match.leader_wallet,
                match.watched_role.value,
                position,
                match.token_id,
            )
        )
        return LeaderTradeCorrelation(
            source_event_id=source_event_id,
            follower_account=self._SYSTEM_CONSUMER,
            copy_task_id=self._SYSTEM_COPY_TASK,
            chain_id=self._chain_id,
            transaction_hash=transaction_hash,
            leader_wallet=match.leader_wallet,
            token_id=match.token_id,
            side=match.side,
            order_hash=match.order_hash,
        )


__all__ = ["SourceTransactionPipeline"]
