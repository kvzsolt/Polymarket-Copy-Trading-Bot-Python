"""Lifecycle wrapper for the Polygon source-transaction pipeline."""

from __future__ import annotations

from application.source_transaction_pipeline import SourceTransactionPipeline
from interfaces.evm_events import EvmConnectionManager


class PolygonSourceTransactionRuntime:
    """Start, stop, and update one composed Polygon confirmation runtime."""

    def __init__(
        self,
        manager: EvmConnectionManager,
        pipeline: SourceTransactionPipeline,
    ) -> None:
        """Initialize the runtime lifecycle.

        Args:
            manager: Reconnecting multiplexed Alchemy connection owner.
            pipeline: Event-to-confirmation application service.
        """

        self._manager = manager
        self._pipeline = pipeline
        self._running = False

    async def start(self) -> None:
        """Connect and resynchronize exactly once."""

        if self._running:
            return
        await self._pipeline.restore()
        await self._manager.start()
        self._running = True

    async def stop(self) -> None:
        """Stop the runtime exactly once."""

        if not self._running:
            return
        self._running = False
        await self._manager.stop()

    def update_watched_wallets(self, wallets: set[str]) -> None:
        """Replace the watched-wallet snapshot used for future candidates.

        Args:
            wallets: Complete current leader wallet set.
        """

        self._pipeline.update_watched_wallets(frozenset(wallet.lower() for wallet in wallets))


__all__ = ["PolygonSourceTransactionRuntime"]
