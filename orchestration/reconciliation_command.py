"""Compose the canonical reconciliation service for manual CLI invocation."""

from __future__ import annotations

from asyncpg import Pool

from config.app_config import AppConfig
from integrations.polymarket.clob_v2_gateway import PolymarketGateway
from integrations.polymarket.runtime_gateway import build_runtime_polymarket_gateway
from integrations.polymarket.sdk_client import create_polymarket_client
from services.reconciliation import ReconciliationResult, ReconciliationService
from storage.postgres.orders_repo import OrdersRepo


class ReconciliationCommandService:
    """Run the same configured reconciliation service used by the runtime."""

    def __init__(self, *, config: AppConfig, pool: Pool) -> None:
        """Store validated config and the caller-owned database pool.

        Args:
            config: Validated application configuration.
            pool: Existing PostgreSQL pool owned by the CLI lifecycle.
        """

        self._config = config
        self._pool = pool

    async def run(self) -> ReconciliationResult:
        """Build the configured venue adapter and run one reconciliation pass.

        Returns:
            Structured reconciliation observations.
        """

        client = await create_polymarket_client(
            wallet_config=self._config.wallet,
            config=self._config,
            authenticated=not self._config.execution.paper_trade_only,
        )
        bundle = await build_runtime_polymarket_gateway(
            sdk_gateway=PolymarketGateway(client, self._config),
            config=self._config,
            wallet_config=self._config.wallet,
        )
        try:
            service = ReconciliationService(
                clob_client=bundle.gateway,
                orders_repo=OrdersRepo(self._pool),
                config=self._config,
            )
            return await service.reconcile()
        finally:
            await bundle.close()
            await client.close()


__all__ = ["ReconciliationCommandService"]
