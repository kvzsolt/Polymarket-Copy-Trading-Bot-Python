"""Compose the canonical reconciliation service for manual CLI invocation."""

from __future__ import annotations

from asyncpg import Pool

from pmcopy.config.app_config import AppConfig
from pmcopy.integrations.polymarket.venue_runtime import build_polymarket_runtime
from pmcopy.services.positions.reconciliation import ReconciliationResult, ReconciliationService
from pmcopy.storage.postgres.orders_repo import OrdersRepo


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
        """Build the CCXT venue runtime and run one reconciliation pass.

        Returns:
            Structured reconciliation observations.
        """

        runtime = await build_polymarket_runtime(
            runtime_user=_cli_runtime_user(self._config),
            config=self._config,
        )
        try:
            service = ReconciliationService(
                clob_client=runtime.gateway,
                orders_repo=OrdersRepo(self._pool),
                config=self._config,
            )
            return await service.reconcile()
        finally:
            await runtime.close()


def _cli_runtime_user(config: AppConfig):
    """Build a minimal runtime-user identity for the single-user CLI path."""

    from pmcopy.services.accounts.multi_user_config import RuntimeUser

    return RuntimeUser(
        user_id=str(getattr(config.wallet, "funder_address", "") or "cli"),
        wallet=config.wallet,
        follows={},
    )


__all__ = ["ReconciliationCommandService"]
