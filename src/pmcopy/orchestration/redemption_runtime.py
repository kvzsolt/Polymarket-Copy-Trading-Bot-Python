"""Resource composition for one explicit gasless settlement command."""

import asyncio
from collections.abc import Awaitable, Callable
import time
import random
from typing import Protocol

import aiohttp
import asyncpg

from pmcopy.clients.network.http_client import AsyncHttpClient, HttpRetryPolicy, build_shared_session
from pmcopy.config.app_config import AppConfig
from pmcopy.domain.redemption import RedemptionReport
from pmcopy.integrations.polymarket.settlement_chain import PolygonSettlementReader
from pmcopy.integrations.polymarket.settlement_gateway import PolymarketSettlementGateway
from pmcopy.integrations.polymarket.settlement_rpc import SettlementRpcHttp
from pmcopy.integrations.polymarket.settlement_wallet import DepositWalletCodec
from pmcopy.interfaces.settlement import SettlementClock
from pmcopy.services.multiUserConfig import RuntimeUser
from pmcopy.services.settlement import RedemptionService
from pmcopy.storage.postgres.connection import init_pool, shutdown_pool
from pmcopy.storage.postgres.redemption_journal import PostgresRedemptionJournal


class AdmittedSettlementProvider(Protocol):
    """Expose metered read transport and its command-owned lifecycle."""

    async def post_json(self, url: str, *, json_body: dict[str, object], silent: bool) -> object:
        """Perform one admitted read."""
        ...

    async def aclose(self) -> None:
        """Close local accounting resources."""
        ...


class SystemSettlementClock:
    """Supply real time only at the outer application boundary."""

    def now(self) -> int:
        """Return Unix seconds for expiring wallet authorizations."""
        return int(time.time())

    def monotonic(self) -> float:
        """Return monotonic seconds for bounded polling."""
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        """Yield during a bounded confirmation interval."""
        await asyncio.sleep(seconds)


class RedemptionRuntime:
    """Build settlement-only adapters without discovery, orders, or bot startup."""

    def __init__(
        self,
        *,
        session_factory: Callable[[], Awaitable[aiohttp.ClientSession]] = build_shared_session,
        pool_factory: Callable[..., Awaitable[asyncpg.Pool]] = init_pool,
        close_pool: Callable[[], Awaitable[None]] = shutdown_pool,
        clock: SettlementClock | None = None,
        jitter: Callable[[float, float], float] = random.uniform,
        provider_factory: Callable[[AppConfig], AdmittedSettlementProvider] | None = None,
    ) -> None:
        """Inject resource factories and time; the command owns their lifetimes."""
        self._session_factory = session_factory
        self._pool_factory = pool_factory
        self._close_pool = close_pool
        self._clock = clock or SystemSettlementClock()
        self._jitter = jitter
        self._provider_factory = provider_factory

    async def run(self, config: AppConfig, user: RuntimeUser, *, execute: bool) -> RedemptionReport:
        """Run once with a single-attempt writer and optional durable journal.

        Args:
            config: Validated application settings, including the explicit live mode.
            user: One explicitly selected follower and its Deposit Wallet.
            execute: Explicit operator intent; false never opens signing or database state.

        Returns:
            A receipt-backed or incomplete report from the settlement service.
        """
        if execute:
            if config.execution.trading_mode != "live":
                raise ValueError("redemption_requires_live_trading_mode")
            config.redemption.require_credentials()
        provider = self._provider_factory(config) if self._provider_factory is not None else None
        session = None
        pool = None
        try:
            session = await self._session_factory()
            journal = None
            if execute:
                pool = await self._pool_factory(config.database.dsn, min_size=1, max_size=2)
                journal = PostgresRedemptionJournal(pool)
                await journal.initialize()
            reader = AsyncHttpClient(session)
            writer = AsyncHttpClient(session, retry_policy=HttpRetryPolicy(attempts=1))
            chain = PolygonSettlementReader(
                provider if provider is not None else SettlementRpcHttp(session),
                user.wallet.polygon_rpc_url,
                user.wallet.fallback_rpc_url,
                clock=self._clock,
                jitter=self._jitter,
            )
            gateway = PolymarketSettlementGateway(
                reader,
                writer,
                chain,
                self._clock,
                credentials=config.redemption if execute else None,
                signer=DepositWalletCodec(user.wallet.private_key) if execute else None,
            )
            return await RedemptionService(gateway, journal, self._clock).run(
                user.wallet.funder_address, execute=execute
            )
        finally:
            try:
                if pool is not None:
                    await self._close_pool()
            finally:
                try:
                    if session is not None:
                        await session.close()
                finally:
                    if provider is not None:
                        await provider.aclose()
