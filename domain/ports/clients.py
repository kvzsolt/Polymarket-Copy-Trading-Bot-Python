"""External-system client ports (ABCs).

These interfaces isolate the application core from direct HTTP / RPC / CLOB
SDK coupling. Phase 3 will provide concrete adapters in ``clients/``; all
application services in Phase 4 will depend on the ABCs declared here.

Async methods mirror the real adapters' call shape (``aiohttp`` /
``py_clob_client_v2`` / ``web3``).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class AbstractMarketClient(ABC):
    """CLOB / market-data surface.

    Implementations wrap ``py_clob_client_v2.ClobClient`` plus the market-meta
    cache currently sitting in ``utils/createClobClient.py``.
    """

    @abstractmethod
    async def get_order_book(self, token_id: str) -> Any:
        """Fetch the current orderbook for *token_id* (bids + asks)."""

    @abstractmethod
    async def get_market_meta(self, condition_id: str) -> Any:
        """Resolve token_id, tick_size, and neg-risk flag for *condition_id*."""

    @abstractmethod
    async def get_last_trade_price(self, token_id: str) -> Any:
        """Return the most recent traded price for *token_id*."""

    @abstractmethod
    async def get_midpoint(self, token_id: str) -> Any:
        """Return the current book midpoint for *token_id*."""


class AbstractBalanceClient(ABC):
    """Wallet balance queries (USDC + native gas token)."""

    @abstractmethod
    async def get_usdc_balance(self, address: str) -> float | None: ...

    @abstractmethod
    async def get_native_balance_wei(self, address: str) -> int | None: ...


class AbstractOrderGateway(ABC):
    """Order submission surface with circuit-breaker awareness."""

    @abstractmethod
    async def post_order(
        self,
        *,
        token_id: str,
        side: str,
        size: float,
        price: float,
        order_type: str,
        idempotency_key: str,
        extra: dict[str, Any] | None = None,
    ) -> Any:
        """Submit an order to the CLOB and return a response envelope."""

    @abstractmethod
    async def cancel_order(self, order_id: str) -> Any: ...


class AbstractAnalyticsClient(ABC):
    """Polymarket Analytics (leader-discovery) HTTP surface."""

    @abstractmethod
    async def fetch_top_traders(
        self,
        *,
        limit: int,
        min_pnl: float,
        min_win_rate: float,
        min_positions: int,
    ) -> list[dict[str, Any]]: ...


class AbstractHttpClient(ABC):
    """Thin contract over the shared aiohttp session for bespoke endpoints."""

    @abstractmethod
    async def get_json(self, url: str, *, silent: bool = False, **kwargs: Any) -> Any: ...

    @abstractmethod
    async def post_json(self, url: str, *, silent: bool = False, **kwargs: Any) -> Any: ...
