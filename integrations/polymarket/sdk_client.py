"""Lifecycle-safe facade over the official unified Polymarket Python SDK.

Only this module constructs SDK clients.  The rest of the application talks to
``ClobGateway`` domain types so beta SDK models and credentials cannot leak into
the money path.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from polymarket import (
    ApiKeyCreds,
    AsyncPublicClient,
    AsyncSecureClient,
    RequestRejectedError,
)

from integrations.polymarket.models import PRICE_TOKEN_NOT_FOUND


@dataclass(frozen=True, slots=True)
class MarketMeta:
    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    best_bid: Decimal | None
    best_ask: Decimal | None


class PolymarketClient:
    """Own public/authenticated SDK clients and close them exactly once."""

    def __init__(
        self,
        public_client: AsyncPublicClient,
        *,
        secure_client: AsyncSecureClient | None = None,
        wallet_config: Any = None,
    ) -> None:
        self.public_client = public_client
        self.secure_client = secure_client
        self.wallet_config = wallet_config
        self._closed = False

    @property
    def client(self) -> AsyncSecureClient | AsyncPublicClient:
        """Compatibility view while callers are moved to the gateway port."""
        return self.secure_client or self.public_client

    @property
    def api_creds(self) -> ApiKeyCreds | None:
        return self.secure_client.credentials if self.secure_client is not None else None

    @property
    def credentials(self) -> ApiKeyCreds | None:
        return self.api_creds

    def require_secure(self) -> AsyncSecureClient:
        if self.secure_client is None:
            raise RuntimeError("authenticated Polymarket client is unavailable in paper mode")
        return self.secure_client

    async def get_order_book(self, token_id: str) -> Any:
        return await self.public_client.get_order_book(token_id=token_id)

    async def get_price(self, token_id: str, side: str) -> dict[str, str] | object | None:
        """Compatibility shape for the risk monitor while it adopts the port."""
        try:
            value = await self.public_client.get_price(
                token_id=token_id,
                side=str(side).upper(),
            )
        except RequestRejectedError as exc:
            if exc.status == 404:
                return PRICE_TOKEN_NOT_FOUND
            return None
        return {"price": str(value)}

    async def get_market_meta(self, token_id: str, *, refresh: bool = False) -> MarketMeta:
        del refresh  # Unified SDK responses are fresh; caching belongs to MarketDataService.
        book = await self.public_client.get_order_book(token_id=token_id)
        bids = tuple(book.bids)
        asks = tuple(book.asks)
        return MarketMeta(
            token_id=str(book.token_id),
            tick_size=Decimal(book.tick_size),
            min_order_size=Decimal(book.min_order_size),
            neg_risk=book.neg_risk,
            best_bid=max((Decimal(level.price) for level in bids), default=None),
            best_ask=min((Decimal(level.price) for level in asks), default=None),
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        # AsyncSecureClient owns its own public transports. A separate public
        # client is still used in paper/live alike for a uniform market-data
        # path, so both must be given a chance to close even when one fails.
        clients = [self.public_client]
        if self.secure_client is not None:
            clients.insert(0, self.secure_client)
        results = await asyncio.gather(
            *(client.close() for client in clients),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def __aenter__(self) -> "PolymarketClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()


def _configured_credentials(config: Any) -> ApiKeyCreds | None:
    clob = getattr(config, "clob", None)
    key = str(getattr(clob, "api_key", "") or "").strip()
    secret = str(getattr(clob, "api_secret", "") or "").strip()
    passphrase = str(getattr(clob, "api_passphrase", "") or "").strip()
    populated = tuple(bool(value) for value in (key, secret, passphrase))
    if any(populated) and not all(populated):
        raise ValueError("CLOB API credentials must be all configured or all omitted")
    if not all(populated):
        return None
    return ApiKeyCreds(key=key, secret=secret, passphrase=passphrase)


async def create_polymarket_client(
    private_key: str | None = None,
    *,
    wallet_config: Any = None,
    config: Any = None,
    authenticated: bool = True,
) -> PolymarketClient:
    """Create a public client and, when key material exists, a secure client.

    Set ``authenticated=False`` for paper runtimes so configured key material
    cannot trigger credential derivation or create a live-capable transport.
    Live startup requires ``secure_client`` and validates the SDK's detected
    wallet and chain before an order can be submitted.
    """
    resolved_key = str(
        private_key
        or getattr(wallet_config, "private_key", "")
        or getattr(getattr(config, "wallet", None), "private_key", "")
        or ""
    ).strip()
    wallet = str(
        getattr(wallet_config, "funder_address", "")
        or getattr(getattr(config, "wallet", None), "funder_address", "")
        or ""
    ).strip()

    public_client = AsyncPublicClient()
    if not authenticated or not resolved_key:
        return PolymarketClient(public_client, wallet_config=wallet_config)

    try:
        secure_client = await AsyncSecureClient.create(
            private_key=resolved_key,
            wallet=wallet or None,
            credentials=_configured_credentials(config),
        )
    except BaseException:
        await public_client.close()
        raise

    return PolymarketClient(
        public_client,
        secure_client=secure_client,
        wallet_config=wallet_config,
    )


__all__ = [
    "MarketMeta",
    "PolymarketClient",
    "create_polymarket_client",
]
