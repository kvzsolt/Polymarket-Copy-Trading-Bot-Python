"""Lifecycle-safe facade over the official unified Polymarket Python SDK.

Only this module constructs SDK clients.  The rest of the application talks to
``ClobGateway`` domain types so beta SDK models and credentials cannot leak into
the money path.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal, cast

from polymarket import (
    ApiKeyCreds,
    AsyncPublicClient,
    AsyncSecureClient,
    RequestRejectedError,
)

from config.env import SignatureType, secret_value, signature_type_from_wallet_type
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

    @property
    def wallet_address(self) -> str | None:
        """SDK-resolved trading wallet address, never a config guess."""
        if self.secure_client is None:
            return None
        wallet = getattr(self.secure_client, "wallet", None)
        return str(wallet) if wallet is not None else None

    @property
    def wallet_type(self) -> str | None:
        """SDK-classified wallet type, authoritative over config hints."""
        if self.secure_client is None:
            return None
        wallet_type = getattr(self.secure_client, "wallet_type", None)
        return str(wallet_type) if wallet_type is not None else None

    @property
    def signature_type(self) -> SignatureType | None:
        """Compatibility signature value mapped from the SDK wallet type."""
        return signature_type_from_wallet_type(self.wallet_type)

    def require_secure(self) -> AsyncSecureClient:
        if self.secure_client is None:
            raise RuntimeError("authenticated Polymarket client is unavailable in paper mode")
        return self.secure_client

    async def get_order_book(self, token_id: str) -> Any:
        return await self.public_client.get_order_book(token_id=token_id)

    async def get_price(self, token_id: str, side: str) -> dict[str, str] | object | None:
        """Compatibility shape for the risk monitor while it adopts the port."""
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            return None
        try:
            value = await self.public_client.get_price(
                token_id=token_id,
                side=cast(Literal["BUY", "SELL"], normalized_side),
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
        clients: list[AsyncSecureClient | AsyncPublicClient] = [self.public_client]
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
    key_material: Any = private_key
    if not key_material:
        key_material = getattr(wallet_config, "private_key", "")
    if not secret_value(key_material):
        key_material = getattr(getattr(config, "wallet", None), "private_key", "")
    resolved_key = secret_value(key_material).strip()
    # The supplied ``wallet_config`` is the authoritative identity for this
    # client. Falling back to the global ``config.wallet.funder_address`` here
    # cross-checked a multi-user entry's own SDK-derived Deposit Wallet against
    # a *different* user's funder and failed startup. A multi-user entry may
    # legitimately omit its funder (``WalletConfig.funder_address`` defaults to
    # "") and let its own key derive the wallet — ``bind_resolved_deposit_wallet``
    # then binds the resolved address back. The single-user path keeps its
    # cross-check because ``load_runtime_users`` copies the configured funder
    # into that user's own ``WalletConfig``.
    if wallet_config is not None:
        configured_wallet = str(getattr(wallet_config, "funder_address", "") or "").strip()
    else:
        configured_wallet = str(
            getattr(getattr(config, "wallet", None), "funder_address", "") or ""
        ).strip()

    public_client = AsyncPublicClient()
    if not authenticated or not resolved_key:
        return PolymarketClient(public_client, wallet_config=wallet_config)

    try:
        # One secret drives the complete official auth chain. The SDK uses the
        # EOA private key for L1 create-or-derive, owns the resulting L2
        # credentials in memory, derives the default Deposit Wallet and maps
        # it to POLY_1271/signature type 3.
        secure_client = await AsyncSecureClient.create(
            private_key=resolved_key,
        )
        resolved_wallet = str(secure_client.wallet)
        if str(secure_client.wallet_type) != "DEPOSIT_WALLET":
            raise RuntimeError(
                "authenticated wallet must resolve to DEPOSIT_WALLET (POLY_1271/signature type 3)"
            )
        if configured_wallet and configured_wallet.lower() != resolved_wallet.lower():
            raise ValueError(
                "configured wallet.funder_address does not match the Deposit "
                "Wallet resolved by the unified SDK"
            )
    except BaseException:
        if "secure_client" in locals():
            await secure_client.close()
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
