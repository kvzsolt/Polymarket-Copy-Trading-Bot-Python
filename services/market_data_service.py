"""Freshness-aware market data service (issue #68).

Single entry-point for execution to obtain quote, book, tick, and metadata.
Guarantees:
  - Data is served from the WSS-fed ``PositionPriceCache`` when fresh.
  - Falls back to REST via the ``ClobGateway`` when cache is stale/missing.
  - Fails **closed** (raises ``StaleDataError``) when REST also fails.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from services.price_cache import (
    BookSnapshot,
    PositionPriceCache,
    QuoteSnapshot,
    TickSnapshot,
)
from utils.logger import get_logger

if TYPE_CHECKING:
    from interfaces.clob_gateway import ClobGateway

logger = get_logger("MarketDataService")


class StaleDataError(Exception):
    """Raised when market data is stale or missing and REST fallback fails."""


@dataclass(frozen=True)
class MarketDataBundle:
    """Complete market data snapshot for a single token."""
    token_id: str
    quote: QuoteSnapshot
    book: BookSnapshot
    tick: TickSnapshot


class MarketDataService:
    """Freshness-aware market data accessor.

    Execution code should call ``require_fresh_data(token_id)`` to get a
    ``MarketDataBundle`` guaranteed to have fresh quote, book, and tick data.
    If any component is stale/missing, a REST fallback fetch is attempted.
    If REST also fails, ``StaleDataError`` is raised (fail-closed).
    """

    def __init__(
        self,
        cache: PositionPriceCache,
        gateway: ClobGateway,
    ) -> None:
        self._cache = cache
        self._gateway = gateway

    async def require_fresh_quote(self, token_id: str) -> QuoteSnapshot:
        """Return a fresh quote, falling back to REST if needed.

        Raises ``StaleDataError`` if data cannot be obtained.
        """
        quote = self._cache.get_quote(token_id)
        if quote is not None:
            return quote
        return await self._rest_fallback_quote(token_id)

    async def require_fresh_book(self, token_id: str) -> BookSnapshot:
        """Return a fresh book, falling back to REST if needed.

        Raises ``StaleDataError`` if data cannot be obtained.
        """
        book = self._cache.get_book(token_id)
        if book is not None:
            return book
        return await self._rest_fallback_book(token_id)

    async def require_fresh_tick(self, token_id: str) -> TickSnapshot:
        """Return fresh tick metadata, falling back to REST if needed.

        Raises ``StaleDataError`` if data cannot be obtained.
        """
        tick = self._cache.get_tick(token_id)
        if tick is not None:
            return tick
        return await self._rest_fallback_tick(token_id)

    async def require_fresh_data(self, token_id: str) -> MarketDataBundle:
        """Return a complete fresh data bundle for a token.

        This is the primary API for execution. Ensures quote, book, and tick
        are all fresh. Raises ``StaleDataError`` if any component cannot be
        obtained.
        """
        quote = await self.require_fresh_quote(token_id)
        book = await self.require_fresh_book(token_id)
        tick = await self.require_fresh_tick(token_id)
        return MarketDataBundle(
            token_id=token_id,
            quote=quote,
            book=book,
            tick=tick,
        )

    async def get_quote_or_none(self, token_id: str) -> QuoteSnapshot | None:
        """Best-effort quote fetch; returns None instead of raising."""
        try:
            return await self.require_fresh_quote(token_id)
        except StaleDataError:
            return None

    async def get_book_or_none(self, token_id: str) -> BookSnapshot | None:
        """Best-effort book fetch; returns None instead of raising."""
        try:
            return await self.require_fresh_book(token_id)
        except StaleDataError:
            return None

    # --- REST fallbacks (private) ---------------------------------------------

    async def _rest_fallback_quote(self, token_id: str) -> QuoteSnapshot:
        """Fetch quote via REST and populate cache."""
        try:
            book = await self._gateway.get_orderbook(token_id)
            best_bid = book.best_bid
            best_ask = book.best_ask
            if best_bid is None and best_ask is None:
                raise StaleDataError(
                    f"REST fallback returned empty book for {token_id}"
                )
            self._cache.set_quote(token_id, best_bid=best_bid, best_ask=best_ask)
            quote = self._cache.get_quote(token_id)
            if quote is None:
                raise StaleDataError(
                    f"Quote still missing after REST fallback for {token_id}"
                )
            logger.info("REST fallback: quote refreshed for %s", token_id)
            return quote
        except StaleDataError:
            raise
        except Exception as exc:
            raise StaleDataError(
                f"REST fallback failed for quote {token_id}: {exc}"
            ) from exc

    async def _rest_fallback_book(self, token_id: str) -> BookSnapshot:
        """Fetch order book via REST and populate cache."""
        try:
            book = await self._gateway.get_orderbook(token_id)
            bids: dict[Decimal, Decimal] = {}
            asks: dict[Decimal, Decimal] = {}
            for level in book.bids:
                bids[level.price] = level.size
            for level in book.asks:
                asks[level.price] = level.size
            self._cache.set_book(token_id, bids=bids, asks=asks)
            snap = self._cache.get_book(token_id)
            if snap is None:
                raise StaleDataError(
                    f"Book still missing after REST fallback for {token_id}"
                )
            logger.info("REST fallback: book refreshed for %s", token_id)
            return snap
        except StaleDataError:
            raise
        except Exception as exc:
            raise StaleDataError(
                f"REST fallback failed for book {token_id}: {exc}"
            ) from exc

    async def _rest_fallback_tick(self, token_id: str) -> TickSnapshot:
        """Fetch tick/metadata via REST and populate cache."""
        try:
            info = await self._gateway.get_market_info(token_id)
            tick_size = Decimal(str(info.get("tick_size", "0")))
            min_order_size = Decimal(str(info.get("min_order_size", "0")))
            neg_risk = bool(info.get("neg_risk", False))
            if tick_size <= 0:
                tick_size = Decimal("0.01")
            self._cache.set_tick(
                token_id,
                tick_size=tick_size,
                min_order_size=min_order_size,
                neg_risk=neg_risk,
            )
            snap = self._cache.get_tick(token_id)
            if snap is None:
                raise StaleDataError(
                    f"Tick still missing after REST fallback for {token_id}"
                )
            logger.info("REST fallback: tick refreshed for %s", token_id)
            return snap
        except StaleDataError:
            raise
        except Exception as exc:
            raise StaleDataError(
                f"REST fallback failed for tick {token_id}: {exc}"
            ) from exc
