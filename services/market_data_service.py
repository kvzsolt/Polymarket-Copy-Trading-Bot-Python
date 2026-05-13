"""Freshness-aware market data service.

Single production entry point for execution/sizing/risk to obtain executable
market data for one Polymarket token.

Design:
  - Prefer WSS-fed MarketStateStore.
  - Fail closed when state is stale, dirty, missing, resolved, or incomplete.
  - Optionally allow REST bootstrap/resync fallback.
  - In live mode, prefer `require_wss_fresh=True` so REST cannot silently mask a
    dead market WebSocket.

Polymarket market data is keyed by token/asset id. Do not key this cache only by
condition id because YES and NO tokens have separate books.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import TYPE_CHECKING, Any

from cache.market_state_store import (
    BookSnapshot,
    MarketStateStore,
    QuoteSnapshot,
    TickSnapshot,
)
from utils.logger import get_logger

if TYPE_CHECKING:
    from interfaces.clob_gateway import ClobGateway

logger = get_logger("MarketDataService")


class MarketDataError(Exception):
    """Base market-data error."""


class StaleDataError(MarketDataError):
    """Raised when market data is stale or missing and cannot be refreshed."""


class MarketDataNotReady(MarketDataError):
    """Raised when market state is intentionally not executable."""


class MarketDataSource(str, Enum):
    WSS = "wss"
    REST_SNAPSHOT = "rest_snapshot"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class MarketDataPolicy:
    max_quote_age_seconds: float = 2.0
    max_book_age_seconds: float = 2.0
    max_tick_age_seconds: float = 300.0
    require_wss_fresh: bool = True
    allow_rest_fallback: bool = False
    allow_rest_snapshot_for_execution: bool = False
    require_book_depth: bool = True


@dataclass(frozen=True)
class MarketDataBundle:
    token_id: str
    quote: QuoteSnapshot
    book: BookSnapshot
    tick: TickSnapshot
    source: MarketDataSource
    reason: str = "fresh"


class MarketDataService:
    """Fail-closed market-data accessor.

    Execution should call require_fresh_data(token_id). It returns quote, book,
    and tick only when all are executable under MarketDataPolicy.
    """

    def __init__(
        self,
        cache: MarketStateStore,
        gateway: ClobGateway,
        *,
        policy: MarketDataPolicy | None = None,
    ) -> None:
        self._cache = cache
        self._gateway = gateway
        self._policy = policy or MarketDataPolicy()

    async def require_fresh_data(
        self,
        token_id: str,
        *,
        policy: MarketDataPolicy | None = None,
    ) -> MarketDataBundle:
        active_policy = policy or self._policy

        bundle = self._bundle_from_cache(token_id, active_policy)
        if bundle is not None:
            return bundle

        if not active_policy.allow_rest_fallback:
            raise StaleDataError(
                f"market data not executable for {token_id}; REST fallback disabled"
            )

        await self._rest_resync(token_id)

        bundle = self._bundle_from_cache(token_id, active_policy)
        if bundle is None:
            raise StaleDataError(
                f"market data still not executable after REST resync for {token_id}"
            )

        if (
            active_policy.require_wss_fresh
            and not active_policy.allow_rest_snapshot_for_execution
            and bundle.source != MarketDataSource.WSS
        ):
            raise StaleDataError(
                f"market data for {token_id} came from {bundle.source}; WSS fresh required"
            )

        return bundle

    async def require_fresh_quote(self, token_id: str) -> QuoteSnapshot:
        return (await self.require_fresh_data(token_id)).quote

    async def require_fresh_book(self, token_id: str) -> BookSnapshot:
        return (await self.require_fresh_data(token_id)).book

    async def require_fresh_tick(self, token_id: str) -> TickSnapshot:
        return (await self.require_fresh_data(token_id)).tick

    async def get_quote_or_none(self, token_id: str) -> QuoteSnapshot | None:
        try:
            return await self.require_fresh_quote(token_id)
        except MarketDataError:
            return None

    async def get_book_or_none(self, token_id: str) -> BookSnapshot | None:
        try:
            return await self.require_fresh_book(token_id)
        except MarketDataError:
            return None

    async def get_tick_or_none(self, token_id: str) -> TickSnapshot | None:
        try:
            return await self.require_fresh_tick(token_id)
        except MarketDataError:
            return None

    def _bundle_from_cache(
        self,
        token_id: str,
        policy: MarketDataPolicy,
    ) -> MarketDataBundle | None:
        if self._is_resolved(token_id):
            raise MarketDataNotReady(f"market/token resolved: {token_id}")

        if self._is_dirty(token_id):
            return None

        quote = self._cache.get_quote(token_id)
        book = self._cache.get_book(token_id)
        tick = self._cache.get_tick(token_id)

        if quote is None or book is None or tick is None:
            return None
        if not self._quote_executable(quote, policy):
            return None
        if not self._book_executable(book, policy):
            return None
        if not self._tick_executable(tick, policy):
            return None

        # Component-level source gating. Live execution requires quote and book
        # to be WSS-fresh; tick/metadata may be REST-sourced if fresh and valid
        # (Polymarket metadata changes are reflected by ``tick_size_change``
        # but the canonical baseline is REST).
        quote_source = self._coerce_source(quote.source)
        book_source = self._coerce_source(book.source)
        tick_source = self._coerce_source(tick.source)

        if (
            policy.require_wss_fresh
            and not policy.allow_rest_snapshot_for_execution
            and (
                quote_source != MarketDataSource.WSS
                or book_source != MarketDataSource.WSS
            )
        ):
            return None

        # Aggregate bundle source for downstream observability.
        if quote_source == MarketDataSource.WSS and book_source == MarketDataSource.WSS:
            bundle_source = MarketDataSource.WSS
        elif (
            quote_source == MarketDataSource.REST_SNAPSHOT
            or book_source == MarketDataSource.REST_SNAPSHOT
            or tick_source == MarketDataSource.REST_SNAPSHOT
        ):
            bundle_source = MarketDataSource.REST_SNAPSHOT
        else:
            bundle_source = MarketDataSource.UNKNOWN

        return MarketDataBundle(
            token_id=token_id,
            quote=quote,
            book=book,
            tick=tick,
            source=bundle_source,
        )

    @staticmethod
    def _coerce_source(raw: Any) -> MarketDataSource:
        try:
            return MarketDataSource(str(raw))
        except Exception:
            return MarketDataSource.UNKNOWN

    def _quote_executable(self, quote: QuoteSnapshot, policy: MarketDataPolicy) -> bool:
        best_bid = getattr(quote, "best_bid", None)
        best_ask = getattr(quote, "best_ask", None)
        if best_bid is None or best_ask is None:
            return False
        if best_bid <= 0 or best_ask <= 0:
            return False
        if best_bid >= best_ask:
            return False
        return self._fresh_enough(quote, policy.max_quote_age_seconds)

    def _book_executable(self, book: BookSnapshot, policy: MarketDataPolicy) -> bool:
        bids = getattr(book, "bids", None)
        asks = getattr(book, "asks", None)
        if policy.require_book_depth and (not bids or not asks):
            return False

        best_bid = getattr(book, "best_bid", None)
        best_ask = getattr(book, "best_ask", None)
        if best_bid is not None and best_ask is not None and best_bid >= best_ask:
            return False

        return self._fresh_enough(book, policy.max_book_age_seconds)

    def _tick_executable(self, tick: TickSnapshot, policy: MarketDataPolicy) -> bool:
        tick_size = getattr(tick, "tick_size", None)
        min_order_size = getattr(tick, "min_order_size", None)
        if tick_size is None or tick_size <= 0:
            return False
        if min_order_size is None or min_order_size <= 0:
            return False
        return self._fresh_enough(tick, policy.max_tick_age_seconds)

    def _fresh_enough(self, snapshot: Any, max_age_seconds: float) -> bool:
        is_fresh = getattr(snapshot, "is_fresh", None)
        if callable(is_fresh):
            try:
                return bool(is_fresh(max_age_seconds))
            except TypeError:
                return bool(is_fresh())

        is_stale = getattr(snapshot, "is_stale", None)
        if callable(is_stale):
            try:
                return not bool(is_stale(max_age_seconds))
            except TypeError:
                return not bool(is_stale())

        for name in ("last_update_monotonic", "updated_monotonic", "created_monotonic", "monotonic_ts"):
            value = getattr(snapshot, name, None)
            if value is not None:
                try:
                    import time
                    return (time.monotonic() - float(value)) <= max_age_seconds
                except (TypeError, ValueError):
                    return False

        # If snapshots do not carry freshness metadata, fail closed.
        return False

    def _is_dirty(self, token_id: str) -> bool:
        method = getattr(self._cache, "is_dirty", None)
        if callable(method):
            return bool(method(token_id))

        get_state = getattr(self._cache, "get_state", None)
        state = get_state(token_id) if callable(get_state) else None
        return bool(getattr(state, "dirty", False)) if state is not None else False

    def _is_resolved(self, token_id: str) -> bool:
        method = getattr(self._cache, "is_resolved", None)
        if callable(method):
            return bool(method(token_id))

        get_state = getattr(self._cache, "get_state", None)
        state = get_state(token_id) if callable(get_state) else None
        return bool(getattr(state, "resolved", False)) if state is not None else False

    def _source_of(self, token_id: str) -> MarketDataSource:
        method = getattr(self._cache, "source_of", None)
        if callable(method):
            raw = method(token_id)
        else:
            get_state = getattr(self._cache, "get_state", None)
            state = get_state(token_id) if callable(get_state) else None
            raw = getattr(state, "source", None) if state is not None else None

        try:
            return MarketDataSource(str(raw))
        except Exception:
            return MarketDataSource.UNKNOWN

    async def _rest_resync(self, token_id: str) -> None:
        logger.info("REST resync: refreshing market data for %s", token_id)

        book = await self._gateway.get_orderbook(token_id)
        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}

        for level in getattr(book, "bids", []) or []:
            price = _to_decimal(getattr(level, "price", None))
            size = _to_decimal(getattr(level, "size", None))
            if price is not None and size is not None and size > 0:
                bids[price] = size

        for level in getattr(book, "asks", []) or []:
            price = _to_decimal(getattr(level, "price", None))
            size = _to_decimal(getattr(level, "size", None))
            if price is not None and size is not None and size > 0:
                asks[price] = size

        if not bids or not asks:
            self._mark_dirty(token_id, "rest_empty_book")
            raise StaleDataError(f"REST returned empty/incomplete book for {token_id}")

        if not self._replace_book_snapshot(token_id, bids, asks):
            self._cache.set_book(token_id, bids=bids, asks=asks)

        best_bid = max(bids)
        best_ask = min(asks)
        self._cache.set_quote(token_id, best_bid=best_bid, best_ask=best_ask)

        info = await self._gateway.get_market_info(token_id)
        tick_size = _to_decimal(info.get("tick_size") or info.get("minimum_tick_size"))
        min_order_size = _to_decimal(
            info.get("min_order_size")
            or info.get("minimum_order_size")
            or info.get("order_min_size")
        )
        neg_risk = bool(info.get("neg_risk", False))

        if tick_size is None or tick_size <= 0:
            self._mark_dirty(token_id, "rest_tick_missing")
            raise StaleDataError(f"REST missing valid tick_size for {token_id}")

        if min_order_size is None or min_order_size <= 0:
            self._mark_dirty(token_id, "rest_min_order_missing")
            raise StaleDataError(f"REST missing valid min_order_size for {token_id}")

        self._cache.set_tick(
            token_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=neg_risk,
        )
        self._mark_fresh(token_id, MarketDataSource.REST_SNAPSHOT)

    def _replace_book_snapshot(
        self,
        token_id: str,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
    ) -> bool:
        method = getattr(self._cache, "replace_book_snapshot", None)
        if not callable(method):
            return False
        method(token_id, bids, asks, {"source": "rest_snapshot"})
        return True

    def _mark_dirty(self, token_id: str, reason: str) -> None:
        method = getattr(self._cache, "mark_dirty", None)
        if callable(method):
            method(token_id, reason)

    def _mark_fresh(self, token_id: str, source: MarketDataSource) -> None:
        method = getattr(self._cache, "mark_fresh", None)
        if callable(method):
            import time
            method(token_id, source.value, time.monotonic())


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None