"""Freshness-aware market data service.

Single production entry point for execution/sizing/risk to obtain executable
market data for one Polymarket token.

Design (PR10.4):
  - When ``policy.primary_source == "rest_snapshot"`` (default), copy-trade
    execution calls :meth:`MarketDataService.require_execution_snapshot` which
    fetches a fresh REST `/book` immediately before sizing, validates it
    against millisecond budgets and book-quality gates, and writes it into
    :class:`MarketStateStore` for downstream sizing/execution.
  - When ``policy.primary_source == "wss"``, the legacy WSS-first path is
    used: :meth:`require_fresh_data` validates the WSS-fed cache and falls
    back to REST resync if explicitly allowed.
  - Fail closed when state is stale, dirty, missing, resolved, or incomplete.

Polymarket market data is keyed by token/asset id. Do not key this cache only by
condition id because YES and NO tokens have separate books.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from domain.units import BASIS_POINTS_PER_UNIT

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
    """Base market-data error.

    Carries an optional ``skip_code`` so callers (e.g. the trade pipeline)
    can map a rejection back to a structured ``log_trade_skip`` reason
    without re-deriving it from the message string.
    """

    def __init__(self, *args: Any, skip_code: str | None = None) -> None:
        super().__init__(*args)
        self.skip_code: str | None = skip_code


class StaleDataError(MarketDataError):
    """Raised when market data is stale or missing and cannot be refreshed."""


class MarketDataNotReady(MarketDataError):
    """Raised when market state is intentionally not executable."""


class MarketDataSource(str, Enum):
    """Identify the bounded source of a market-data observation."""

    WSS = "wss"
    REST_SNAPSHOT = "rest_snapshot"
    REST_DEGRADED = "rest_degraded"
    REST_BOOTSTRAP = "rest_bootstrap"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class MarketDataPolicy:
    """Define freshness and quality limits for market data."""

    max_quote_age_seconds: float = 2.0
    max_book_age_seconds: float = 2.0
    max_tick_age_seconds: float = 300.0
    require_wss_fresh: bool = True
    allow_rest_fallback: bool = False
    allow_rest_snapshot_for_execution: bool = False
    allow_rest_bootstrap_for_execution: bool = False
    rest_bootstrap_max_age_ms: int = 500
    require_book_depth: bool = True
    # PR10.4: REST snapshot as primary execution source.
    primary_source: str = "rest_snapshot"
    rest_snapshot_max_age_ms: int = 500
    rest_snapshot_request_timeout_ms: int = 750
    rest_snapshot_total_budget_ms: int = 1000
    rest_snapshot_max_spread_bps: int = 300
    # A WSS-sourced quote/book past its own age window is still executable
    # while the WSS connection is proven live within this silence window. A
    # quiet book is not a stale book; a dead connection is. Set <= 0 to
    # disable the liveness path and require strict per-snapshot age.
    wss_connection_max_silence_seconds: float = 20.0


@dataclass(frozen=True)
class MarketDataBundle:
    """Hold one immutable normalized market-data snapshot."""

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
        is_wss_down_fn: Callable[[], bool] | None = None,
    ) -> None:
        self._cache = cache
        self._gateway = gateway
        self._policy = policy or MarketDataPolicy()
        self._is_wss_down_fn = is_wss_down_fn

    async def require_fresh_data(
        self,
        token_id: str,
        *,
        policy: MarketDataPolicy | None = None,
        side: str | None = None,
        condition_id: str | None = None,
    ) -> MarketDataBundle:
        """Return a fresh complete market-data snapshot or fail closed."""
        active_policy = policy or self._policy

        if active_policy.primary_source == "rest_snapshot" and side is not None:
            # REST is the prefetch / bootstrap source: the pipeline takes one
            # fresh REST `/book` before the first order and then subscribes the
            # token to the market WSS. From that point the WSS stream is the
            # primary source — so prefer the live WSS-fed cache here and only
            # fetch a fresh REST snapshot when the cache is not yet executable.
            #
            # ``_bundle_from_cache`` returns a bundle only when quote+book are
            # WSS-fresh (or a still-fresh REST snapshot bridging the gap before
            # the first WSS delta lands); a cold/stale cache returns ``None``
            # and we fall through to a gated REST fetch. This keeps "REST
            # prefetch, WSS after subscription" while staying fail-closed, and
            # it uses the same execution gates the WSS-primary path already
            # applies. (A resolved market raises MarketDataNotReady and
            # propagates — we must not trade it.)
            cached = self._bundle_from_cache(token_id, active_policy)
            if cached is not None:
                return cached
            return await self.require_execution_snapshot(
                token_id,
                side=side,
                condition_id=condition_id,
                policy=active_policy,
            )

        bundle = self._bundle_from_cache(token_id, active_policy)
        if bundle is not None:
            return bundle

        if not active_policy.allow_rest_fallback:
            debug = ""
            get_debug = getattr(self._cache, "get_debug_state", None)
            if callable(get_debug):
                try:
                    d = get_debug(token_id)
                    if isinstance(d, dict):
                        debug = (
                            f"; quote_source={d.get('quote_source')}"
                            f" book_source={d.get('book_source')}"
                            f" has_book={d.get('has_book')}"
                            f" bid_count={d.get('bid_count')}"
                            f" ask_count={d.get('ask_count')}"
                            f" active={d.get('active')}"
                            f" resolved={d.get('resolved')}"
                        )
                except Exception:
                    pass
            raise StaleDataError(
                f"market data not executable for {token_id}; REST fallback disabled{debug}",
                skip_code="market_data_stale",
            )

        # REST fallback is configured. Only enter degraded path when WSS is
        # actually globally down — not merely stale for this one token.
        wss_down = self._is_wss_down_fn() if self._is_wss_down_fn is not None else False
        if not wss_down:
            raise StaleDataError(
                f"market data not executable for {token_id}; "
                "REST fallback configured but market WSS is healthy",
                skip_code="market_data_stale",
            )

        logger.warning("market_data_rest_degraded_enabled | token_id=%s", token_id)

        await self._rest_resync(token_id)

        bundle = self._bundle_from_cache(token_id, active_policy)
        if bundle is None:
            raise StaleDataError(
                f"market data still not executable after REST resync for {token_id}",
                skip_code="market_data_stale",
            )

        if (
            active_policy.require_wss_fresh
            and not active_policy.allow_rest_snapshot_for_execution
            and bundle.source not in (MarketDataSource.WSS, MarketDataSource.REST_DEGRADED)
        ):
            raise StaleDataError(
                f"market data for {token_id} came from {bundle.source}; WSS fresh required",
                skip_code="market_data_stale",
            )

        logger.warning(
            "market_data_rest_degraded_used | token_id=%s source=%s", token_id, bundle.source
        )
        return bundle

    async def require_execution_snapshot(
        self,
        token_id: str,
        *,
        side: str,
        condition_id: str | None = None,
        policy: MarketDataPolicy | None = None,
    ) -> MarketDataBundle:
        """Fetch a fresh REST `/book` snapshot and gate it for execution.

        Used as the primary execution market-data path when
        ``policy.primary_source == "rest_snapshot"``. Everything is measured
        on :func:`time.monotonic` so wall-clock drift cannot inflate the
        freshness budget.

        Raises:
            MarketDataNotReady: token is resolved/inactive
                (``skip_code="market_resolved"``).
            StaleDataError: REST fetch timed out, errored, or returned a
                book that fails the gate. ``skip_code`` is one of
                ``market_data_stale``, ``market_data_timeout``,
                ``book_no_liquidity``, ``book_spread_wide``.
        """
        active_policy = policy or self._policy
        normalized_side = self._normalize_side(side)

        if self._is_resolved(token_id):
            raise MarketDataNotReady(
                f"market/token resolved: {token_id}",
                skip_code="market_resolved",
            )

        t_start = time.monotonic()
        max_age_ms = int(active_policy.rest_snapshot_max_age_ms)
        request_timeout_s = active_policy.rest_snapshot_request_timeout_ms / 1000.0
        total_budget_ms = int(active_policy.rest_snapshot_total_budget_ms)
        max_spread_bps = int(active_policy.rest_snapshot_max_spread_bps)

        logger.info(
            "market_data_rest_snapshot_start | token_id=%s cond=%s side=%s",
            token_id,
            condition_id,
            normalized_side,
        )

        fetch_start = time.monotonic()
        try:
            book_payload = await asyncio.wait_for(
                self._gateway.get_orderbook(token_id),
                timeout=request_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            logger.warning(
                "market_data_rest_snapshot_timeout | token_id=%s timeout_ms=%d",
                token_id,
                active_policy.rest_snapshot_request_timeout_ms,
            )
            raise StaleDataError(
                f"REST snapshot timeout for {token_id}",
                skip_code="market_data_timeout",
            ) from exc
        except Exception as exc:
            logger.warning(
                "market_data_rest_snapshot_rejected | token_id=%s reason=fetch_error error=%r",
                token_id,
                exc,
            )
            raise StaleDataError(
                f"REST snapshot fetch failed for {token_id}: {exc!r}",
                skip_code="market_data_stale",
            ) from exc

        request_ms = (time.monotonic() - fetch_start) * 1000.0
        # ``fetched_at`` anchors the freshness gate. Pre-fix this was set
        # AFTER the book fetch completed, which made ``data_age_ms``
        # measure "post-fetch processing latency" (always sub-millisecond)
        # rather than "how old is the book by the time we use it". The
        # anchor must be ``fetch_start`` so that a slow network round-trip
        # actually counts against the freshness budget.
        fetched_at = fetch_start

        bids: dict[Decimal, Decimal] = {}
        asks: dict[Decimal, Decimal] = {}
        for level in getattr(book_payload, "bids", []) or []:
            price = _to_decimal(getattr(level, "price", None))
            size = _to_decimal(getattr(level, "size", None))
            if price is not None and size is not None and size > 0:
                bids[price] = size
        for level in getattr(book_payload, "asks", []) or []:
            price = _to_decimal(getattr(level, "price", None))
            size = _to_decimal(getattr(level, "size", None))
            if price is not None and size is not None and size > 0:
                asks[price] = size

        if not bids and not asks:
            logger.warning(
                "market_data_rest_snapshot_rejected | token_id=%s reason=empty_book",
                token_id,
            )
            raise StaleDataError(
                f"REST snapshot empty book for {token_id}",
                skip_code="market_data_stale",
            )

        if normalized_side == "BUY" and not asks:
            logger.warning(
                "market_data_rest_snapshot_rejected | token_id=%s reason=no_ask_liquidity",
                token_id,
            )
            raise StaleDataError(
                f"REST snapshot has no ask liquidity for BUY on {token_id}",
                skip_code="book_no_liquidity",
            )
        if normalized_side == "SELL" and not bids:
            logger.warning(
                "market_data_rest_snapshot_rejected | token_id=%s reason=no_bid_liquidity",
                token_id,
            )
            raise StaleDataError(
                f"REST snapshot has no bid liquidity for SELL on {token_id}",
                skip_code="book_no_liquidity",
            )

        best_bid = max(bids) if bids else None
        best_ask = min(asks) if asks else None
        spread_bps: int | None = None
        if best_bid is not None and best_ask is not None:
            if best_bid >= best_ask or best_ask <= 0:
                logger.warning(
                    "market_data_rest_snapshot_rejected | token_id=%s reason=invalid_spread"
                    " best_bid=%s best_ask=%s",
                    token_id,
                    best_bid,
                    best_ask,
                )
                raise StaleDataError(
                    f"REST snapshot invalid spread for {token_id}",
                    skip_code="market_data_stale",
                )
            spread_bps = int(((best_ask - best_bid) / best_ask) * BASIS_POINTS_PER_UNIT)
            if max_spread_bps > 0 and spread_bps > max_spread_bps:
                # FAIL CLOSED. Pre-fix this gate logged "allowed" and
                # let the order proceed — orders could land in 1000bps
                # books while the configured cap was 300bps. The
                # ``rest_snapshot_max_spread_bps`` config exists
                # specifically to refuse trade-through against
                # illiquid / manipulated books, so log + raise.
                logger.warning(
                    "market_data_rest_snapshot_rejected | token_id=%s reason=wide_spread"
                    " spread_bps=%d configured_max_bps=%d",
                    token_id,
                    spread_bps,
                    max_spread_bps,
                )
                raise StaleDataError(
                    f"REST snapshot spread {spread_bps}bps exceeds "
                    f"max {max_spread_bps}bps for {token_id}",
                    skip_code="book_spread_wide",
                )

        data_age_ms = (time.monotonic() - fetched_at) * 1000.0
        if data_age_ms > max_age_ms:
            logger.warning(
                "market_data_rest_snapshot_rejected | token_id=%s reason=stale"
                " age_ms=%.1f max_ms=%d",
                token_id,
                data_age_ms,
                max_age_ms,
            )
            raise StaleDataError(
                f"REST snapshot age {data_age_ms:.1f}ms exceeds {max_age_ms}ms for {token_id}",
                skip_code="market_data_stale",
            )

        total_elapsed_ms = (time.monotonic() - t_start) * 1000.0
        if total_elapsed_ms > total_budget_ms:
            logger.warning(
                "market_data_rest_snapshot_timeout | token_id=%s reason=total_budget_exceeded"
                " elapsed_ms=%.1f budget_ms=%d",
                token_id,
                total_elapsed_ms,
                total_budget_ms,
            )
            raise StaleDataError(
                f"REST snapshot total budget exceeded for {token_id}",
                skip_code="market_data_timeout",
            )

        # The unified SDK's typed OrderBook is the canonical metadata source.
        # Do not merge partial book metadata with a cache or a second endpoint:
        # that can create a combination which never existed atomically at the
        # venue, and missing neg_risk must never silently become False.
        tick_size = _to_decimal(
            getattr(book_payload, "tick_size", None)
            or getattr(book_payload, "minimum_tick_size", None)
        )
        min_order_size = _to_decimal(
            getattr(book_payload, "min_order_size", None)
            or getattr(book_payload, "minimum_order_size", None)
            or getattr(book_payload, "order_min_size", None)
        )
        neg_risk = getattr(book_payload, "neg_risk", None)
        if (
            tick_size is None
            or tick_size <= 0
            or min_order_size is None
            or min_order_size <= 0
            or not isinstance(neg_risk, bool)
        ):
            logger.warning(
                "market_data_rest_snapshot_rejected | token_id=%s reason=invalid_metadata",
                token_id,
            )
            raise StaleDataError(
                f"REST snapshot missing valid execution metadata for {token_id}",
                skip_code="market_data_stale",
            )

        self._cache.store_rest_snapshot(
            token_id,
            condition_id=condition_id,
            bids=bids,
            asks=asks,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=neg_risk,
            fetched_at_monotonic=fetched_at,
        )

        quote = self._cache.get_quote(token_id)
        book = self._cache.get_book(token_id)
        tick = self._cache.get_tick(token_id)
        if quote is None or book is None or tick is None:
            logger.warning(
                "market_data_rest_snapshot_rejected | token_id=%s reason=missing_components"
                " has_quote=%s has_book=%s has_tick=%s",
                token_id,
                quote is not None,
                book is not None,
                tick is not None,
            )
            raise StaleDataError(
                f"REST snapshot missing components for {token_id}",
                skip_code="market_data_stale",
            )

        logger.info(
            "market_data_rest_snapshot_used | token_id=%s cond=%s side=%s"
            " request_ms=%.1f data_age_ms=%.1f total_elapsed_ms=%.1f"
            " bid_count=%d ask_count=%d best_bid=%s best_ask=%s spread_bps=%s"
            " source=rest_snapshot",
            token_id,
            condition_id,
            normalized_side,
            request_ms,
            data_age_ms,
            total_elapsed_ms,
            len(bids),
            len(asks),
            best_bid,
            best_ask,
            spread_bps,
        )

        return MarketDataBundle(
            token_id=token_id,
            quote=quote,
            book=book,
            tick=tick,
            source=MarketDataSource.REST_SNAPSHOT,
            reason="rest_snapshot",
        )

    @staticmethod
    def _normalize_side(side: str | None) -> str:
        value = str(side or "").upper()
        if value in {"BUY", "BID"}:
            return "BUY"
        if value in {"SELL", "ASK"}:
            return "SELL"
        return "BUY"

    async def require_fresh_quote(self, token_id: str) -> QuoteSnapshot:
        """Return a fresh quote or fail closed."""
        return (await self.require_fresh_data(token_id)).quote

    async def require_fresh_book(self, token_id: str) -> BookSnapshot:
        """Return a fresh order book or fail closed."""
        return (await self.require_fresh_data(token_id)).book

    async def require_fresh_tick(self, token_id: str) -> TickSnapshot:
        """Return a fresh authoritative tick size or fail closed."""
        return (await self.require_fresh_data(token_id)).tick

    async def get_quote_or_none(self, token_id: str) -> QuoteSnapshot | None:
        """Return a fresh quote when available without fabricating a value."""
        try:
            return await self.require_fresh_quote(token_id)
        except MarketDataError:
            return None

    async def get_book_or_none(self, token_id: str) -> BookSnapshot | None:
        """Return a fresh order book when available."""
        try:
            return await self.require_fresh_book(token_id)
        except MarketDataError:
            return None

    async def get_tick_or_none(self, token_id: str) -> TickSnapshot | None:
        """Return a fresh authoritative tick size when available."""
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
            raise MarketDataNotReady(
                f"market/token resolved: {token_id}",
                skip_code="market_resolved",
            )

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

        wss_fresh = quote_source == MarketDataSource.WSS and book_source == MarketDataSource.WSS
        rest_bootstrap_ok = False
        rest_snapshot_ok = False

        if not wss_fresh:
            if (
                policy.allow_rest_bootstrap_for_execution
                and quote_source == MarketDataSource.REST_BOOTSTRAP
                and book_source == MarketDataSource.REST_BOOTSTRAP
            ):
                quote_age_ms = (time.monotonic() - quote.monotonic_ts) * 1000
                book_age_ms = (time.monotonic() - book.monotonic_ts) * 1000
                within_cap = (
                    quote_age_ms <= policy.rest_bootstrap_max_age_ms
                    and book_age_ms <= policy.rest_bootstrap_max_age_ms
                )
                # REST bootstrap is the current state of the book until WSS
                # sends a delta. While the WSS connection is proven live, a
                # quiet stream is not a stale book — same rule as WSS-sourced
                # silence path. Without WSS liveness, fall back to the strict
                # bootstrap age cap.
                if within_cap or self._wss_connection_live(
                    policy.wss_connection_max_silence_seconds
                ):
                    rest_bootstrap_ok = True

            # REST snapshot freshness path: active only when the policy
            # designates rest_snapshot as primary. Both components must be
            # rest_snapshot-sourced and within the millisecond age cap.
            # Used when WSS has not yet upgraded the cache (e.g. in the
            # window between a REST fetch and the first WSS delta landing).
            if (
                not rest_bootstrap_ok
                and policy.allow_rest_snapshot_for_execution
                and quote_source == MarketDataSource.REST_SNAPSHOT
                and book_source == MarketDataSource.REST_SNAPSHOT
            ):
                quote_age_ms = (time.monotonic() - quote.monotonic_ts) * 1000.0
                book_age_ms = (time.monotonic() - book.monotonic_ts) * 1000.0
                if (
                    quote_age_ms <= policy.rest_snapshot_max_age_ms
                    and book_age_ms <= policy.rest_snapshot_max_age_ms
                ):
                    rest_snapshot_ok = True

            if not rest_bootstrap_ok and not rest_snapshot_ok:
                if policy.require_wss_fresh or policy.allow_rest_snapshot_for_execution:
                    return None

        # Aggregate bundle source for downstream observability.
        if wss_fresh:
            bundle_source = MarketDataSource.WSS
        elif rest_bootstrap_ok:
            bundle_source = MarketDataSource.REST_BOOTSTRAP
        elif rest_snapshot_ok:
            bundle_source = MarketDataSource.REST_SNAPSHOT
        elif (
            quote_source == MarketDataSource.REST_DEGRADED
            or book_source == MarketDataSource.REST_DEGRADED
        ):
            bundle_source = MarketDataSource.REST_DEGRADED
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
        return self._snapshot_fresh(quote, policy.max_quote_age_seconds, policy)

    def _book_executable(self, book: BookSnapshot, policy: MarketDataPolicy) -> bool:
        bids = getattr(book, "bids", None)
        asks = getattr(book, "asks", None)
        if policy.require_book_depth and (not bids or not asks):
            return False

        best_bid = getattr(book, "best_bid", None)
        best_ask = getattr(book, "best_ask", None)
        if best_bid is not None and best_ask is not None and best_bid >= best_ask:
            return False

        return self._snapshot_fresh(book, policy.max_book_age_seconds, policy)

    def _snapshot_fresh(
        self,
        snapshot: Any,
        max_age_seconds: float,
        policy: MarketDataPolicy,
    ) -> bool:
        """Freshness for a quote/book snapshot.

        Fast path: the snapshot's own timestamp is within ``max_age_seconds``.
        Liveness path: a WSS-sourced snapshot past that window is still the
        current state while the WSS connection is proven live. Illiquid markets
        legitimately go many seconds without a delta — silence on a healthy
        stream is not staleness. A dead stream fails both paths and is rejected.
        """
        if self._fresh_enough(snapshot, max_age_seconds):
            return True

        if policy.wss_connection_max_silence_seconds <= 0:
            return False
        snap_src = self._coerce_source(getattr(snapshot, "source", None))
        if snap_src not in (MarketDataSource.WSS, MarketDataSource.REST_BOOTSTRAP):
            return False
        return self._wss_connection_live(policy.wss_connection_max_silence_seconds)

    def _wss_connection_live(self, max_silence_seconds: float) -> bool:
        fn = getattr(self._cache, "wss_connection_age_seconds", None)
        if not callable(fn):
            return False
        try:
            age = fn()
        except Exception:
            return False
        if isinstance(age, bool) or not isinstance(age, (int, float)):
            return False
        return age <= max_silence_seconds

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

        for name in (
            "last_update_monotonic",
            "updated_monotonic",
            "created_monotonic",
            "monotonic_ts",
        ):
            value = getattr(snapshot, name, None)
            if value is not None:
                try:
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
            raise StaleDataError(
                f"REST returned empty/incomplete book for {token_id}",
                skip_code="market_data_stale",
            )

        if not self._replace_book_snapshot(token_id, bids, asks):
            self._cache.set_book(token_id, bids=bids, asks=asks, source="rest_snapshot")

        best_bid = max(bids)
        best_ask = min(asks)
        self._cache.set_quote(
            token_id, best_bid=best_bid, best_ask=best_ask, source="rest_snapshot"
        )

        info = await self._gateway.get_market_info(token_id)
        tick_size = _to_decimal(info.get("tick_size") or info.get("minimum_tick_size"))
        min_order_size = _to_decimal(
            info.get("min_order_size")
            or info.get("minimum_order_size")
            or info.get("order_min_size")
        )
        neg_risk = info.get("neg_risk")

        if tick_size is None or tick_size <= 0:
            self._mark_dirty(token_id, "rest_tick_missing")
            raise StaleDataError(
                f"REST missing valid tick_size for {token_id}",
                skip_code="market_data_stale",
            )

        if min_order_size is None or min_order_size <= 0:
            self._mark_dirty(token_id, "rest_min_order_missing")
            raise StaleDataError(
                f"REST missing valid min_order_size for {token_id}",
                skip_code="market_data_stale",
            )

        if not isinstance(neg_risk, bool):
            self._mark_dirty(token_id, "rest_neg_risk_missing")
            raise StaleDataError(
                f"REST missing valid neg_risk for {token_id}",
                skip_code="market_data_stale",
            )

        self._cache.set_tick(
            token_id,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=neg_risk,
            source="rest_snapshot",
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
            method(token_id, source.value, time.monotonic())


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
