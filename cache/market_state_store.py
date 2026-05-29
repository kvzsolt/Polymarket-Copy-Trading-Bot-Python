"""Shared market state store.

Token-keyed cache written by both REST `/book` snapshots and WSS deltas. Under
PR10.4, REST snapshot is the primary execution source: trade-pipeline fetches a
fresh REST book, writes it via :meth:`MarketStateStore.store_rest_snapshot`, and
hands the resulting cached state to sizing/execution. WSS continues to overwrite
component sources to ``"wss"`` when its events land — those upgrades are logged
via ``market_data_rest_snapshot_wss_override`` for observability.

Important:
  - Key this store by token_id, not condition_id.
  - YES and NO tokens under the same condition have separate books.
  - Position state should live elsewhere, keyed by:
        user + condition_id + token_id
  - This store contains market state only:
        book, best bid/ask, tick size, min order size, resolved/stale/dirty flags.
  - Component-level sources (``quote_source``, ``book_source``, ``tick_source``,
    ``metadata_source``) are authoritative for executable freshness checks and
    are only set by the dedicated setters / REST-snapshot helper / WSS handlers.

Used by:
  - clients.market_ws.MarketWebSocket
  - services.market_data_service.MarketDataService
  - services.order_sizing
  - services.executionService
"""

from __future__ import annotations

import logging
import os
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from decimal import Decimal
from threading import RLock
from typing import Any


def _env_int(name: str, default: int) -> int:
    """Read a non-negative int env override, falling back to ``default``."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default

_logger = logging.getLogger("MarketStateStore")


# ---------------------------------------------------------------------------
# Snapshots returned to services
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QuoteSnapshot:
    token_id: str
    best_bid: Decimal | None
    best_ask: Decimal | None
    last_trade_price: Decimal | None
    source: str
    monotonic_ts: float

    def is_fresh(self, max_age_seconds: float) -> bool:
        return time.monotonic() - self.monotonic_ts <= max_age_seconds


@dataclass(frozen=True)
class BookSnapshot:
    token_id: str
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    best_bid: Decimal | None
    best_ask: Decimal | None
    source: str
    monotonic_ts: float

    def is_fresh(self, max_age_seconds: float) -> bool:
        return time.monotonic() - self.monotonic_ts <= max_age_seconds


@dataclass(frozen=True)
class TickSnapshot:
    token_id: str
    tick_size: Decimal
    min_order_size: Decimal
    neg_risk: bool
    source: str
    monotonic_ts: float

    def is_fresh(self, max_age_seconds: float) -> bool:
        return time.monotonic() - self.monotonic_ts <= max_age_seconds


# ---------------------------------------------------------------------------
# Mutable internal state
# ---------------------------------------------------------------------------


@dataclass
class TokenMarketState:
    token_id: str

    condition_id: str | None = None

    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)

    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    last_trade_price: Decimal | None = None

    tick_size: Decimal | None = None
    min_order_size: Decimal | None = None
    neg_risk: bool = False

    active: bool = True
    resolved: bool = False

    # dirty=True means execution must not trade from this token until a fresh
    # authoritative book/tick state is available.
    dirty: bool = True
    stale: bool = True
    dirty_reason: str | None = "not_initialized"
    stale_reason: str | None = "not_initialized"

    # Aggregate ``source`` kept for back-compat with callers that just want a
    # last-touched hint. Component-specific sources are authoritative for
    # executable freshness checks. They are NOT promoted by tick-size changes
    # — a WSS ``tick_size_change`` event must not make a REST-only quote/book
    # appear WSS-fresh.
    source: str = "unknown"
    quote_source: str = "unknown"
    book_source: str = "unknown"
    tick_source: str = "unknown"
    metadata_source: str = "unknown"

    last_update_monotonic: float = 0.0
    last_book_monotonic: float = 0.0
    last_quote_monotonic: float = 0.0
    last_tick_monotonic: float = 0.0
    last_trade_monotonic: float = 0.0

    last_event_raw: dict[str, Any] | None = None

    def recompute_top_of_book(self) -> None:
        self.best_bid = max(self.bids) if self.bids else None
        self.best_ask = min(self.asks) if self.asks else None

    def mark_updated(self) -> None:
        self.last_update_monotonic = time.monotonic()


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class MarketStateStore:
    """Thread-safe/sync token market-state store.

    This class intentionally exposes synchronous methods. Your WSS client calls
    cache hooks from a synchronous message handler, so these methods should not
    require `await`.

    If you later move message handling to fully async code, you can wrap this
    behind an async service, but keeping the store sync is simple and safe.
    """

    def __init__(self, *, max_tokens: int | None = None) -> None:
        # Issue #99: bound the per-process token state to prevent unbounded
        # memory growth on a long-running bot watching many markets. Oldest-
        # inserted entries are evicted first (insertion-order LRU). Active
        # tokens are unaffected because each WSS/REST update re-touches the
        # state via _ensure_state_locked which re-inserts at the back.
        if max_tokens is not None:
            # PR #105 review: validate explicit overrides. max_tokens=0
            # evicts every insert; negative values crash the eviction loop.
            if not isinstance(max_tokens, int) or max_tokens <= 0:
                raise ValueError(
                    f"max_tokens must be a positive int, got {max_tokens!r}"
                )
            self._max_tokens = max_tokens
        else:
            self._max_tokens = _env_int("MARKET_STATE_MAX_TOKENS", 5000)
        self._states: OrderedDict[str, TokenMarketState] = OrderedDict()
        self._lock = RLock()
        self._last_ws_heartbeat_monotonic: float | None = None
        self._last_ws_pong_monotonic: float | None = None

    # ------------------------------------------------------------------
    # Core access
    # ------------------------------------------------------------------

    def get_state(self, token_id: str) -> TokenMarketState | None:
        with self._lock:
            state = self._states.get(token_id)
            if state is None:
                return None
            return self._copy_state(state)

    def ensure_state(self, token_id: str) -> TokenMarketState:
        with self._lock:
            return self._ensure_state_locked(token_id)

    def _ensure_state_locked(self, token_id: str) -> TokenMarketState:
        token_id = str(token_id)
        state = self._states.get(token_id)
        if state is None:
            state = TokenMarketState(token_id=token_id)
            self._states[token_id] = state
            # Issue #99: bounded cache — evict oldest insertion when over cap.
            while len(self._states) > self._max_tokens:
                evicted_id, _ = self._states.popitem(last=False)
                _logger.debug(
                    "market_state_store_evicted | token_id=%s reason=max_tokens cap=%d",
                    evicted_id, self._max_tokens,
                )
        else:
            # Touch on access: move to MRU end so eviction targets truly cold
            # tokens, not active ones that happen to have been seen first.
            self._states.move_to_end(token_id)
        return state

    # ------------------------------------------------------------------
    # Snapshots for MarketDataService
    # ------------------------------------------------------------------

    def get_quote(self, token_id: str) -> QuoteSnapshot | None:
        with self._lock:
            state = self._states.get(token_id)
            if state is None:
                return None
            if state.best_bid is None and state.best_ask is None:
                return None
            return QuoteSnapshot(
                token_id=state.token_id,
                best_bid=state.best_bid,
                best_ask=state.best_ask,
                last_trade_price=state.last_trade_price,
                source=state.quote_source if state.quote_source != "unknown" else state.source,
                monotonic_ts=state.last_quote_monotonic or state.last_update_monotonic,
            )

    def get_book(self, token_id: str) -> BookSnapshot | None:
        with self._lock:
            state = self._states.get(token_id)
            if state is None:
                return None
            if not state.bids and not state.asks:
                return None
            return BookSnapshot(
                token_id=state.token_id,
                bids=dict(state.bids),
                asks=dict(state.asks),
                best_bid=state.best_bid,
                best_ask=state.best_ask,
                source=state.book_source if state.book_source != "unknown" else state.source,
                monotonic_ts=state.last_book_monotonic or state.last_update_monotonic,
            )

    def get_tick(self, token_id: str) -> TickSnapshot | None:
        with self._lock:
            state = self._states.get(token_id)
            if state is None:
                return None
            if state.tick_size is None or state.min_order_size is None:
                return None
            return TickSnapshot(
                token_id=state.token_id,
                tick_size=state.tick_size,
                min_order_size=state.min_order_size,
                neg_risk=state.neg_risk,
                source=state.tick_source if state.tick_source != "unknown" else state.source,
                monotonic_ts=state.last_tick_monotonic or state.last_update_monotonic,
            )

    # ------------------------------------------------------------------
    # Full book and deltas
    # ------------------------------------------------------------------

    def replace_book_snapshot(
        self,
        token_id: str,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
        raw_event: dict[str, Any] | None = None,
        *,
        fetched_at_monotonic: float | None = None,
    ) -> None:
        """Replace the entire book for token_id.

        Use this for:
          - WSS `book`
          - REST snapshot writes

        This must clear old levels. Do not implement book snapshots as deltas.

        When ``fetched_at_monotonic`` is supplied (REST snapshot path), the
        cached freshness timestamps reflect the actual fetch instant rather
        than the write-back instant. WSS deltas omit it and fall back to
        ``time.monotonic()``.
        """
        with self._lock:
            state = self._ensure_state_locked(token_id)

            prev_book_src = state.book_source
            prev_quote_src = state.quote_source

            state.bids = {
                Decimal(str(price)): Decimal(str(size))
                for price, size in bids.items()
                if Decimal(str(size)) > 0
            }
            state.asks = {
                Decimal(str(price)): Decimal(str(size))
                for price, size in asks.items()
                if Decimal(str(size)) > 0
            }

            state.recompute_top_of_book()

            now = (
                fetched_at_monotonic
                if fetched_at_monotonic is not None
                else time.monotonic()
            )
            state.last_book_monotonic = now
            state.last_quote_monotonic = now
            state.last_update_monotonic = now
            state.last_event_raw = raw_event

            source = self._source_from_raw(raw_event, default="wss")
            state.source = source
            state.book_source = source
            state.quote_source = source

            if raw_event:
                cid = (
                    raw_event.get("condition_id")
                    or raw_event.get("market")
                    or raw_event.get("conditionId")
                )
                if cid:
                    state.condition_id = str(cid).strip() or state.condition_id

            # A full book makes the book clean. Tick may still be missing, but
            # MarketDataService will reject that separately.
            state.stale = False
            state.stale_reason = None
            state.dirty = False
            state.dirty_reason = None

            if source == "wss" and (
                prev_book_src == "rest_snapshot" or prev_quote_src == "rest_snapshot"
            ):
                _logger.info(
                    "market_data_rest_snapshot_wss_override | token_id=%s "
                    "prev_book=%s prev_quote=%s",
                    token_id,
                    prev_book_src,
                    prev_quote_src,
                )

    def apply_book_delta(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
    ) -> None:
        """Apply one book level update.

        side:
          - BUY/BID -> bids
          - SELL/ASK -> asks

        size <= 0 deletes the level.
        """
        normalized_side = self._normalize_side(side)
        if normalized_side is None:
            return

        with self._lock:
            state = self._ensure_state_locked(token_id)
            book = state.bids if normalized_side == "BUY" else state.asks

            prev_book_src = state.book_source
            prev_quote_src = state.quote_source

            price = Decimal(str(price))
            size = Decimal(str(size))

            if size <= 0:
                book.pop(price, None)
            else:
                book[price] = size

            state.recompute_top_of_book()

            now = time.monotonic()
            state.last_book_monotonic = now
            state.last_quote_monotonic = now
            state.last_update_monotonic = now
            state.source = "wss"
            state.book_source = "wss"
            state.quote_source = "wss"

            if not state.resolved:
                state.stale = False
                state.stale_reason = None
                if (
                    state.bids
                    and state.asks
                    and state.best_bid is not None
                    and state.best_ask is not None
                ):
                    state.dirty = False
                    state.dirty_reason = None

            if prev_book_src == "rest_snapshot" or prev_quote_src == "rest_snapshot":
                _logger.info(
                    "market_data_rest_snapshot_wss_override | token_id=%s "
                    "prev_book=%s prev_quote=%s event=apply_book_delta",
                    token_id,
                    prev_book_src,
                    prev_quote_src,
                )

    # ------------------------------------------------------------------
    # Quote / tick / metadata updates
    # ------------------------------------------------------------------

    def on_price_update(
        self,
        token_id: str,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
    ) -> None:
        with self._lock:
            state = self._ensure_state_locked(token_id)

            if best_bid is not None:
                state.best_bid = Decimal(str(best_bid))
            if best_ask is not None:
                state.best_ask = Decimal(str(best_ask))

            now = time.monotonic()
            state.last_quote_monotonic = now
            state.last_update_monotonic = now
            state.source = "wss"
            state.quote_source = "wss"

            if not state.resolved:
                state.stale = False
                state.stale_reason = None

    def on_best_bid_ask(
        self,
        token_id: str,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
        raw_event: dict[str, Any] | None = None,
    ) -> None:
        """Top-of-book update.

        This updates quote. It does not replace full book depth.
        """
        self.on_price_update(token_id, best_bid, best_ask)

        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.last_event_raw = raw_event

            # Optional consistency check. If top-of-book contradicts known book,
            # mark dirty so execution performs REST/WSS resync before trading.
            book_bid = max(state.bids) if state.bids else None
            book_ask = min(state.asks) if state.asks else None

            # Only flag conflict when the book is already WSS-authoritative.
            # During REST→WSS transition the book is still "rest_snapshot"
            # while the WSS top-of-book reflects a slightly newer market
            # state; mismatches here are expected and must not trigger dirty.
            if state.book_source == "wss":
                if book_bid is not None and best_bid is not None and book_bid != best_bid:
                    state.dirty = True
                    state.dirty_reason = "best_bid_conflicts_with_book"
                if book_ask is not None and best_ask is not None and book_ask != best_ask:
                    state.dirty = True
                    state.dirty_reason = "best_ask_conflicts_with_book"

    def on_last_trade_price(
        self,
        token_id: str,
        price: Decimal,
        raw_event: dict[str, Any] | None = None,
    ) -> None:
        """Store last trade for observability only.

        Do not use last trade price as executable price.
        """
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.last_trade_price = Decimal(str(price))
            state.last_trade_monotonic = time.monotonic()
            state.last_event_raw = raw_event
            state.mark_updated()

    def on_tick_size_change(self, token_id: str, tick_size: Decimal) -> None:
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.tick_size = Decimal(str(tick_size))
            if state.min_order_size is None:
                state.min_order_size = Decimal("0")

            now = time.monotonic()
            state.last_tick_monotonic = now
            state.last_update_monotonic = now
            state.source = "wss"
            # Tick-only WSS event — do not mutate quote/book sources. A
            # ``tick_size_change`` must not make a REST-bootstrapped book
            # appear WSS-fresh for execution.
            state.tick_source = "wss"

            # A tick-size change invalidates prepared sizing/idempotency
            # decisions. It should force recompute before next trade.
            state.dirty = True
            state.dirty_reason = "tick_size_changed"

            if not state.resolved:
                state.stale = False
                state.stale_reason = None

    def set_tick(
        self,
        token_id: str,
        *,
        tick_size: Decimal,
        min_order_size: Decimal,
        neg_risk: bool = False,
        source: str = "rest_snapshot",
        fetched_at_monotonic: float | None = None,
    ) -> None:
        """Update tick + metadata for token_id.

        Only ``tick_source``, ``metadata_source``, and ``state.source`` move.
        Quote/book component sources are not mutated — a tick-only update
        must not promote a REST-only quote/book to appear WSS-fresh.
        """
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.tick_size = Decimal(str(tick_size))
            state.min_order_size = Decimal(str(min_order_size))
            state.neg_risk = bool(neg_risk)

            now = (
                fetched_at_monotonic
                if fetched_at_monotonic is not None
                else time.monotonic()
            )
            state.last_tick_monotonic = now
            state.last_update_monotonic = now
            state.source = source
            state.tick_source = source
            state.metadata_source = source

            if not state.resolved:
                state.stale = False
                state.stale_reason = None

    def set_quote(
        self,
        token_id: str,
        *,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
        source: str = "rest_snapshot",
        fetched_at_monotonic: float | None = None,
    ) -> None:
        """Update top-of-book only.

        ``book_source`` is intentionally untouched — a quote-only update
        must not promote a REST-only book to WSS or vice versa.
        """
        with self._lock:
            state = self._ensure_state_locked(token_id)

            if best_bid is not None:
                state.best_bid = Decimal(str(best_bid))
            if best_ask is not None:
                state.best_ask = Decimal(str(best_ask))

            now = (
                fetched_at_monotonic
                if fetched_at_monotonic is not None
                else time.monotonic()
            )
            state.last_quote_monotonic = now
            state.last_update_monotonic = now
            state.source = source
            state.quote_source = source

    def set_book(
        self,
        token_id: str,
        *,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
        source: str = "rest_snapshot",
        condition_id: str | None = None,
        fetched_at_monotonic: float | None = None,
    ) -> None:
        """Replace the full book and derive top-of-book quote.

        Quote source is set to the same value because the top-of-book is
        derived directly from this book. Tick metadata is untouched.
        """
        raw: dict[str, Any] = {"source": source}
        if condition_id:
            raw["condition_id"] = condition_id
        self.replace_book_snapshot(
            token_id,
            bids=bids,
            asks=asks,
            raw_event=raw,
            fetched_at_monotonic=fetched_at_monotonic,
        )

    def store_rest_snapshot(
        self,
        token_id: str,
        *,
        condition_id: str | None,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
        tick_size: Decimal | None,
        min_order_size: Decimal | None,
        neg_risk: bool = False,
        fetched_at_monotonic: float,
    ) -> dict[str, Any]:
        """Write a fresh REST `/book` snapshot atomically.

        Quote/book/tick all land with ``source="rest_snapshot"`` and the
        same ``fetched_at_monotonic`` for downstream age accounting.
        ``tick_size`` and ``min_order_size`` are written only when both are
        present and positive — partial tick state must not be considered
        executable.

        Returns ``get_debug_state(token_id)`` for caller logging.
        """
        self.set_book(
            token_id,
            bids=bids,
            asks=asks,
            source="rest_snapshot",
            condition_id=condition_id,
            fetched_at_monotonic=fetched_at_monotonic,
        )
        if (
            tick_size is not None
            and tick_size > 0
            and min_order_size is not None
            and min_order_size > 0
        ):
            self.set_tick(
                token_id,
                tick_size=Decimal(str(tick_size)),
                min_order_size=Decimal(str(min_order_size)),
                neg_risk=neg_risk,
                source="rest_snapshot",
                fetched_at_monotonic=fetched_at_monotonic,
            )
        return self.get_debug_state(token_id)

    # ------------------------------------------------------------------
    # Market lifecycle
    # ------------------------------------------------------------------

    def on_new_market(self, raw_event: dict[str, Any]) -> None:
        """Optional metadata hook.

        This should not automatically enable trading.
        """
        token_id = self._asset_id_from_event(raw_event)
        if token_id is None:
            return

        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.active = True
            state.resolved = False
            state.last_event_raw = raw_event
            state.mark_updated()

            condition_id = raw_event.get("condition_id") or raw_event.get("conditionId")
            if condition_id:
                state.condition_id = str(condition_id)

    def on_market_resolved(self, *args: Any) -> None:
        """Mark token/market resolved.

        Supports both:
            on_market_resolved(token_id, event)
            on_market_resolved(event)
        """
        if len(args) == 1 and isinstance(args[0], dict):
            raw_event = args[0]
            token_id = self._asset_id_from_event(raw_event)
        elif len(args) >= 1:
            token_id = str(args[0])
            raw_event = args[1] if len(args) > 1 and isinstance(args[1], dict) else None
        else:
            return

        if not token_id:
            return

        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.active = False
            state.resolved = True
            state.dirty = True
            state.dirty_reason = "market_resolved"
            state.stale = True
            state.stale_reason = "market_resolved"
            state.last_event_raw = raw_event
            state.mark_updated()

    # ------------------------------------------------------------------
    # Freshness / dirty flags
    # ------------------------------------------------------------------

    def mark_stale(self, token_id: str, reason: str) -> None:
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.stale = True
            state.stale_reason = reason
            state.dirty = True
            state.dirty_reason = reason
            state.mark_updated()

    def mark_dirty(self, token_id: str, reason: str) -> None:
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.dirty = True
            state.dirty_reason = reason
            state.mark_updated()

    def mark_fresh(
        self,
        token_id: str,
        source: str = "wss",
        monotonic_ts: float | None = None,
    ) -> None:
        """Touch aggregate freshness without mutating component sources.

        Component-level sources (``quote_source``, ``book_source``,
        ``tick_source``, ``metadata_source``) are authoritative for executable
        freshness checks. They are only set by the dedicated setters, WSS
        handlers, and ``replace_book_snapshot``/``apply_book_delta``.
        ``mark_fresh`` must not promote them blindly — doing so allowed a
        single touch to make a REST-only quote/book appear WSS-fresh.
        """
        with self._lock:
            state = self._ensure_state_locked(token_id)
            now = monotonic_ts if monotonic_ts is not None else time.monotonic()
            state.source = source
            state.stale = False
            state.stale_reason = None
            state.last_update_monotonic = now

            # Only clear dirty when enough executable state exists.
            if (
                not state.resolved
                and state.best_bid is not None
                and state.best_ask is not None
                and state.bids
                and state.asks
                and state.tick_size is not None
                and state.min_order_size is not None
            ):
                state.dirty = False
                state.dirty_reason = None

    def invalidate(self, token_id: str) -> None:
        """Clear volatile quote/book data for token_id; tick metadata persists."""
        with self._lock:
            state = self._states.get(token_id)
            if state is None:
                return
            state.best_bid = None
            state.best_ask = None
            state.last_trade_price = None
            state.bids = {}
            state.asks = {}
            state.stale = True
            state.stale_reason = "invalidated"
            state.dirty = True
            state.dirty_reason = "invalidated"
            state.mark_updated()

    def evict(self, token_id: str) -> None:
        """Remove all cached market state for token_id.

        Use after own fills: old book/quote data is stale by definition and
        must not be traded against until the WSS delivers fresh state.
        """
        with self._lock:
            self._states.pop(token_id, None)

    def invalidate_sizing_decision(self, token_id: str) -> None:
        """Mark token's sizing-derived state stale after a tick-size change.

        Pre-fix: ``clients/market_ws.py:_handle_tick_size_change`` called
        ``_call_cache_optional("invalidate_sizing_decision", asset_id)``
        but no method by that name existed on MarketStateStore — the
        helper silently returned ``False`` and any sizing decision derived
        from the old tick could be reused unchanged.

        The handler order is:
          (1) on_tick_size_change(asset_id, tick)   — store new tick
          (2) _mark_asset_dirty(asset_id, ...)      — sets state.dirty
          (3) invalidate_sizing_decision(asset_id)  — THIS METHOD

        Because step 1 has already landed the FRESH tick, this method
        must NOT clear ``tick_size`` or ``min_order_size``. Instead it
        documents the contract: the dirty flag set in step 2 signals to
        sizing-derived caches (e.g. PriceCache, idempotency keystore)
        that any decision computed with the previous tick is now invalid
        and must be re-derived from the freshly-stored tick.

        Today this is effectively a marker / wiring point — the method
        EXISTS so ``_call_cache_optional`` returns True (and the
        existing dirty flag carries the freshness signal). Future
        sizing-derived caches can plug into this hook without changing
        the WSS handler.
        """
        with self._lock:
            state = self._states.get(token_id)
            if state is None:
                return
            # Defensive: ensure the dirty flag is set with a sizing-
            # specific reason so any downstream consumer reading
            # ``dirty_reason`` can attribute the invalidation correctly.
            if not state.dirty:
                state.dirty = True
            state.dirty_reason = "tick_size_change_invalidate_sizing"
            state.mark_updated()

    def invalidate_all(self) -> None:
        """Full wipe for reconnect bootstrap — clears quote, book, and tick."""
        with self._lock:
            for state in self._states.values():
                state.best_bid = None
                state.best_ask = None
                state.last_trade_price = None
                state.bids = {}
                state.asks = {}
                state.tick_size = None
                state.min_order_size = None
                state.stale = True
                state.stale_reason = "invalidate_all"
                state.dirty = True
                state.dirty_reason = "invalidate_all"
                state.mark_updated()

    def mark_all_stale(self, reason: str) -> None:
        with self._lock:
            for state in self._states.values():
                state.stale = True
                state.stale_reason = reason
                state.dirty = True
                state.dirty_reason = reason
                state.mark_updated()

    def mark_all_dirty(self, reason: str) -> None:
        with self._lock:
            for state in self._states.values():
                state.dirty = True
                state.dirty_reason = reason
                state.mark_updated()

    def is_dirty(self, token_id: str) -> bool:
        with self._lock:
            state = self._states.get(token_id)
            return True if state is None else state.dirty

    def is_stale(self, token_id: str) -> bool:
        with self._lock:
            state = self._states.get(token_id)
            return True if state is None else state.stale

    def is_resolved(self, token_id: str) -> bool:
        with self._lock:
            state = self._states.get(token_id)
            return False if state is None else state.resolved

    def source_of(self, token_id: str) -> str:
        with self._lock:
            state = self._states.get(token_id)
            return "unknown" if state is None else state.source

    def quote_source_of(self, token_id: str) -> str:
        with self._lock:
            state = self._states.get(token_id)
            return "unknown" if state is None else state.quote_source

    def book_source_of(self, token_id: str) -> str:
        with self._lock:
            state = self._states.get(token_id)
            return "unknown" if state is None else state.book_source

    def tick_source_of(self, token_id: str) -> str:
        with self._lock:
            state = self._states.get(token_id)
            return "unknown" if state is None else state.tick_source

    def metadata_source_of(self, token_id: str) -> str:
        with self._lock:
            state = self._states.get(token_id)
            return "unknown" if state is None else state.metadata_source

    def touch(self, token_id: str, source: str, monotonic_ts: float | None = None) -> None:
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.source = source
            state.last_update_monotonic = monotonic_ts or time.monotonic()

    def mark_resolved(self, token_id: str, condition_id: str | None = None) -> None:
        """Mark a token as resolved and non-executable.

        Thin wrapper over on_market_resolved that also stores condition_id.
        """
        self.on_market_resolved(token_id)
        if condition_id:
            with self._lock:
                state = self._ensure_state_locked(token_id)
                state.condition_id = str(condition_id).strip() or state.condition_id

    def get_debug_state(self, token_id: str) -> dict[str, Any]:
        """Return a snapshot of token state for logging and error messages.

        Field names are stable across known/unknown branches so callers can
        format them uniformly. Ages are reported in milliseconds (``*_ms``).
        """
        with self._lock:
            state = self._states.get(token_id)
            if state is None:
                return {
                    "token_id": token_id,
                    "known": False,
                    "has_quote": False,
                    "has_book": False,
                    "quote_source": None,
                    "book_source": None,
                    "tick_source": None,
                    "metadata_source": None,
                    "source": None,
                    "quote_age_ms": None,
                    "book_age_ms": None,
                    "best_bid": None,
                    "best_ask": None,
                    "bid_count": 0,
                    "ask_count": 0,
                    "tick_size": None,
                    "active": None,
                    "resolved": None,
                    "condition_id": None,
                    "dirty": None,
                    "stale": None,
                    "dirty_reason": None,
                    "stale_reason": None,
                }
            now = time.monotonic()
            quote_age_ms = (
                round((now - state.last_quote_monotonic) * 1000.0, 1)
                if state.last_quote_monotonic
                else None
            )
            book_age_ms = (
                round((now - state.last_book_monotonic) * 1000.0, 1)
                if state.last_book_monotonic
                else None
            )
            return {
                "token_id": token_id,
                "known": True,
                "has_quote": state.best_bid is not None or state.best_ask is not None,
                "has_book": bool(state.bids or state.asks),
                "quote_source": state.quote_source,
                "book_source": state.book_source,
                "tick_source": state.tick_source,
                "metadata_source": state.metadata_source,
                "source": state.source,
                "quote_age_ms": quote_age_ms,
                "book_age_ms": book_age_ms,
                "best_bid": str(state.best_bid) if state.best_bid is not None else None,
                "best_ask": str(state.best_ask) if state.best_ask is not None else None,
                "bid_count": len(state.bids),
                "ask_count": len(state.asks),
                "tick_size": str(state.tick_size) if state.tick_size is not None else None,
                "active": state.active,
                "resolved": state.resolved,
                "condition_id": state.condition_id,
                "dirty": state.dirty,
                "stale": state.stale,
                "dirty_reason": state.dirty_reason,
                "stale_reason": state.stale_reason,
            }

    def update_tick_size_from_book(self, token_id: str, tick_size: Decimal) -> None:
        """Update tick_size metadata from a WSS book snapshot without dirtying state.

        Book events carry the current tick_size as informational metadata, not as a
        change notification. Unlike on_tick_size_change, this does not dirty the state
        or reset min_order_size — those side effects are appropriate only for an actual
        tick_size_change event where the tick has changed.
        """
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.tick_size = Decimal(str(tick_size))
            now = time.monotonic()
            state.last_tick_monotonic = now
            state.last_update_monotonic = now
            state.tick_source = "wss"

    def on_market_ws_heartbeat(self, monotonic_ts: float) -> None:
        with self._lock:
            self._last_ws_heartbeat_monotonic = monotonic_ts

    def on_market_ws_pong(self, monotonic_ts: float) -> None:
        with self._lock:
            self._last_ws_pong_monotonic = monotonic_ts

    def wss_connection_age_seconds(self) -> float | None:
        """Seconds since the most recent market WSS heartbeat or pong.

        Connection liveness, not data freshness. MarketDataService uses this so
        a quiet-but-connected WSS stream stays executable: an unchanged book is
        still the current book while the WSS pipe is proven alive. Returns None
        when no heartbeat/pong has been observed yet (e.g. pre-connect).
        """
        with self._lock:
            timestamps = [
                ts
                for ts in (
                    self._last_ws_heartbeat_monotonic,
                    self._last_ws_pong_monotonic,
                )
                if ts is not None
            ]
            if not timestamps:
                return None
            return time.monotonic() - max(timestamps)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _copy_state(self, state: TokenMarketState) -> TokenMarketState:
        return TokenMarketState(
            token_id=state.token_id,
            condition_id=state.condition_id,
            bids=dict(state.bids),
            asks=dict(state.asks),
            best_bid=state.best_bid,
            best_ask=state.best_ask,
            last_trade_price=state.last_trade_price,
            tick_size=state.tick_size,
            min_order_size=state.min_order_size,
            neg_risk=state.neg_risk,
            active=state.active,
            resolved=state.resolved,
            dirty=state.dirty,
            stale=state.stale,
            dirty_reason=state.dirty_reason,
            stale_reason=state.stale_reason,
            source=state.source,
            quote_source=state.quote_source,
            book_source=state.book_source,
            tick_source=state.tick_source,
            metadata_source=state.metadata_source,
            last_update_monotonic=state.last_update_monotonic,
            last_book_monotonic=state.last_book_monotonic,
            last_quote_monotonic=state.last_quote_monotonic,
            last_tick_monotonic=state.last_tick_monotonic,
            last_trade_monotonic=state.last_trade_monotonic,
            last_event_raw=dict(state.last_event_raw) if state.last_event_raw else None,
        )

    @staticmethod
    def _normalize_side(side: str) -> str | None:
        value = str(side or "").upper()
        if value in {"BUY", "BID"}:
            return "BUY"
        if value in {"SELL", "ASK"}:
            return "SELL"
        return None

    @staticmethod
    def _source_from_raw(raw_event: dict[str, Any] | None, *, default: str) -> str:
        if not raw_event:
            return default
        return str(raw_event.get("source") or default)

    @staticmethod
    def _asset_id_from_event(event: dict[str, Any]) -> str | None:
        raw = (
            event.get("asset_id")
            or event.get("asset")
            or event.get("token_id")
            or event.get("token")
        )
        if raw is None:
            return None
        token_id = str(raw).strip()
        return token_id or None