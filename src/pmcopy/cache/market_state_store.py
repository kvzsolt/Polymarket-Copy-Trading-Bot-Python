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
import time
from collections import OrderedDict
from decimal import Decimal
from threading import RLock
from typing import Any


from pmcopy.cache.market_state_lifecycle import _MarketStateLifecycleMixin
from pmcopy.cache.snapshots import (
    BookSnapshot,
    QuoteSnapshot,
    TickSnapshot,
    TokenMarketState,
    _env_int,
)

_logger = logging.getLogger("MarketStateStore")


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class MarketStateStore(_MarketStateLifecycleMixin):
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
                raise ValueError(f"max_tokens must be a positive int, got {max_tokens!r}")
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
                    evicted_id,
                    self._max_tokens,
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
                valid=not (
                    state.resolved
                    or (state.stale and state.stale_reason != "not_initialized")
                    or (state.dirty and state.dirty_reason != "not_initialized")
                ),
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
            if (
                state.tick_size is None
                or state.min_order_size is None
                or not isinstance(state.neg_risk, bool)
            ):
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
        try:
            validated_bids = self._validated_levels(bids)
            validated_asks = self._validated_levels(asks)
        except ValueError:
            self.mark_stale(token_id, "invalid_book_update")
            raise
        with self._lock:
            state = self._ensure_state_locked(token_id)

            prev_book_src = state.book_source
            prev_quote_src = state.quote_source

            state.bids = validated_bids
            state.asks = validated_asks

            state.recompute_top_of_book()

            now = fetched_at_monotonic if fetched_at_monotonic is not None else time.monotonic()
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

    def apply_book_deltas(self, updates: tuple[tuple[str, str, Decimal, Decimal], ...]) -> None:
        """Publish a validated message's level updates under one book-store lock."""
        try:
            for _token_id, side, price, size in updates:
                self._validated_levels({price: size})
                if self._normalize_side(side) is None:
                    raise ValueError("Book side is invalid")
        except ValueError:
            for token_id, *_rest in updates:
                self.mark_stale(token_id, "invalid_book_delta_batch")
            raise
        with self._lock:
            for token_id, side, price, size in updates:
                self.apply_book_delta(token_id, side, price, size)

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
        try:
            self._validated_levels({price: size})
        except ValueError:
            self.mark_stale(token_id, "invalid_book_update")
            raise
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

            if not state.resolved and not state.stale:
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

    @staticmethod
    def _validated_levels(levels: dict[Decimal, Decimal]) -> dict[Decimal, Decimal]:
        """Validate all levels before publication; zero shares delete a level."""
        validated: dict[Decimal, Decimal] = {}
        for price, size in levels.items():
            if isinstance(price, bool) or isinstance(size, bool):
                raise ValueError("Book levels must be numeric")
            try:
                parsed_price = Decimal(str(price))
                parsed_size = Decimal(str(size))
            except ArithmeticError:
                raise ValueError("Book levels must be numeric") from None
            if (
                not parsed_price.is_finite()
                or not parsed_size.is_finite()
                or not Decimal(0) <= parsed_price <= Decimal(1)
                or parsed_size < 0
            ):
                raise ValueError(
                    "Book levels require finite bounded prices and non-negative shares"
                )
            if parsed_size:
                validated[parsed_price] = parsed_size
        return validated

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

    def set_tick(
        self,
        token_id: str,
        *,
        tick_size: Decimal,
        min_order_size: Decimal,
        neg_risk: bool,
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

            now = fetched_at_monotonic if fetched_at_monotonic is not None else time.monotonic()
            state.last_tick_monotonic = now
            state.last_update_monotonic = now
            state.source = source
            state.tick_source = source
            state.metadata_source = source

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

            now = fetched_at_monotonic if fetched_at_monotonic is not None else time.monotonic()
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
        neg_risk: bool,
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
        raw_event: dict[Any, Any] | None = None
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

