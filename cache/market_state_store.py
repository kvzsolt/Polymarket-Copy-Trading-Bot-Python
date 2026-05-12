"""WSS-backed market state store.

Shared market-data cache keyed by Polymarket token/asset id.

Important:
  - Key this store by token_id, not condition_id.
  - YES and NO tokens under the same condition have separate books.
  - Position state should live elsewhere, keyed by:
        user + condition_id + token_id
  - This store contains market state only:
        book, best bid/ask, tick size, min order size, resolved/stale/dirty flags.

Used by:
  - clients.market_ws.MarketWebSocket
  - services.market_data_service.MarketDataService
  - services.order_sizing
  - services.executionService
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from decimal import Decimal
from threading import RLock
from typing import Any


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

    source: str = "unknown"

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

    def __init__(self) -> None:
        self._states: dict[str, TokenMarketState] = {}
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
                source=state.source,
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
                source=state.source,
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
                source=state.source,
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
    ) -> None:
        """Replace the entire book for token_id.

        Use this for:
          - WSS `book`
          - REST snapshot bootstrap/resync

        This must clear old levels. Do not implement book snapshots as deltas.
        """
        with self._lock:
            state = self._ensure_state_locked(token_id)

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

            now = time.monotonic()
            state.last_book_monotonic = now
            state.last_quote_monotonic = now
            state.last_update_monotonic = now
            state.last_event_raw = raw_event

            source = self._source_from_raw(raw_event, default="wss")
            state.source = source

            # A full book makes the book clean. Tick may still be missing, but
            # MarketDataService will reject that separately.
            state.stale = False
            state.stale_reason = None
            state.dirty = False
            state.dirty_reason = None

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

            if not state.resolved:
                state.stale = False
                state.stale_reason = None

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
    ) -> None:
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.tick_size = Decimal(str(tick_size))
            state.min_order_size = Decimal(str(min_order_size))
            state.neg_risk = bool(neg_risk)

            now = time.monotonic()
            state.last_tick_monotonic = now
            state.last_update_monotonic = now
            state.source = source

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
            state.source = source

    def set_book(
        self,
        token_id: str,
        *,
        bids: dict[Decimal, Decimal],
        asks: dict[Decimal, Decimal],
        source: str = "rest_snapshot",
    ) -> None:
        self.replace_book_snapshot(
            token_id,
            bids=bids,
            asks=asks,
            raw_event={"source": source},
        )

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

    def touch(self, token_id: str, source: str, monotonic_ts: float | None = None) -> None:
        with self._lock:
            state = self._ensure_state_locked(token_id)
            state.source = source
            state.last_update_monotonic = monotonic_ts or time.monotonic()

    def on_market_ws_heartbeat(self, monotonic_ts: float) -> None:
        with self._lock:
            self._last_ws_heartbeat_monotonic = monotonic_ts

    def on_market_ws_pong(self, monotonic_ts: float) -> None:
        with self._lock:
            self._last_ws_pong_monotonic = monotonic_ts

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