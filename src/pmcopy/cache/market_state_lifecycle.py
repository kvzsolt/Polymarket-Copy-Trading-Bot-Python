"""Staleness, invalidation, WSS liveness, and diagnostic inspection mixin."""

from __future__ import annotations

import time
from collections import OrderedDict
from decimal import Decimal
from threading import RLock
from typing import Any

from pmcopy.cache.snapshots import TokenMarketState


class _MarketStateLifecycleMixin:
    """Lifecycle, invalidation, source-query, and debug inspection helpers."""

    _lock: RLock
    _states: OrderedDict[str, TokenMarketState]
    _last_ws_heartbeat_monotonic: float | None
    _last_ws_pong_monotonic: float | None

    def on_market_resolved(self, *args: Any) -> None:
        raise NotImplementedError

    def _ensure_state_locked(self, token_id: str) -> TokenMarketState:
        raise NotImplementedError

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

        Pre-fix: ``clients/ws/market_ws.py:_handle_tick_size_change`` called
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
        """Seconds since the most recent INBOUND market WSS activity.

        Connection liveness, not data freshness. MarketDataService uses this so
        a quiet-but-connected WSS stream stays executable: an unchanged book is
        still the current book while the WSS pipe is proven alive. Returns None
        when no inbound activity has been observed yet (e.g. pre-connect).

        P1-d: liveness is anchored on inbound activity only. The outbound PING
        (``_last_ws_heartbeat_monotonic``) is self-generated every ~5s and must
        NOT count toward liveness — including it masked a silent / half-open
        feed as fresh until the slower in-loop pong detector closed it, serving
        a stale book past the book-age budget in the meantime. Only a frame
        from the server (PONG or data, fed via ``on_market_ws_pong``) proves
        the pipe is alive.
        """
        with self._lock:
            last_inbound = self._last_ws_pong_monotonic
            if last_inbound is None:
                return None
            return time.monotonic() - last_inbound

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
