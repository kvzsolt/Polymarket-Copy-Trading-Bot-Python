"""Phase 2: In-memory price cache with TTL eviction and Gamma fallback.

Narrow, single-purpose cache for the last-known executable price of a
``(condition_id, token_id)`` pair. Sized for hot-path reads in copy/decision
flows where a full :class:`MarketStateStore` lookup would be overkill.

Design notes:
  - Key is the pair ``(condition_id, token_id)`` so neg-risk markets sharing
    a YES/NO outcome with the same token cannot collide.
  - Value is ``(Decimal price, monotonic timestamp)``. Prices are always
    coerced to ``Decimal`` — never floats — to match the rest of the bot.
  - TTL eviction is lazy on read plus an explicit :meth:`evict_expired` for
    callers that want a batch sweep on reconnect.
  - Thread-safe and async-safe: synchronous methods guarded by an ``RLock``
    so async coroutines and synchronous WSS handlers can share the cache
    without an event-loop dependency.
  - On miss/expiry, callers can pass a Gamma-style ``fetch_callback`` via
    :meth:`get_or_fetch` to repopulate the entry. The cache itself stays
    transport-agnostic.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal
from threading import RLock

DEFAULT_TTL_SECONDS: float = 30.0
# Default LRU cap matches MarketStateStore.max_tokens so the two caches
# are bounded similarly. Operators tracking thousands of tokens can
# override via constructor.
DEFAULT_MAX_ENTRIES: int = 5000

PriceKey = tuple[str, str]
FetchCallback = Callable[[str, str], Awaitable[Decimal | None]]


@dataclass(frozen=True)
class PriceEntry:
    """Immutable snapshot returned to callers."""

    condition_id: str
    token_id: str
    price: Decimal
    monotonic_ts: float

    def age_seconds(self) -> float:
        return time.monotonic() - self.monotonic_ts

    def is_fresh(self, max_age_seconds: float) -> bool:
        return self.age_seconds() <= max_age_seconds


class PriceCache:
    """TTL-bounded ``(condition_id, token_id) -> price`` cache."""

    def __init__(
        self,
        *,
        default_ttl_seconds: float = DEFAULT_TTL_SECONDS,
        max_entries: int = DEFAULT_MAX_ENTRIES,
    ) -> None:
        if default_ttl_seconds <= 0:
            raise ValueError("default_ttl_seconds must be > 0")
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        self._default_ttl = float(default_ttl_seconds)
        self._max_entries = int(max_entries)
        # OrderedDict so we can do LRU eviction with O(1) move-to-end on
        # set/touch. Pre-fix this was a plain dict with no upper bound —
        # a long-running bot watching many markets accumulated entries
        # indefinitely.
        self._entries: OrderedDict[PriceKey, PriceEntry] = OrderedDict()
        self._lock = RLock()

    @property
    def default_ttl_seconds(self) -> float:
        return self._default_ttl

    def _key(self, condition_id: str, token_id: str) -> PriceKey:
        return (str(condition_id), str(token_id))

    def set_price(
        self,
        condition_id: str,
        token_id: str,
        price: Decimal,
        *,
        monotonic_ts: float | None = None,
    ) -> PriceEntry:
        """Store ``price`` for ``(condition_id, token_id)``.

        ``price`` is coerced to :class:`Decimal`. ``monotonic_ts`` defaults
        to ``time.monotonic()``; callers may pass an earlier instant when
        attributing freshness to the fetch time rather than the write time.
        """
        if not condition_id or not token_id:
            raise ValueError("condition_id and token_id must be non-empty")
        coerced = Decimal(str(price))
        ts = float(monotonic_ts) if monotonic_ts is not None else time.monotonic()
        entry = PriceEntry(
            condition_id=str(condition_id),
            token_id=str(token_id),
            price=coerced,
            monotonic_ts=ts,
        )
        with self._lock:
            key = self._key(condition_id, token_id)
            self._entries[key] = entry
            # MRU position so cold tokens drift toward the eviction edge.
            self._entries.move_to_end(key)
            # LRU enforcement: trim from the oldest end if over cap.
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
        return entry

    def get_price(
        self,
        condition_id: str,
        token_id: str,
        *,
        max_age_seconds: float | None = None,
    ) -> Decimal | None:
        """Return the cached price if still fresh; else ``None``.

        ``max_age_seconds`` overrides ``default_ttl_seconds`` for this call.
        Expired entries are evicted lazily.
        """
        ttl = float(max_age_seconds if max_age_seconds is not None else self._default_ttl)
        with self._lock:
            key = self._key(condition_id, token_id)
            entry = self._entries.get(key)
            if entry is None:
                return None
            if not entry.is_fresh(ttl):
                self._entries.pop(key, None)
                return None
            # Touch on hit so frequently-read tokens stay MRU.
            self._entries.move_to_end(key)
            return entry.price

    def get_entry(
        self,
        condition_id: str,
        token_id: str,
    ) -> PriceEntry | None:
        """Return the raw entry without freshness checking (debug/observability)."""
        with self._lock:
            return self._entries.get(self._key(condition_id, token_id))

    def is_fresh(
        self,
        condition_id: str,
        token_id: str,
        max_age_seconds: float | None = None,
    ) -> bool:
        ttl = float(max_age_seconds if max_age_seconds is not None else self._default_ttl)
        with self._lock:
            entry = self._entries.get(self._key(condition_id, token_id))
            if entry is None:
                return False
            return entry.is_fresh(ttl)

    def invalidate(self, condition_id: str, token_id: str) -> None:
        with self._lock:
            self._entries.pop(self._key(condition_id, token_id), None)

    def evict_expired(self, *, max_age_seconds: float | None = None) -> int:
        """Drop expired entries. Returns number of evictions."""
        ttl = float(max_age_seconds if max_age_seconds is not None else self._default_ttl)
        with self._lock:
            stale_keys = [k for k, e in self._entries.items() if not e.is_fresh(ttl)]
            for k in stale_keys:
                self._entries.pop(k, None)
            return len(stale_keys)

    def clear(self) -> None:
        """Drop all entries — use on WSS reconnect before resync."""
        with self._lock:
            self._entries.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._entries)

    def snapshot(self) -> dict[PriceKey, PriceEntry]:
        """Return a shallow copy of current entries for inspection/tests."""
        with self._lock:
            return dict(self._entries)

    async def get_or_fetch(
        self,
        condition_id: str,
        token_id: str,
        fetch_callback: FetchCallback,
        *,
        max_age_seconds: float | None = None,
    ) -> Decimal | None:
        """Return a fresh cached price; on miss, call ``fetch_callback``.

        ``fetch_callback`` must be an async callable that returns a
        ``Decimal`` or ``None``. A successful fetch is written back to the
        cache. Callback failures propagate; cache state is left unchanged.
        """
        cached = self.get_price(
            condition_id, token_id, max_age_seconds=max_age_seconds
        )
        if cached is not None:
            return cached
        fetched = await fetch_callback(condition_id, token_id)
        if fetched is None:
            return None
        coerced = Decimal(str(fetched))
        self.set_price(condition_id, token_id, coerced)
        return coerced
