"""One bounded shared market-price cache for all leader workers.

The cache is the single shared quote authority across every leader worker.
WSS is the primary source; REST is used only for bootstrap, gap recovery, and
reconnect repair. The cache stores Decimal two-sided quotes and rejects
crossed books and stale entries.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True, slots=True)
class PriceQuote:
    """Normalized two-sided quote from WSS or an explicit repair source."""

    token_id: str
    best_bid: Decimal
    best_ask: Decimal
    observed_at: datetime
    source: str

    def __post_init__(self) -> None:
        if not str(self.token_id).strip():
            raise ValueError("token_id is required")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")
        for name in ("best_bid", "best_ask"):
            try:
                value = Decimal(str(getattr(self, name)))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be decimal-like") from exc
            if not value.is_finite() or not Decimal("0") <= value <= Decimal("1"):
                raise ValueError(f"{name} must be finite and within [0, 1]")
            object.__setattr__(self, name, value)
        if self.best_bid > self.best_ask:
            raise ValueError("crossed quote is not cacheable")


class MarketPriceCache:
    """Bound memory while sharing fresh quotes across every leader worker."""

    def __init__(self, *, max_entries: int = 5000) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._quotes: OrderedDict[str, PriceQuote] = OrderedDict()

    def put(self, quote: PriceQuote) -> None:
        """Insert or replace a quote and evict the least recently inserted token."""

        token = quote.token_id
        self._quotes.pop(token, None)
        self._quotes[token] = quote
        while len(self._quotes) > self._max_entries:
            self._quotes.popitem(last=False)

    def get(
        self,
        token_id: str,
        *,
        now: datetime,
        max_age: timedelta,
    ) -> PriceQuote | None:
        """Return a quote only when it satisfies the explicit freshness budget."""

        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if max_age.total_seconds() < 0:
            raise ValueError("max_age must be non-negative")
        quote = self._quotes.get(str(token_id))
        if quote is None:
            return None
        if now - quote.observed_at > max_age:
            return None
        self._quotes.move_to_end(str(token_id))
        return quote

    def discard(self, token_id: str) -> bool:
        """Remove one token after resolution or subscription teardown."""

        return self._quotes.pop(str(token_id), None) is not None

    def clear(self) -> None:
        """Drop all cached quotes during a hard feed reset."""

        self._quotes.clear()

    def __len__(self) -> int:
        return len(self._quotes)


__all__ = ["MarketPriceCache", "PriceQuote"]