"""Value cached positions only from fresh, finite, two-sided market evidence."""

from decimal import Decimal
from math import isfinite
from typing import Protocol

from pmcopy.domain.performance import PerformanceUnits


class QuoteLike(Protocol):
    """Expose cached prices and their freshness without transport methods."""

    @property
    def best_bid(self) -> Decimal | None:
        """Return the cached best bid."""
        ...

    @property
    def best_ask(self) -> Decimal | None:
        """Return the cached best ask."""
        ...

    def is_fresh(self, max_age_seconds: float) -> bool:
        """Return whether the quote is within the configured age bound."""
        ...


class MarketQuoteSource(Protocol):
    """Read cached quotes without venue or database work."""

    def get_quote(self, token_id: str) -> QuoteLike | None:
        """Return the cached instrument quote, or missing evidence."""
        ...


class CachedMarkPolicy:
    """Share one explicit mark policy between portfolio and position PnL views."""

    def __init__(self, max_age_seconds: float) -> None:
        """Bind the existing executable-quote age limit in seconds."""
        if (
            isinstance(max_age_seconds, bool)
            or not isfinite(max_age_seconds)
            or max_age_seconds <= 0
        ):
            raise ValueError("Mark age limit must be finite and positive")
        self._max_age_seconds = max_age_seconds

    def midpoint(self, source: MarketQuoteSource | None, token_id: str) -> Decimal | None:
        """Return a fresh cached midpoint, or None when valuation evidence is unavailable.

        A midpoint is a valuation mark, not a guaranteed executable exit price.
        """
        if source is None:
            return None
        try:
            quote = source.get_quote(token_id)
            if quote is None or not quote.is_fresh(self._max_age_seconds):
                return None
            bid = PerformanceUnits.finite(quote.best_bid)
            ask = PerformanceUnits.finite(quote.best_ask)
        except Exception:
            return None
        if bid is None or ask is None or not Decimal(0) < bid <= ask < Decimal(1):
            return None
        return (bid + ask) / Decimal(2)
