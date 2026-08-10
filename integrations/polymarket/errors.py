"""Venue error taxonomy for the CLOB gateway.

Errors are split into two hierarchies:
- **RetryableVenueError**: may succeed on retry (timeouts, 5xx, rate-limits)
- **TerminalVenueError**: will never succeed on retry (auth, invalid params)

All gateway methods raise these instead of returning error dicts.
"""

from __future__ import annotations


class RetryableVenueError(Exception):
    """A venue error that may succeed on retry (timeout, 5xx, rate-limit)."""

    pass


class TerminalVenueError(Exception):
    """A venue error that will not succeed on retry (auth, invalid params)."""

    pass


class VenueUnavailableError(RetryableVenueError):
    """The venue endpoint is unreachable or returned 5xx."""

    pass


class VenueRateLimitError(RetryableVenueError):
    """The venue rate-limited the request."""

    pass


class VenueAuthError(TerminalVenueError):
    """Authentication/authorization failure at the venue."""

    pass


class VenueInvalidOrderError(TerminalVenueError):
    """The order was rejected due to invalid parameters."""

    pass


class VenueInsufficientBalanceError(VenueInvalidOrderError):
    """The venue rejected the order because the wallet balance/allowance is
    insufficient for the requested amount.

    Subclasses ``VenueInvalidOrderError`` so every existing terminal handler
    keeps catching it; the distinct type lets the risk monitor treat it as a
    position-drift signal (phantom position) instead of an opaque rejection.
    """

    pass


class VenueOrderNotFoundError(TerminalVenueError):
    """The venue reports no order with this id (HTTP 404).

    Terminal-not-retryable by HTTP semantics: re-querying the same id will
    keep returning 404. Distinct from ``VenueInvalidOrderError`` so the
    reconciler can apply the eventual-consistency guard (a just-submitted
    order can 404 briefly before the venue indexes it) before settling.
    """

    pass


class VenueOrderbookNotFoundError(TerminalVenueError):
    """The venue reports no orderbook for this token id (HTTP 404).

    This is market state, not venue health: the token is unavailable/resolved
    and retrying the same book request will not repair local market data.
    """

    pass
