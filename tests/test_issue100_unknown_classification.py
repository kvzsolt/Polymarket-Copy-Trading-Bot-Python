"""Issue #100 — unknown CLOB SDK exceptions must be ambiguous, not terminal.

If the SDK raises an exception type whose name and message do not match any
known substring (rate-limit / timeout / auth / invalid_order / 5xx), the
gateway used to classify it as ``VenueInvalidOrderError`` (terminal). That
allowed the coordinator to release the reservation and the duplicate-lock,
even though the order may have already landed on the venue.

The fix: unknown classifications now raise ``VenueUnavailableError``
(ambiguous, retryable / submitted-unconfirmed safety net) so duplicate
locks and reservations stay active until reconciliation proves the
terminal state.
"""
from __future__ import annotations

import pytest

from integrations.polymarket.clob_v2_gateway import _raise_classified
from integrations.polymarket.errors import (
    VenueInvalidOrderError,
    VenueUnavailableError,
)


class _NovelSDKException(Exception):
    """An SDK exception type the substring classifier has never seen."""


class TestUnknownExceptionAmbiguous:
    def test_unknown_exception_raises_unavailable_not_invalid(self):
        with pytest.raises(VenueUnavailableError) as exc_info:
            _raise_classified(_NovelSDKException("something unexpected"))
        assert "unknown" in str(exc_info.value).lower()

    def test_unknown_exception_not_classified_as_invalid_order(self):
        # The previous (unsafe) behavior was raising VenueInvalidOrderError;
        # this regression-guards against it ever coming back.
        with pytest.raises(Exception) as exc_info:
            _raise_classified(_NovelSDKException("upstream closed"))
        assert not isinstance(exc_info.value, VenueInvalidOrderError)

    def test_invalid_order_substring_still_terminal(self):
        # The fix must NOT widen too far — a clear invalid-order message
        # (e.g. tick-size violation) is still terminal.
        with pytest.raises(VenueInvalidOrderError):
            _raise_classified(Exception("tick size does not match"))

    def test_timeout_substring_still_retryable_ambiguous(self):
        # Timeouts remain retryable / ambiguous as before.
        with pytest.raises(VenueUnavailableError):
            _raise_classified(Exception("connection timeout after submit"))
