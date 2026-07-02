"""P1-q: an 'unmatched' marketable order is resting on the book (live), not a
terminal failure.

Context7 (Polymarket order statuses): ``unmatched`` = "marketable order placed
on the book after the delay expired without a match" — it is live / resting.
It was in none of the reconciler's classification sets, so it fell through to
the ``("failed","failed")`` default and a live resting order was settled as
failed (releasing its reservation and unlocking the trade key for a duplicate).
It must be classified non-terminal so the reconciler keeps polling via the
order id.
"""

from services.execution.reconciliation_service import (
    _FINAL_FILL_STATUSES_TRADE,
    _VENUE_NON_TERMINAL,
    _VENUE_TO_LIFECYCLE,
)


def test_unmatched_is_non_terminal():
    assert "unmatched" in _VENUE_NON_TERMINAL


def test_unmatched_maps_to_live_lifecycle():
    assert "unmatched" in _VENUE_TO_LIFECYCLE
    local_status, lifecycle = _VENUE_TO_LIFECYCLE["unmatched"]
    assert local_status == "order_live"
    assert lifecycle == "live"


def test_unmatched_does_not_fall_through_to_failed_default():
    # ("failed","failed") is the reconciler's default for an unmapped status.
    assert _VENUE_TO_LIFECYCLE.get("unmatched", ("failed", "failed")) != ("failed", "failed")


def test_unmatched_is_not_a_final_fill():
    assert "unmatched" not in _FINAL_FILL_STATUSES_TRADE
