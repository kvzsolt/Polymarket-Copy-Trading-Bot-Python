"""Regression contracts for ambiguous submission retry safety.

Only a pre-POST lock may expire by time. Once a POST may have reached the
venue, reconciliation or an explicit operator decision must resolve it.
"""

from __future__ import annotations

from pmcopy.services.execution.submission import (
    is_pending_submission_expired,
    is_retry_blocking_existing,
)


def test_submitted_unconfirmed_without_order_id_never_expires_by_time() -> None:
    """Keep a response-lost POST duplicate-blocking after its old TTL."""

    now = 10_000.0
    existing = {
        "status": "submitted_unconfirmed",
        "order_id": None,
        "response": {
            "status": "submitted_unconfirmed",
            "pending_since": 1.0,
            "pending_expires_at": 121.0,
        },
    }

    assert is_pending_submission_expired(existing, now_ts=now) is False
    assert is_retry_blocking_existing(existing, now_ts=now) is True


def test_confirmed_absent_is_the_explicit_retryable_transition() -> None:
    """Allow retry only after authoritative/operator absence resolution."""

    existing = {
        "status": "confirmed_absent",
        "order_id": None,
        "response": {"status": "confirmed_absent"},
    }

    assert is_pending_submission_expired(existing, now_ts=10_000.0) is False
    assert is_retry_blocking_existing(existing, now_ts=10_000.0) is False
