"""Unit tests for venue status classification — P0 safety invariant."""
import pytest
from domain.order_lifecycle import classify_venue_status, OrderLifecycleStatus

def test_missing_status_does_not_confirm_fill():
    assert classify_venue_status(None) == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED

def test_empty_status_does_not_confirm_fill():
    assert classify_venue_status("") == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED

def test_success_true_delayed_status_becomes_submitted_unconfirmed():
    assert classify_venue_status("delayed") == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED

def test_live_status_does_not_persist_fill():
    assert classify_venue_status("live") == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED

def test_matched_status_persists_fill():
    assert classify_venue_status("matched") == OrderLifecycleStatus.FILLED

def test_filled_status_persists_fill():
    assert classify_venue_status("filled") == OrderLifecycleStatus.FILLED

def test_unknown_status_requires_reconciliation():
    assert classify_venue_status("something_weird") == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED

def test_cancelled_status_is_terminal_failed():
    assert classify_venue_status("cancelled") == OrderLifecycleStatus.TERMINAL_FAILED

def test_expired_status_is_terminal_failed():
    assert classify_venue_status("expired") == OrderLifecycleStatus.TERMINAL_FAILED

def test_rejected_status_is_terminal_failed():
    assert classify_venue_status("rejected") == OrderLifecycleStatus.TERMINAL_FAILED

def test_pending_status_is_submitted_unconfirmed():
    assert classify_venue_status("pending") == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED

def test_open_status_is_submitted_unconfirmed():
    assert classify_venue_status("open") == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED

def test_submitted_status_is_submitted_unconfirmed():
    assert classify_venue_status("submitted") == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED

def test_case_insensitive():
    assert classify_venue_status("FILLED") == OrderLifecycleStatus.FILLED
    assert classify_venue_status("Live") == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
