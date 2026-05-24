
from domain.order_lifecycle import OrderLifecycleStatus, classify_venue_status

def test_missing_status_does_not_confirm_fill():
    status = classify_venue_status(None)
    assert status == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED

def test_empty_status_does_not_confirm_fill():
    status = classify_venue_status("")
    assert status == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED
    status_spaces = classify_venue_status("   ")
    assert status_spaces == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED

def test_success_true_delayed_status_becomes_submitted_unconfirmed():
    status = classify_venue_status("delayed")
    assert status == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED

def test_live_status_does_not_persist_fill():
    status = classify_venue_status("live")
    assert status == OrderLifecycleStatus.SUBMITTED_UNCONFIRMED

def test_matched_status_persists_fill():
    status = classify_venue_status("matched")
    assert status == OrderLifecycleStatus.FILLED

def test_filled_status_persists_fill():
    status = classify_venue_status("filled")
    assert status == OrderLifecycleStatus.FILLED

def test_unknown_status_requires_reconciliation():
    status = classify_venue_status("some_random_string_from_venue")
    assert status == OrderLifecycleStatus.UNKNOWN_UNCONFIRMED
