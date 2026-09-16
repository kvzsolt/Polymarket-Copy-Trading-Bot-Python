"""P1-p: a FAK POST that returns an order id must never be reposted — the
reconciler owns its fill accounting.

Per Context7 (Polymarket POST /order) the create-order response returns
``makingAmount`` / ``takingAmount`` + ``status`` and NEVER ``size_matched``
(that is GET-only). So ``_response_matched_size`` reads 0 from a live POST. A
FAK that partially fills and then cancels the remainder returns an order id +
``status: cancelled`` with a partial ``makingAmount`` — seen as matched 0 and
non-deferred, the repost path fired and re-submitted the FULL amount, double-
buying the already-filled portion. Any POST that produced an order id is
reconciler-owned and must not be reposted.
"""

from services.executionService import _is_fak_repostable_no_fill


def test_partial_fill_then_cancel_with_order_id_is_not_reposted():
    # makingAmount not parsed into size_matched -> matched reads 0, status
    # cancelled -> previously repostable (full re-buy over the partial fill).
    response = {
        "orderID": "ord-123",
        "status": "cancelled",
        "makingAmount": "3",  # partial fill the POST-parser does not read
    }
    assert _is_fak_repostable_no_fill(response) is False


def test_genuine_no_match_without_order_id_is_classified_but_policy_does_not_repost():
    # This low-level parser classification is retained for protective exits.
    # Copy-trade policy ignores it and permits only one venue POST.
    response = {"status": "cancelled", "error": "fak_no_match"}
    assert _is_fak_repostable_no_fill(response) is True


def test_explicit_fak_no_match_with_order_id_is_classified_for_exit_policy():
    # A genuine FAK no-match carries an explicit no-match error (nothing
    # filled), so it stays on the bounded-retry path even with an order id —
    # only a bare 'cancelled' (possible partial fill) is deferred.
    response = {
        "orderID": "ord-fak-0",
        "status": "cancelled",
        "error": "fak_no_match",
        "non_retryable": True,
    }
    assert _is_fak_repostable_no_fill(response) is True


def test_deferred_ack_with_order_id_is_not_reposted():
    response = {"orderID": "ord-9", "status": "matched"}
    assert _is_fak_repostable_no_fill(response) is False


def test_ambiguous_post_failure_never_reposted():
    response = {"status": "cancelled", "ambiguous_post_failure": True}
    assert _is_fak_repostable_no_fill(response) is False
