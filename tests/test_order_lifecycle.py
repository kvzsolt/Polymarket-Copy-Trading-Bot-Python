from pmcopy.domain.order_lifecycle import (
    IntentReason,
    IntentSide,
    OrderIntentKey,
    OrderLifecycleStatus,
    classify_venue_status,
)


def _intent_key(source_event_id: str | None) -> OrderIntentKey:
    return OrderIntentKey(
        user_id="0xUser",
        leader_wallet="0xLeader",
        condition_id="0xcond",
        token_id="0xtok",
        side=IntentSide.BUY,
        reason=IntentReason.COPY_BUY,
        source_event_id=source_event_id,
    )


def test_stable_key_distinct_source_events_reserve_independently():
    # Two *different* leader trades on the same (condition, token, side,
    # reason) must produce distinct reservation keys so the second copy is
    # not dropped as ``reservation_conflict``.
    key_a = _intent_key("0xtxAAA").stable_key()
    key_b = _intent_key("0xtxBBB").stable_key()
    assert key_a != key_b


def test_stable_key_same_source_event_still_collides():
    # Retries of the *same* trade carry the same source id, so they MUST
    # collide on the reservation key and remain deduped.
    assert _intent_key("0xtxAAA").stable_key() == _intent_key("0xtxAAA").stable_key()


def test_legacy_stable_key_is_six_field_prefix_of_new_key():
    # The transitional dual-read keys off the 6-field legacy format. With a
    # source id present, the new key is exactly the legacy key plus the
    # ``|{source_event_id}`` component.
    key = _intent_key("0xtxAAA")
    legacy = key.legacy_stable_key()
    assert legacy.count("|") == 5  # six components
    assert key.stable_key() == f"{legacy}|0xtxAAA"
    # Legacy form is independent of the per-trade id.
    assert _intent_key("0xtxBBB").legacy_stable_key() == legacy


def test_stable_key_none_source_is_scope_consistent():
    # Within a single deployment, omitting the per-trade id keeps the legacy
    # scoped behaviour: one active reservation per
    # (user, leader, condition, token, side, reason). NOTE: this is a
    # within-version consistency guarantee, not cross-deployment format
    # stability — the trailing empty component changes the stored
    # ``order_intent_key`` format, so a rollout must drain in-flight
    # reservations (they self-heal within RESERVATION_TTL_SECONDS).
    assert _intent_key(None).stable_key() == _intent_key(None).stable_key()
    assert _intent_key(None).stable_key().endswith("copy_buy|")


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
