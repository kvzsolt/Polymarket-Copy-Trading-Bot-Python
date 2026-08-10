"""Phase 9 state-machine hardening tests (PR2).

The :class:`OrderLifecycleStatus` enum already had the states the
deep-research reconciliation sketch names, but ``ALLOWED_TRANSITIONS`` did
not include transitions for several of them. PR2 extended the table so the
reconciler can exercise the full ladder and the ambiguous-hold ->
safe-release path.

These tests pin the extended transitions and the safety invariants:
  * delayed / live / matched orders can reach the terminal success states,
  * ambiguous-hold can only release to AUTO_RELEASED_AMBIGUOUS or
    TRADE_CONFIRMED (never directly to a non-terminal state),
  * AUTO_RELEASED_AMBIGUOUS and TRADE_CONFIRMED are terminal.
"""

from __future__ import annotations

import pytest
from domain.order_lifecycle import OrderLifecycleStatus
from services.execution.state_machine import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATUSES,
    assert_transition_allowed,
)

S = OrderLifecycleStatus


def test_delayed_order_can_reach_live_and_matched_and_reconcile() -> None:
    assert_transition_allowed(S.SUBMITTED_DELAYED, S.ORDER_LIVE)
    assert_transition_allowed(S.SUBMITTED_DELAYED, S.MATCHED_PENDING_CONFIRMATION)
    assert_transition_allowed(S.SUBMITTED_DELAYED, S.RECONCILIATION_REQUIRED)
    assert_transition_allowed(S.SUBMITTED_DELAYED, S.AMBIGUOUS_TIMEOUT)


def test_live_order_remains_reservable_through_matched() -> None:
    assert_transition_allowed(S.ORDER_LIVE, S.MATCHED_PENDING_CONFIRMATION)
    assert_transition_allowed(S.ORDER_LIVE, S.RECONCILIATION_REQUIRED)
    assert_transition_allowed(S.ORDER_LIVE, S.AMBIGUOUS_TIMEOUT)


def test_matched_can_reach_mined_and_retrying() -> None:
    assert_transition_allowed(S.MATCHED_PENDING_CONFIRMATION, S.TRADE_MINED)
    assert_transition_allowed(S.MATCHED_PENDING_CONFIRMATION, S.TRADE_RETRYING)
    assert_transition_allowed(S.MATCHED_PENDING_CONFIRMATION, S.AMBIGUOUS_TIMEOUT)


def test_mined_can_reach_confirmed_or_ambiguous() -> None:
    assert_transition_allowed(S.TRADE_MINED, S.TRADE_CONFIRMED)
    assert_transition_allowed(S.TRADE_MINED, S.AMBIGUOUS_TIMEOUT)


def test_retrying_can_return_to_matched_or_mined() -> None:
    assert_transition_allowed(S.TRADE_RETRYING, S.MATCHED_PENDING_CONFIRMATION)
    assert_transition_allowed(S.TRADE_RETRYING, S.TRADE_MINED)
    assert_transition_allowed(S.TRADE_RETRYING, S.TERMINAL_FAILED)


def test_ambiguous_can_release_or_confirm_or_fail() -> None:
    # Release only when all probes are clean — the transition table allows
    # AUTO_RELEASED_AMBIGUOUS, TRADE_CONFIRMED, TERMINAL_FAILED, and
    # RECONCILIATION_REQUIRED. It does NOT allow a direct jump to a
    # non-terminal "live" state.
    assert_transition_allowed(S.AMBIGUOUS_TIMEOUT, S.AUTO_RELEASED_AMBIGUOUS)
    assert_transition_allowed(S.AMBIGUOUS_TIMEOUT, S.TRADE_CONFIRMED)
    assert_transition_allowed(S.AMBIGUOUS_TIMEOUT, S.TERMINAL_FAILED)
    assert_transition_allowed(S.AMBIGUOUS_TIMEOUT, S.RECONCILIATION_REQUIRED)


def test_ambiguous_cannot_jump_directly_to_live() -> None:
    with pytest.raises(ValueError, match="invalid order transition"):
        assert_transition_allowed(S.AMBIGUOUS_TIMEOUT, S.ORDER_LIVE)


def test_auto_released_ambiguous_is_terminal() -> None:
    assert S.AUTO_RELEASED_AMBIGUOUS in TERMINAL_STATUSES
    with pytest.raises(ValueError, match="terminal order cannot transition"):
        assert_transition_allowed(S.AUTO_RELEASED_AMBIGUOUS, S.ORDER_LIVE)


def test_trade_confirmed_is_terminal() -> None:
    assert S.TRADE_CONFIRMED in TERMINAL_STATUSES
    with pytest.raises(ValueError, match="terminal order cannot transition"):
        assert_transition_allowed(S.TRADE_CONFIRMED, S.FILLED)


def test_reconciliation_can_reach_all_termini_and_intermediates() -> None:
    assert_transition_allowed(S.RECONCILIATION_REQUIRED, S.FILLED)
    assert_transition_allowed(S.RECONCILIATION_REQUIRED, S.TRADE_CONFIRMED)
    assert_transition_allowed(S.RECONCILIATION_REQUIRED, S.AUTO_RELEASED_AMBIGUOUS)
    assert_transition_allowed(S.RECONCILIATION_REQUIRED, S.TRADE_MINED)
    assert_transition_allowed(S.RECONCILIATION_REQUIRED, S.ORDER_LIVE)


def test_every_non_terminal_status_has_an_allowed_transitions_entry() -> None:
    """Every status that is not terminal must have an entry in the table."""
    non_terminal = set(S) - TERMINAL_STATUSES
    missing = [s for s in non_terminal if s not in ALLOWED_TRANSITIONS]
    assert not missing, f"Non-terminal statuses missing from ALLOWED_TRANSITIONS: {missing}"


def test_submitting_can_reach_delayed_and_live() -> None:
    assert_transition_allowed(S.SUBMITTING, S.SUBMITTED_DELAYED)
    assert_transition_allowed(S.SUBMITTING, S.ORDER_LIVE)


def test_submitted_unconfirmed_can_reach_live_and_matched() -> None:
    assert_transition_allowed(S.SUBMITTED_UNCONFIRMED, S.ORDER_LIVE)
    assert_transition_allowed(S.SUBMITTED_UNCONFIRMED, S.MATCHED_PENDING_CONFIRMATION)
    assert_transition_allowed(S.SUBMITTED_UNCONFIRMED, S.AMBIGUOUS_TIMEOUT)
