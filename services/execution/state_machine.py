"""Order lifecycle state machine (PR2 hardening: Phase 9).

The :class:`OrderLifecycleStatus` enum (``domain/order_lifecycle.py``) already
defines every state the deep-research reconciliation sketch names — the enum
was extended in an earlier phase but ``ALLOWED_TRANSITIONS`` was never updated
to include transitions for the newer states. PR2 closes that gap so the
reconciler can exercise the full INTENT_CREATED -> ... -> CONFIRMED ladder
and the AMBIGUOUS_HELD -> SAFE_RELEASED path without tripping the state
machine.

Mapping (deep-research sketch -> existing enum):

  INTENT_CREATED     -> CREATED
  SUBMITTED          -> SUBMITTING / SUBMITTED_UNCONFIRMED
  ACKED_LIVE         -> ORDER_LIVE
  ACKED_DELAYED      -> SUBMITTED_DELAYED
  MATCHED            -> MATCHED_PENDING_CONFIRMATION
  MINED              -> TRADE_MINED
  CONFIRMED          -> TRADE_CONFIRMED
  RETRYING           -> TRADE_RETRYING
  AMBIGUOUS_HELD     -> AMBIGUOUS_TIMEOUT
  SAFE_RELEASED      -> AUTO_RELEASED_AMBIGUOUS
"""

from domain.order_lifecycle import OrderLifecycleStatus

ALLOWED_TRANSITIONS: dict[OrderLifecycleStatus, set[OrderLifecycleStatus]] = {
    OrderLifecycleStatus.CREATED: {
        OrderLifecycleStatus.RESERVED,
        OrderLifecycleStatus.TERMINAL_FAILED,
    },
    OrderLifecycleStatus.RESERVED: {
        OrderLifecycleStatus.SUBMITTING,
        OrderLifecycleStatus.TERMINAL_FAILED,
    },
    OrderLifecycleStatus.SUBMITTING: {
        OrderLifecycleStatus.SUBMITTED_UNCONFIRMED,
        OrderLifecycleStatus.SUBMITTED_DELAYED,
        OrderLifecycleStatus.ORDER_LIVE,
        OrderLifecycleStatus.RETRYABLE_FAILED,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
        OrderLifecycleStatus.UNKNOWN_UNCONFIRMED,
        OrderLifecycleStatus.FILLED,  # venue returns sync fill
        OrderLifecycleStatus.PARTIALLY_FILLED,
        OrderLifecycleStatus.REJECTED,
        OrderLifecycleStatus.CANCELLED,
    },
    OrderLifecycleStatus.SUBMITTED_UNCONFIRMED: {
        OrderLifecycleStatus.ORDER_LIVE,
        OrderLifecycleStatus.SUBMITTED_DELAYED,
        OrderLifecycleStatus.MATCHED_PENDING_CONFIRMATION,
        OrderLifecycleStatus.PARTIALLY_FILLED,
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.REJECTED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
        OrderLifecycleStatus.AMBIGUOUS_TIMEOUT,
    },
    OrderLifecycleStatus.SUBMITTED_DELAYED: {
        OrderLifecycleStatus.ORDER_LIVE,
        OrderLifecycleStatus.MATCHED_PENDING_CONFIRMATION,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.AMBIGUOUS_TIMEOUT,
        OrderLifecycleStatus.TERMINAL_FAILED,
    },
    OrderLifecycleStatus.ORDER_LIVE: {
        OrderLifecycleStatus.MATCHED_PENDING_CONFIRMATION,
        OrderLifecycleStatus.PARTIALLY_FILLED,
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.AMBIGUOUS_TIMEOUT,
    },
    OrderLifecycleStatus.MATCHED_PENDING_CONFIRMATION: {
        OrderLifecycleStatus.TRADE_MINED,
        OrderLifecycleStatus.TRADE_RETRYING,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
        OrderLifecycleStatus.AMBIGUOUS_TIMEOUT,
        OrderLifecycleStatus.PARTIALLY_FILLED,
        OrderLifecycleStatus.FILLED,
    },
    OrderLifecycleStatus.TRADE_MINED: {
        OrderLifecycleStatus.TRADE_CONFIRMED,
        OrderLifecycleStatus.AMBIGUOUS_TIMEOUT,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleStatus.TRADE_RETRYING: {
        OrderLifecycleStatus.MATCHED_PENDING_CONFIRMATION,
        OrderLifecycleStatus.TRADE_MINED,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleStatus.TRADE_CONFIRMED: {
        # Terminal success state; no outgoing transitions. Listed for
        # documentation so readers can see the ladder terminates here.
    },
    OrderLifecycleStatus.UNKNOWN_UNCONFIRMED: {
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.AMBIGUOUS_TIMEOUT,
    },
    OrderLifecycleStatus.RETRYABLE_FAILED: {
        OrderLifecycleStatus.RESERVED,
        OrderLifecycleStatus.SUBMITTING,
        OrderLifecycleStatus.TERMINAL_FAILED,
    },
    OrderLifecycleStatus.RECONCILIATION_REQUIRED: {
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.PARTIALLY_FILLED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.SUBMITTED_UNCONFIRMED,
        OrderLifecycleStatus.ORDER_LIVE,
        OrderLifecycleStatus.MATCHED_PENDING_CONFIRMATION,
        OrderLifecycleStatus.TRADE_MINED,
        OrderLifecycleStatus.TRADE_CONFIRMED,
        OrderLifecycleStatus.AMBIGUOUS_TIMEOUT,
        OrderLifecycleStatus.AUTO_RELEASED_AMBIGUOUS,
    },
    OrderLifecycleStatus.PARTIALLY_FILLED: {
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleStatus.AMBIGUOUS_TIMEOUT: {
        # Release only when all configured probes are clean (never TTL-only).
        OrderLifecycleStatus.TRADE_CONFIRMED,
        OrderLifecycleStatus.AUTO_RELEASED_AMBIGUOUS,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
    },
    # AUTO_RELEASED_AMBIGUOUS is terminal-success for the held branch.
}


TERMINAL_STATUSES = {
    OrderLifecycleStatus.FILLED,
    OrderLifecycleStatus.TRADE_CONFIRMED,
    OrderLifecycleStatus.AUTO_RELEASED_AMBIGUOUS,
    OrderLifecycleStatus.CANCELLED,
    OrderLifecycleStatus.EXPIRED,
    OrderLifecycleStatus.REJECTED,
    OrderLifecycleStatus.TERMINAL_FAILED,
}


def assert_transition_allowed(
    old: OrderLifecycleStatus,
    new: OrderLifecycleStatus,
) -> None:
    if old in TERMINAL_STATUSES:
        raise ValueError(f"terminal order cannot transition: {old} -> {new}")

    allowed = ALLOWED_TRANSITIONS.get(old, set())
    if new not in allowed:
        raise ValueError(f"invalid order transition: {old} -> {new}")