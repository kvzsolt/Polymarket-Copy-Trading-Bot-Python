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
        OrderLifecycleStatus.RETRYABLE_FAILED,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
        OrderLifecycleStatus.UNKNOWN_UNCONFIRMED,
        OrderLifecycleStatus.FILLED,  # Add FILLED here if venue returns sync fill
        OrderLifecycleStatus.PARTIALLY_FILLED,
        OrderLifecycleStatus.REJECTED,
        OrderLifecycleStatus.CANCELLED,
    },
    OrderLifecycleStatus.SUBMITTED_UNCONFIRMED: {
        OrderLifecycleStatus.PARTIALLY_FILLED,
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.REJECTED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
    },
    OrderLifecycleStatus.UNKNOWN_UNCONFIRMED: {
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.TERMINAL_FAILED,
    },
    OrderLifecycleStatus.RETRYABLE_FAILED: {
        OrderLifecycleStatus.RESERVED,
        OrderLifecycleStatus.SUBMITTING,
        OrderLifecycleStatus.TERMINAL_FAILED,
    },
    OrderLifecycleStatus.RECONCILIATION_REQUIRED: {
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.SUBMITTED_UNCONFIRMED,
    },
    OrderLifecycleStatus.PARTIALLY_FILLED: {
        OrderLifecycleStatus.FILLED,
        OrderLifecycleStatus.CANCELLED,
        OrderLifecycleStatus.EXPIRED,
        OrderLifecycleStatus.TERMINAL_FAILED,
        OrderLifecycleStatus.RECONCILIATION_REQUIRED,
    },
}


TERMINAL_STATUSES = {
    OrderLifecycleStatus.FILLED,
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
