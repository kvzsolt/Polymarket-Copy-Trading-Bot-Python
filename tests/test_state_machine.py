import pytest
from pmcopy.domain.order_lifecycle import OrderLifecycleStatus
from pmcopy.services.execution.state_machine import assert_transition_allowed


def test_valid_transitions():
    assert_transition_allowed(OrderLifecycleStatus.CREATED, OrderLifecycleStatus.RESERVED)
    assert_transition_allowed(OrderLifecycleStatus.RESERVED, OrderLifecycleStatus.SUBMITTING)
    assert_transition_allowed(
        OrderLifecycleStatus.SUBMITTING, OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
    )
    assert_transition_allowed(OrderLifecycleStatus.SUBMITTING, OrderLifecycleStatus.FILLED)
    assert_transition_allowed(
        OrderLifecycleStatus.SUBMITTED_UNCONFIRMED, OrderLifecycleStatus.FILLED
    )


def test_terminal_state_cannot_transition():
    with pytest.raises(ValueError, match="terminal order cannot transition"):
        assert_transition_allowed(OrderLifecycleStatus.FILLED, OrderLifecycleStatus.CANCELLED)

    with pytest.raises(ValueError, match="terminal order cannot transition"):
        assert_transition_allowed(
            OrderLifecycleStatus.TERMINAL_FAILED, OrderLifecycleStatus.RESERVED
        )


def test_invalid_transitions():
    with pytest.raises(ValueError, match="invalid order transition"):
        assert_transition_allowed(OrderLifecycleStatus.CREATED, OrderLifecycleStatus.FILLED)

    with pytest.raises(ValueError, match="invalid order transition"):
        assert_transition_allowed(OrderLifecycleStatus.RESERVED, OrderLifecycleStatus.FILLED)
