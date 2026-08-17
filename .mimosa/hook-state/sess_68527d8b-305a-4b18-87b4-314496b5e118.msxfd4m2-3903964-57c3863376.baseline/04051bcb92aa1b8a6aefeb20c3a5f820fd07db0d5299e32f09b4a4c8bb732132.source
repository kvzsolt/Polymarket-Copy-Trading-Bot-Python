"""Release-gate: reconciler owns terminal settlement (Issue #75 item 9).

CLAUDE.md mandates that ``ReconciliationService`` is the only writer of
terminal order state. This test pins:
* the terminal-mutation surface lives on ``ReconciliationService``
  (``reconcile_order``, ``reconcile_trade_record``, ``confirm_fill``,
  ``reconcile_position_drift``);
* ``ExecutionService`` does NOT expose a method named ``mark_filled`` or
  ``apply_fill`` (which would let it own terminal state).
"""

from __future__ import annotations

import inspect

from services.execution.reconciliation_service import ReconciliationService
from services.executionService import ExecutionService


REQUIRED_RECONCILER_METHODS = (
    "confirm_fill",
    "reconcile_order",
    "reconcile_trade_record",
    "reconcile_position_drift",
)


def test_reconciler_exposes_terminal_methods() -> None:
    missing = [
        name
        for name in REQUIRED_RECONCILER_METHODS
        if not callable(getattr(ReconciliationService, name, None))
    ]
    assert not missing, f"ReconciliationService is missing: {missing}"


def test_reconciler_methods_are_async() -> None:
    for name in REQUIRED_RECONCILER_METHODS:
        method = getattr(ReconciliationService, name)
        assert inspect.iscoroutinefunction(method), (
            f"ReconciliationService.{name} must be a coroutine function"
        )


def test_execution_service_does_not_own_terminal_writes() -> None:
    forbidden = ("mark_filled", "apply_fill", "settle_order", "finalize_order")
    leaked = [name for name in forbidden if hasattr(ExecutionService, name)]
    assert not leaked, (
        f"ExecutionService exposes terminal-write methods that belong to the reconciler: {leaked}"
    )
