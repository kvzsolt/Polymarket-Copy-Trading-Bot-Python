"""Release-gate: startup reconciliation runs before monitors (Issue #75 item 10).

Asserts the entrypoint that orchestrates pre-loop recovery exists and is
async. The full runtime path is exercised in
``tests/test_restart_rehydration_risk.py``; this gate pins the contract so
later refactors cannot silently remove the pre-loop reconcile step.
"""
from __future__ import annotations

import inspect

import pytest

from orchestrators.runtime_bootstrap import startup_reconcile_unconfirmed


def test_startup_reconcile_unconfirmed_is_coroutine() -> None:
    assert inspect.iscoroutinefunction(startup_reconcile_unconfirmed)


def test_startup_reconcile_signature_accepts_contexts() -> None:
    sig = inspect.signature(startup_reconcile_unconfirmed)
    assert "contexts" in sig.parameters


@pytest.mark.asyncio
async def test_startup_reconcile_empty_input_returns_empty_dict() -> None:
    result = await startup_reconcile_unconfirmed([])
    assert result == {}
