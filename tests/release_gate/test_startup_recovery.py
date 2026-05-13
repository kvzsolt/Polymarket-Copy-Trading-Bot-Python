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


# ---------------------------------------------------------------------------
# PR 4 — startup ordering pin
# ---------------------------------------------------------------------------

def _line_of_first_call(tree, qualname_part: str) -> int | None:
    """Return the source line of the first Call whose func name contains the substring."""
    import ast

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = ""
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr
            if qualname_part in name:
                return node.lineno
    return None


def test_app_main_calls_startup_reconcile_before_monitors() -> None:
    """``app.main`` must await ``startup_reconcile_unconfirmed`` before
    scheduling the risk monitor, near-one-exit tasks, or leader monitor tasks.

    Parsed as AST so the gate stays cheap and resilient to call-site moves.
    """
    import ast
    from pathlib import Path

    source = (Path(__file__).resolve().parents[2] / "app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    reconcile_line = _line_of_first_call(tree, "startup_reconcile_unconfirmed")
    assert reconcile_line is not None, "app.py must call startup_reconcile_unconfirmed"

    # Look for monitor scheduling: risk_monitor_loop_for_contexts,
    # NearOneExitService(...), run_leader_monitor.
    risk_line = _line_of_first_call(tree, "risk_monitor_loop_for_contexts")
    near_one_line = _line_of_first_call(tree, "NearOneExitService")
    leader_line = _line_of_first_call(tree, "run_leader_monitor")

    for label, line in [
        ("risk_monitor_loop_for_contexts", risk_line),
        ("NearOneExitService", near_one_line),
        ("run_leader_monitor", leader_line),
    ]:
        assert line is not None, f"app.py must schedule {label}"
        assert reconcile_line < line, (
            f"startup_reconcile_unconfirmed (line {reconcile_line}) must precede "
            f"{label} (line {line}) in app.py"
        )
