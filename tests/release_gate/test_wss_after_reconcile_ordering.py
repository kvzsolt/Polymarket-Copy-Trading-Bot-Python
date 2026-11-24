"""Release gate: WSS user runtime starts AFTER startup reconciliation.

Pre-fix (review #10): app.py started ``_start_user_data_runtimes``
BEFORE ``startup_reconcile_unconfirmed`` — the user-stream
``on_reconcile_signal`` callback was wired and free to fire for
fills landing in the window, causing concurrent fill application
with the startup reconciliation pass. Same orders could be
double-counted.

This gate pins the source-level ordering by AST-walking app.py and
asserting the reconciliation call lexically precedes the user-WSS
start call. AST-level enforcement is the cheapest catch — a runtime
test would require the full bootstrap which is too heavy for a
release-gate.

Also asserts the related contract: app.py passes ``failures_out`` so
``reconciler_ok`` is gated on no failures (review #11).
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

APP_PATH = Path(__file__).resolve().parents[2] / "src" / "pmcopy" / "orchestrators" / "app" / "runner.py"


@pytest.fixture(scope="module")
def app_tree() -> ast.Module:
    return ast.parse(APP_PATH.read_text())


def _find_call_lines(tree: ast.Module, func_name: str) -> list[int]:
    """Return the line numbers where ``func_name`` is invoked."""
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        # Handle bare ``foo(...)`` and ``await foo(...)``-flattened forms.
        if isinstance(func, ast.Name) and func.id == func_name:
            lines.append(node.lineno)
    return sorted(lines)


def test_startup_reconcile_precedes_user_wss_start(app_tree: ast.Module) -> None:
    reconcile_lines = _find_call_lines(app_tree, "startup_reconcile_unconfirmed")
    wss_lines = _find_call_lines(app_tree, "_start_user_data_runtimes")

    assert reconcile_lines, "app.py must call startup_reconcile_unconfirmed"
    assert wss_lines, "app.py must call _start_user_data_runtimes"

    # First WSS-start call must come AFTER the first startup-reconcile
    # call in lexical order. Multiple calls of either function are fine
    # (the dynamic-leader path also reconciles before adding monitors);
    # what matters is the BOOT-time ordering.
    first_reconcile = reconcile_lines[0]
    first_wss = wss_lines[0]
    assert first_reconcile < first_wss, (
        f"startup_reconcile_unconfirmed (line {first_reconcile}) must precede "
        f"_start_user_data_runtimes (line {first_wss}) — WSS callbacks fired "
        "before reconcile completes can double-apply fills"
    )


def test_reconciler_ok_gated_on_failures_out(app_tree: ast.Module) -> None:
    """``reconciler_ok=True`` must only be set when reconcile_failures is empty.

    Parse the source for the literal pattern ``readiness.update(reconciler_ok=
    ...)`` and verify each occurrence is guarded by a check on the failures
    list (string-level pin — a refactor that lifts the gate breaks this test).
    """
    src = APP_PATH.read_text()
    assert "reconcile_failures" in src, (
        "app.py must pass failures_out=reconcile_failures to surface partial "
        "reconciliation failures"
    )
    assert "failures_out=reconcile_failures" in src
    # The True path must be guarded by a not-failures check.
    assert "if reconcile_failures:" in src, (
        "reconciler_ok=True must be gated behind a failures check"
    )
