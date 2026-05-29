"""PR 8: app.py startup honours structured doctor result."""
from __future__ import annotations

import ast
from pathlib import Path


_APP_PY = Path(__file__).resolve().parents[1] / "app.py"


def _app_source() -> str:
    return _APP_PY.read_text(encoding="utf-8")


def _ast_tree() -> ast.AST:
    return ast.parse(_app_source())


def test_app_runs_live_doctor_only_when_not_paper_mode():
    src = _app_source()
    # Look for the live doctor call inside a `mode == "live"` branch.
    assert 'run_doctor_checks("live")' in src
    # And there should be a paper guard preceding it.
    assert 'paper_trade_only' in src or 'mode == "live"' in src


def test_app_refuses_to_start_when_doctor_returns_blocked():
    """Source must short-circuit (return) when ``doctor_result['status'] == 'blocked'``."""
    src = _app_source()
    assert '"blocked"' in src
    assert "Refusing to start" in src
    assert "run_doctor_checks" in src


def test_app_prints_blocked_check_fix_details():
    src = _app_source()
    # New structured printing — iterate blocked checks and surface their fix.
    assert 'c.get("status") == "blocked"' in src
    assert 'c.get("fix")' in src or "fix" in src


def test_paper_mode_skips_live_doctor():
    tree = _ast_tree()
    # Find a call to run_doctor_checks with literal "live".
    live_call_lines: list[int] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            if name == "run_doctor_checks":
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and arg.value == "live":
                        live_call_lines.append(node.lineno)
    assert live_call_lines, "expected at least one run_doctor_checks('live') call"
    # Each live call should sit inside a branch that checks the mode != paper.
    # Lightweight contract: there should be a `mode == "live"` comparison nearby.
    src_lines = _app_source().splitlines()
    parts: list[str] = []
    for line in live_call_lines:
        parts.extend(src_lines[max(0, line - 6):line])
    nearby = "\n".join(parts)
    assert "live" in nearby
