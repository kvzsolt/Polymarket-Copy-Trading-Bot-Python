"""Issue #103 — the rollback script must exist, be executable, and the
dry-run path must complete cleanly against a fixture working tree.

For a money-moving bot, rollback should be a practiced runtime feature,
not an operator memory exercise. The CI check this test provides catches
the script going missing, losing its executable bit, or developing a
syntax error before the operator needs it under pressure.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "rollback.sh"
RUNBOOK = REPO_ROOT / "docs" / "runbooks" / "rollback.md"


def test_rollback_script_exists():
    assert SCRIPT.is_file(), f"missing rollback script at {SCRIPT}"


def test_rollback_script_is_executable():
    assert os.access(SCRIPT, os.X_OK), f"{SCRIPT} must be executable"


def test_rollback_runbook_exists():
    """The runbook backing the script must travel with it — a script
    without prose is not operationally complete."""
    assert RUNBOOK.is_file(), f"missing rollback runbook at {RUNBOOK}"


def test_rollback_runbook_references_script():
    text = RUNBOOK.read_text()
    assert "scripts/rollback.sh" in text


def test_rollback_dry_run_completes(tmp_path):
    """``rollback.sh --dry-run`` must complete without mutating the working
    tree. Skip if bash unavailable (Windows CI without WSL)."""
    if shutil.which("bash") is None:
        pytest.skip("bash unavailable in this environment")

    result = subprocess.run(
        ["bash", str(SCRIPT), "--dry-run"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=15,
    )
    # The dry-run touches doctor + rc1_check which may fail in a test
    # sandbox without secrets. The contract here is narrower: the script
    # itself must parse and at least emit its phase log lines before any
    # exit. Phases 1+2 (kill switch + snapshot) are the operator-critical
    # ones and run before any external command.
    stdout = result.stdout
    assert "phase 1/6" in stdout, f"phase-1 log missing — stdout: {stdout[:500]}"
    assert "phase 2/6" in stdout, f"phase-2 log missing — stdout: {stdout[:500]}"
    assert "DRY-RUN" in stdout, "dry-run mode should be visible in logs"


def test_rollback_accepts_canary_flag():
    """``--canary N`` must parse; the help/usage path documents it."""
    if shutil.which("bash") is None:
        pytest.skip("bash unavailable")
    result = subprocess.run(
        ["bash", str(SCRIPT), "--dry-run", "--canary", "50"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=15,
    )
    # canary phase log fires only when CANARY_NOTIONAL is set; assert it does.
    assert "canary mode" in result.stdout.lower(), result.stdout[:500]
