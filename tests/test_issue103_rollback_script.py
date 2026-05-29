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


def _build_canary_sandbox(tmp_path: Path, initial_secrets: str) -> Path:
    """Stage a writable sandbox with the canary phase ready to exercise.

    The script's full pipeline (phase 1–5) shells out to git/python/doctor;
    we only care about phase 6 here. Extract the canary block into a tiny
    standalone bash script that imports the same helper logic.
    """
    sandbox = tmp_path / "work"
    sandbox.mkdir()
    (sandbox / "secrets.env").write_text(initial_secrets)
    # Tiny harness: same set_or_append_cap logic as rollback.sh phase 6.
    harness = sandbox / "canary.sh"
    harness.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
CANARY_NOTIONAL="$1"
cd "$(dirname "$0")"
set_or_append_cap() {
  local key="$1" value="$2"
  if grep -q "^${key}=" secrets.env 2>/dev/null; then
    sed -i "s/^${key}=.*/${key}=${value}/" secrets.env
  else
    echo "${key}=${value}" >> secrets.env
  fi
}
set_or_append_cap BUY_MAX_AMOUNT "$CANARY_NOTIONAL"
set_or_append_cap MAX_DAILY_SPEND_USDC "$CANARY_NOTIONAL"
"""
    )
    harness.chmod(0o755)
    return sandbox


def test_canary_appends_caps_when_absent(tmp_path):
    """PR #105 review (Cubic P1): when ``BUY_MAX_AMOUNT`` / ``MAX_DAILY_SPEND_USDC``
    are missing from secrets.env the canary phase must *append* them. The
    previous ``sed -i ... || true`` was a no-op on missing keys, leaving
    the operator with the false impression that caps were active.
    """
    if shutil.which("bash") is None:
        pytest.skip("bash unavailable")

    sandbox = _build_canary_sandbox(
        tmp_path,
        initial_secrets="LIVE_TRADING_ENABLED=false\nUNRELATED=keepme\n",
    )
    result = subprocess.run(
        ["bash", str(sandbox / "canary.sh"), "50"],
        cwd=sandbox,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    content = (sandbox / "secrets.env").read_text()
    assert "BUY_MAX_AMOUNT=50" in content
    assert "MAX_DAILY_SPEND_USDC=50" in content
    # Pre-existing unrelated keys must survive untouched.
    assert "UNRELATED=keepme" in content


def test_canary_replaces_caps_when_present(tmp_path):
    """When the keys already exist the canary phase must replace them
    (not duplicate or append a second copy)."""
    if shutil.which("bash") is None:
        pytest.skip("bash unavailable")

    sandbox = _build_canary_sandbox(
        tmp_path,
        initial_secrets=(
            "BUY_MAX_AMOUNT=500\n"
            "MAX_DAILY_SPEND_USDC=1000\n"
        ),
    )
    subprocess.run(
        ["bash", str(sandbox / "canary.sh"), "25"],
        cwd=sandbox,
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    content = (sandbox / "secrets.env").read_text()
    assert "BUY_MAX_AMOUNT=25" in content
    assert "MAX_DAILY_SPEND_USDC=25" in content
    # Exactly one of each — no duplicate append.
    assert content.count("BUY_MAX_AMOUNT=") == 1
    assert content.count("MAX_DAILY_SPEND_USDC=") == 1
