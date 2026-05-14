"""PR 9: RC1 release artifacts (docs + validation script)."""
from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


_DOCS = (
    "docs/live_runbook.md",
    "docs/emergency_shutdown.md",
    "docs/recovery_playbook.md",
    "docs/known_limitations.md",
    "docs/rc1_validation.md",
)


def test_rc1_docs_exist(repo_root: Path) -> None:
    for rel in _DOCS:
        path = repo_root / rel
        assert path.is_file(), f"missing {rel}"


def test_runbook_mentions_doctor_live(repo_root: Path) -> None:
    text = (repo_root / "docs/live_runbook.md").read_text(encoding="utf-8")
    assert "doctor" in text.lower()
    assert "--live" in text


def test_emergency_doc_mentions_kill_switch_and_shutdown(repo_root: Path) -> None:
    text = (repo_root / "docs/emergency_shutdown.md").read_text(encoding="utf-8")
    assert "KILL_SWITCH" in text
    assert "ambiguous_timeout" in text


def test_recovery_doc_mentions_reconcile_and_drift(repo_root: Path) -> None:
    text = (repo_root / "docs/recovery_playbook.md").read_text(encoding="utf-8")
    assert "startup_reconcile_unconfirmed" in text
    assert "startup_drift_check" in text
    assert "auto_released_ambiguous" in text


def test_known_limitations_lists_expected_items(repo_root: Path) -> None:
    text = (repo_root / "docs/known_limitations.md").read_text(encoding="utf-8")
    lowered = text.lower()
    for token in (
        "http polling",
        "execute_intent",
        "risk_monitor_loop",
        "external",
    ):
        assert token in lowered, token


def test_rc1_validation_doc_includes_forbid_scans(repo_root: Path) -> None:
    text = (repo_root / "docs/rc1_validation.md").read_text(encoding="utf-8")
    for token in (
        "services.price_cache",
        "PositionPriceCache",
        "MarketMetaService",
        "execution_service.execute",
        "compileall",
        "pytest",
        "doctor",
    ):
        assert token in text, token


def test_rc1_check_script_exists_and_executable(repo_root: Path) -> None:
    p = repo_root / "scripts/rc1_check.sh"
    assert p.is_file()
    assert os.access(p, os.X_OK), "scripts/rc1_check.sh must be executable"


def test_rc1_check_script_runs_forbid_scans(repo_root: Path) -> None:
    text = (repo_root / "scripts/rc1_check.sh").read_text(encoding="utf-8")
    for token in (
        "services.price_cache",
        "PositionPriceCache",
        "MarketMetaService",
        "execution_coordinator_missing",
        "compileall",
        "pytest",
    ):
        assert token in text, token
