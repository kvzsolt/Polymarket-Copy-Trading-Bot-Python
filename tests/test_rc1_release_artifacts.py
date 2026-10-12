"""PR 9: RC1 release artifacts (docs + validation script)."""

from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess
import sys

import pytest
import yaml


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
        "submit_copy_trade",
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


def test_rc1_live_doctor_detects_file_backed_wallet_config(repo_root: Path) -> None:
    text = (repo_root / "scripts/rc1_check.sh").read_text(encoding="utf-8")
    assert "python -m bot_cli.main doctor --live" in text
    assert "${PMCOPY__WALLET__PRIVATE_KEY-}" not in text


def test_documented_config_templates_exist(repo_root: Path) -> None:
    """Every ``config/*.example.*`` path named in the docs must be shippable.

    The setup instructions are literal ``cp`` commands, so a template that is
    referenced but absent breaks first-run setup for every new operator.
    """
    sources = [repo_root / "README.md", *(repo_root / "docs").rglob("*.md")]
    referenced: dict[str, list[str]] = {}
    for source in sources:
        for match in re.finditer(
            r"config/[\w./-]*example[\w./-]*\.ya?ml", source.read_text(encoding="utf-8")
        ):
            referenced.setdefault(match.group(0), []).append(str(source.relative_to(repo_root)))

    assert referenced, "expected the docs to reference a config template"
    missing = {
        rel: sorted(set(where))
        for rel, where in referenced.items()
        if not (repo_root / rel).is_file()
    }
    assert not missing, f"documented config templates do not exist: {missing}"


def test_example_environment_template_is_paper_safe_and_secret_free(
    repo_root: Path,
) -> None:
    """The copied-verbatim environment template must never arm live trading."""
    path = repo_root / "config" / "environments" / "example" / "execution.yml"
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))

    assert payload["trading_mode"] == "paper"
    assert not re.search(r"0x[0-9a-fA-F]{64}", path.read_text(encoding="utf-8")), (
        "secrets belong in mode-0600 secrets.env, never in committed YAML"
    )


def test_packaged_service_files_are_paper_safe_and_secret_free(
    repo_root: Path,
) -> None:
    """Canonical service files ship paper-mode, kill-switches off, no keys."""
    execution = yaml.safe_load((repo_root / "config" / "execution.yml").read_text(encoding="utf-8"))
    app_cfg = yaml.safe_load((repo_root / "config" / "app.yml").read_text(encoding="utf-8"))
    risk = yaml.safe_load((repo_root / "config" / "risk.yml").read_text(encoding="utf-8"))
    wallet = yaml.safe_load((repo_root / "config" / "wallet.yml").read_text(encoding="utf-8"))

    assert execution["trading_mode"] == "paper"
    assert "venue_backend" not in execution
    assert "allow_experimental_ccxt_live" not in execution
    assert app_cfg["kill_switch"] is False
    assert risk["kill_switch"] is False
    assert wallet.get("private_key", "") == ""
    for path in sorted((repo_root / "config").glob("*.yml")):
        if path.name == "runtime.local.yml":  # operator-local, git-ignored
            continue
        assert not re.search(r"0x[0-9a-fA-F]{64}", path.read_text(encoding="utf-8")), (
            f"secret-shaped value in {path.name}"
        )


def test_bot_cli_module_executes_its_entrypoint(repo_root: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "bot_cli.main", "--help"],
        cwd=repo_root,
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0
    assert "usage:" in result.stdout.lower()
