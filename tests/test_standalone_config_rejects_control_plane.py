"""Standalone configuration must reject control-plane keys fail-closed.

Task 12 of the standalone refactor removes the operator control plane. The
configuration surface it owned (``control_plane`` section, the YAML files, the
``PMCOPY__CONTROL_PLANE__*`` secret/env keys) no longer exists, and a stale
operator configuration carrying any of them must fail loudly at load time
rather than be silently dropped — the same fail-closed policy the removed
``venue_backend`` keys follow.

Test function names deliberately avoid the literal section name so a tmp-path
string can never satisfy an error-message match by accident.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

from config.app_config import EnvValidationError, load_config

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]

#: Minimal paper-mode requirements so a probe's failure can only come from the
#: obsolete operator-plane key under test, never from unrelated validation.
_BASE_OVERRIDES = {
    "database": {"dsn": "postgresql://placeholder.invalid/db"},
    "wallet": {"funder_address": "0x" + "11" * 20},
}


def test_operator_plane_override_is_rejected_fail_closed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(EnvValidationError, match=r"control_plane"):
        load_config({**_BASE_OVERRIDES, "control_plane": {"enabled": True}})


def test_operator_plane_secret_key_is_rejected_fail_closed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY__CONTROL_PLANE__AUTH__TOKEN=stale-operator-token\n", encoding="utf-8")
    secrets.chmod(0o600)
    with pytest.raises(EnvValidationError, match=r"control_plane"):
        load_config(_BASE_OVERRIDES, secrets_path=str(secrets))


def test_operator_plane_env_key_is_rejected_fail_closed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PMCOPY__CONTROL_PLANE__AUTH__TOKEN", "stale-operator-token")
    try:
        with pytest.raises(EnvValidationError, match=r"control_plane"):
            load_config(_BASE_OVERRIDES)
    finally:
        monkeypatch.delenv("PMCOPY__CONTROL_PLANE__AUTH__TOKEN")


def test_error_message_names_the_removal_and_the_operator_action(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(EnvValidationError) as excinfo:
        load_config({**_BASE_OVERRIDES, "control_plane": {"enabled": True}})
    message = str(excinfo.value)
    assert "obsolete" in message or "removed" in message
    assert "control_plane" in message


def test_startup_layered_model_no_longer_carries_the_operator_plane_section():
    from config.app_config import _LayeredModel

    assert "control_plane" not in _LayeredModel.model_fields


def test_operator_plane_service_files_are_gone():
    removed = (
        REPOSITORY_ROOT / "config" / "control_plane.yml",
        REPOSITORY_ROOT / "config" / "environments" / "live" / "control_plane.yml",
        REPOSITORY_ROOT / "config" / "environments" / "example" / "control_plane.yml",
    )
    for path in removed:
        assert not path.exists(), f"{path} must not ship after control-plane removal"


def test_secrets_template_lists_no_operator_plane_credentials():
    template = (REPOSITORY_ROOT / "secrets.env.example").read_text(encoding="utf-8")
    assert "PMCOPY__CONTROL_PLANE" not in template


def _cli_help(command: list[str]) -> str:
    completed = subprocess.run(
        [sys.executable, "-m", "bot_cli.main", *command],
        capture_output=True,
        text=True,
        timeout=120,
        cwd=REPOSITORY_ROOT,
    )
    assert completed.returncode == 0, completed.stderr
    return completed.stdout + completed.stderr


def test_cli_help_surfaces_no_operator_plane():
    """``pmcopy`` entry-point help must not advertise a control plane."""

    for command in (["--help"], ["run", "--help"], ["doctor", "--help"]):
        combined = _cli_help(command)
        assert "control_plane" not in combined, command
        assert "control-plane" not in combined, command
