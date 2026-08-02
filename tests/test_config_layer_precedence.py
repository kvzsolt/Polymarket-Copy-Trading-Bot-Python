"""Phase 3.11 — config loader layer precedence, null semantics, and
deployment-root resolution.

The audit found:
- ``trader_discovery.yaml`` (packaged) was merged AFTER ``runtime.local.yml``,
  so no ``discovery.*`` key could be overridden from the deployment-local
  file — silently ignored. This caused the secret leak: the only editable
  path for discovery tuning was the tracked YAML.
- ``_deep_merge`` let a YAML ``null`` override a packaged ``[]`` (root cause
  of the malformed ``runtime.local.yml`` bricking startup).
- Secrets/local-config paths were CWD-relative; running from another
  directory silently dropped layers.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from config.app_config import EnvValidationError, load_config

ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "1" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {"funder_address": ADDRESS, "polygon_rpc_url": "https://rpc.invalid"},
        "network": {"rpc_url": "https://rpc.invalid"},
    }


def test_discovery_local_override_takes_precedence_over_packaged_yaml(
    tmp_path: Path,
) -> None:
    """A ``discovery.*`` key set in ``runtime.local.yml`` must override the
    packaged ``trader_discovery.yaml``. Today the packaged file merges last
    and silently wins, so this fails."""
    local = tmp_path / "runtime.local.yml"
    local.write_text(
        yaml.safe_dump({"discovery": {"heisenberg": {"candidate_limit": 7}}}),
        encoding="utf-8",
    )
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n", encoding="utf-8")
    secrets.chmod(0o600)

    config = load_config(
        _overrides(),
        local_path=local,
        discovery_path=ROOT / "config/trader_discovery.yaml",
        secrets_path=secrets,
    )

    assert config.discovery.heisenberg.candidate_limit == 7


def test_null_local_override_does_not_erase_packaged_value(
    tmp_path: Path,
) -> None:
    """A null-valued key in a local override must mean "unset" (leave the
    packaged value intact), not "erase to None". Previously a null
    ``leader.wallets`` bricked startup because it overrode the packaged
    ``[]`` with ``None``."""
    local = tmp_path / "runtime.local.yml"
    # ``wallets:`` with no value parses to null in YAML.
    local.write_text("leader:\n  wallets:\n", encoding="utf-8")
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n", encoding="utf-8")
    secrets.chmod(0o600)

    config = load_config(
        _overrides(),
        local_path=local,
        discovery_path=ROOT / "config/trader_discovery.yaml",
        secrets_path=secrets,
    )

    # The packaged default is [] -> the null override must not erase it to
    # None (which would fail Pydantic tuple validation).
    assert config.leader.wallets == ()


def test_load_config_logs_which_layers_were_found(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Operators need a visible record of which config layers loaded so a
    foreign-CWD run that silently drops secrets/local config is diagnosable."""
    local = tmp_path / "runtime.local.yml"
    local.write_text("leader:\n  wallets: []\n", encoding="utf-8")
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n", encoding="utf-8")
    secrets.chmod(0o600)

    import logging

    with caplog.at_level(logging.INFO, logger="AppConfig"):
        load_config(
            _overrides(),
            local_path=local,
            discovery_path=ROOT / "config/trader_discovery.yaml",
            secrets_path=secrets,
        )

    messages = " ".join(r.message for r in caplog.records)
    assert "runtime.local" in messages or "local" in messages.lower()
    assert "secrets" in messages.lower()


def test_deployment_root_resolved_from_foreign_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Running from a foreign directory must still find the deployment's
    secrets.env and runtime.local.yml via marker-file walk-up, not silently
    drop them."""
    deployment = tmp_path / "deployment"
    (deployment / "config").mkdir(parents=True)
    # Copy the packaged runtime.yml so all required sections are present; the
    # marker is the presence of config/runtime.yml in the deployment root.
    (deployment / "config" / "runtime.yml").write_text(
        (ROOT / "config/runtime.yml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    secrets = deployment / "secrets.env"
    secrets.write_text(
        "PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n"
        "PMCOPY__NETWORK__RPC_URL=https://from-secrets.invalid\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    local = deployment / "config" / "runtime.local.yml"
    local.write_text("leader:\n  wallets: []\n", encoding="utf-8")

    # Run from a foreign CWD with no env override.
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    monkeypatch.chdir(foreign)
    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)

    config = load_config(
        _overrides(),
        runtime_path=deployment / "config" / "runtime.yml",
        discovery_path=ROOT / "config/trader_discovery.yaml",
    )

    # The deployment-root walk-up found the secrets.env (RPC URL from secrets)
    # and the local file (empty wallets -> not bricked).
    assert config.network.rpc_url == "https://from-secrets.invalid"
    assert config.leader.wallets == ()


def test_explicit_deployment_root_env_var_wins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PMCOPY_DEPLOYMENT_ROOT explicitly pins the deployment root."""
    deployment = tmp_path / "explicit-deployment"
    (deployment / "config").mkdir(parents=True)
    secrets = deployment / "secrets.env"
    secrets.write_text(
        "PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n"
        "PMCOPY__NETWORK__RPC_URL=https://explicit.invalid\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)

    foreign = tmp_path / "foreign"
    foreign.mkdir()
    monkeypatch.chdir(foreign)
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config(
        _overrides(),
        discovery_path=ROOT / "config/trader_discovery.yaml",
    )

    assert config.network.rpc_url == "https://explicit.invalid"