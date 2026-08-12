"""Config loader layer precedence, null semantics, and deployment-root
resolution — carried across the service-owned config split.

Original Phase 3.11 findings this file pins:
- discovery keys must be overridable from a deployment-local layer (now the
  legacy ``runtime.local.yml`` shim);
- ``_deep_merge`` must treat a YAML ``null`` as "unset", never as an eraser
  (root cause of a malformed ``runtime.local.yml`` bricking startup);
- secrets/local-config paths resolve against the deployment root (env var or
  marker-file walk-up), never the raw process CWD.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from config.app_config import load_config

ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "1" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {"funder_address": ADDRESS, "polygon_rpc_url": "https://rpc.invalid"},
        "polymarket": {"rpc_url": "https://rpc.invalid"},
    }


def _deployment(tmp_path: Path, local_payload: str) -> Path:
    deployment = tmp_path / "deployment"
    (deployment / "config").mkdir(parents=True)
    (deployment / "config" / "runtime.local.yml").write_text(local_payload, encoding="utf-8")
    return deployment


def test_discovery_local_override_takes_precedence_over_packaged_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``discovery.*`` key set in the legacy ``runtime.local.yml`` shim must
    override the packaged ``config/discovery.yml``."""
    deployment = _deployment(
        tmp_path,
        yaml.safe_dump({"discovery": {"falcon": {"candidate_limit": 7}}}),
    )
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config(_overrides())

    assert config.discovery.falcon.candidate_limit == 7


def test_null_local_override_does_not_erase_packaged_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A null-valued key in a local override must mean "unset" (leave the
    packaged value intact), not "erase to None". Previously a null
    ``leader.wallets`` bricked startup because it overrode the packaged
    ``[]`` with ``None``."""
    # ``wallets:`` with no value parses to null in YAML.
    deployment = _deployment(tmp_path, "leader:\n  wallets:\n")
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config(_overrides())

    # The packaged default is [] -> the null override must not erase it to
    # None (which would fail Pydantic tuple validation).
    assert config.copy_trading.wallets == ()


def test_load_config_logs_which_layers_were_found(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Operators need a visible record that the deprecated legacy shim is
    still being consulted so its planned removal cannot silently drop live
    overrides."""
    deployment = _deployment(tmp_path, "leader:\n  wallets: []\n")
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    import logging

    with caplog.at_level(logging.INFO, logger="AppConfig"):
        load_config(_overrides())

    messages = " ".join(r.message for r in caplog.records)
    assert "runtime.local" in messages
    assert "deprecated" in messages


def test_deployment_root_resolved_from_foreign_cwd_via_env_var(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Running from a foreign directory with PMCOPY_DEPLOYMENT_ROOT set must
    still find the deployment's secrets.env and legacy shim."""
    deployment = _deployment(tmp_path, "leader:\n  wallets: []\n")
    secrets = deployment / "secrets.env"
    secrets.write_text(
        "PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n"
        "PMCOPY__NETWORK__RPC_URL=https://from-secrets.invalid\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)

    foreign = tmp_path / "foreign"
    foreign.mkdir()
    monkeypatch.chdir(foreign)
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config(_overrides())

    # Legacy secret alias PMCOPY__NETWORK__RPC_URL routes to polymarket.
    assert config.polymarket.rpc_url == "https://from-secrets.invalid"
    assert config.copy_trading.wallets == ()


def test_deployment_root_marker_walk_up_from_inside_the_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the env var, the nearest ancestor containing a config marker
    or secrets.env is the deployment root."""
    deployment = _deployment(tmp_path, "sizing:\n  percentage: 9.5\n")
    secrets = deployment / "secrets.env"
    secrets.write_text("PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n", encoding="utf-8")
    secrets.chmod(0o600)
    nested = deployment / "some" / "nested" / "dir"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)

    config = load_config(_overrides())

    assert config.sizing.percentage == 9.5
