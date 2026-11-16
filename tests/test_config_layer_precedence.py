"""Config loader layer precedence, null semantics, and deployment-root
resolution — carried across the service-owned config split.

Original Phase 3.11 findings this file pins:
- discovery keys must be overridable from a deployment-local layer (now the
  legacy ``runtime.local.yml`` shim);
- ``_deep_merge`` must treat a YAML ``null`` as "unset", never as an eraser
  (root cause of a malformed ``runtime.local.yml`` bricking startup);
- secrets/local-config paths resolve against the deployment root (the
  ``PMCOPY_DEPLOYMENT_ROOT`` env var or the CWD — ancestors are never
  walked), never the raw process CWD's parents.
"""

from __future__ import annotations

from pathlib import Path
from shutil import copyfile

import pytest
import yaml

from pmcopy.config.app_config import SERVICE_CONFIG_FILES, load_config

ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "1" * 40
PACKAGED_TRADER_WALLETS = (
    "0xc4ac7dee4d55d1698e88a8160d5c61ede96b9722",
    "0x31864feb9d25dee93728c6225ba891530967e9ca",
    "0xbca08c1bc204a34f2fddbe47b438b9bd42ac9705",
)


@pytest.fixture(autouse=True)
def _isolated_packaged_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep service defaults while seeding a roster independent of operator profiles."""
    packaged_config = tmp_path / "packaged-config"
    packaged_config.mkdir()
    for service in SERVICE_CONFIG_FILES:
        source = ROOT / "src" / "pmcopy" / "config" / f"{service}.yml"
        if source.exists():
            copyfile(source, packaged_config / source.name)
    traders = packaged_config / "traders"
    traders.mkdir()
    for index, wallet in enumerate(PACKAGED_TRADER_WALLETS):
        name = f"synthetic-trader-{index}"
        (traders / f"{name}.yml").write_text(
            yaml.safe_dump({"name": name, "wallet": wallet, "enabled": True}),
            encoding="utf-8",
        )
    monkeypatch.setattr("pmcopy.config.app_config._CONFIG_ROOT", packaged_config)


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {"funder_address": ADDRESS},
        "detection": {
            "onchain": {
                "alchemy_wss_url": "wss://polygon-mainnet.g.alchemy.com/v2/test-key-123"
            }
        },
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

    # The packaged trader files supply the roster; the null override must not
    # erase it to None (which would fail Pydantic tuple validation).
    assert config.copy_trading.enabled_wallets() == PACKAGED_TRADER_WALLETS


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
    secrets_rpc = "https://polygon-mainnet.g.alchemy.com/v2/from-secrets-key-456"
    secrets.write_text(
        "PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n"
        f"PMCOPY__NETWORK__RPC_URL={secrets_rpc}\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)

    foreign = tmp_path / "foreign"
    foreign.mkdir()
    monkeypatch.chdir(foreign)
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config(_overrides())

    # Legacy secret alias PMCOPY__NETWORK__RPC_URL routes to polymarket.
    assert config.polymarket.rpc_url == secrets_rpc
    assert config.copy_trading.enabled_wallets() == PACKAGED_TRADER_WALLETS


def test_deployment_root_is_cwd_without_the_env_var(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the env var the deployment root is the CWD itself — ancestors
    are never walked (Task 19: no credential discovery from arbitrary parent
    directories). A nested CWD inside a deployment no longer discovers the
    ancestor's shim or secrets; run from the deployment root or set
    ``PMCOPY_DEPLOYMENT_ROOT``."""
    deployment = _deployment(tmp_path, "sizing:\n  percentage: 9.5\n")
    secrets = deployment / "secrets.env"
    secrets.write_text("PMCOPY__DATABASE__DSN=postgresql://t:t@l/t\n", encoding="utf-8")
    secrets.chmod(0o600)
    nested = deployment / "some" / "nested" / "dir"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)
    monkeypatch.delenv("PMCOPY_DEPLOYMENT_ROOT", raising=False)

    config = load_config(_overrides())

    # Ancestor layers were not merged: the packaged default survives.
    assert config.sizing.percentage != 9.5
