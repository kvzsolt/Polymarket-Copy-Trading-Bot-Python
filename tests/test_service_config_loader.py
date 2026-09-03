"""Service-owned configuration loader contracts (Phase B slice 3).

Layer precedence, lowest to highest:
typed defaults -> config/<service>.yml -> legacy runtime.local.yml shim ->
config/environments/<PMCOPY_ENVIRONMENT>/<service>.yml -> config/traders/*.yml
(canonical followed-leader definitions) -> legacy config/leaders/*.yml
fallback -> nested test/composition overrides -> secrets.env (mode 0600) ->
process PMCOPY__* environment. The environment is selected explicitly
(default "paper"), never inferred from credential presence.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

from config.app_config import (
    EnvValidationError,
    dump_config_payload,
    load_config,
)

ADDRESS = "0x" + "1" * 40
LEADER_A = "0x" + "a" * 40
LEADER_B = "0x" + "b" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {"funder_address": ADDRESS},
        "polymarket": {"rpc_url": "https://rpc.invalid"},
    }


def _tree(tmp_path: Path, files: dict[str, dict]) -> Path:
    """Write a temporary service-config tree and return its config dir."""
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    for relative, payload in files.items():
        target = config_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return config_dir


def test_canonical_service_files_load(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "execution.yml": {"trading_mode": "paper", "slippage_tolerance": 3.5},
            "sizing.yml": {"percentage": 7.5},
            "risk.yml": {"max_open_positions": 9},
        },
    )

    config = load_config(_overrides(), config_dir=config_dir)

    assert config.execution.trading_mode == "paper"
    assert config.execution.slippage_tolerance == 3.5
    assert config.sizing.percentage == 7.5
    assert config.risk.max_open_positions == 9


def test_environment_override_beats_canonical(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "sizing.yml": {"percentage": 4.0},
            "environments/live/sizing.yml": {"percentage": 2.0},
        },
    )

    config = load_config(_overrides(), config_dir=config_dir, environment="live")

    assert config.sizing.percentage == 2.0
    assert config.environment == "live"


def test_selected_environment_is_runtime_metadata_not_service_yaml(tmp_path: Path) -> None:
    """The control plane can identify its layer without persisting a fake section."""

    config_dir = _tree(tmp_path, {})

    config = load_config(_overrides(), config_dir=config_dir, environment="paper")

    assert config.environment == "paper"
    assert "environment" not in dump_config_payload(config)


def test_live_execution_file_affects_only_execution(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "sizing.yml": {"percentage": 4.0},
            "risk.yml": {"max_open_positions": 5},
            "environments/live/execution.yml": {"trading_mode": "live"},
        },
    )
    overrides = _overrides()
    overrides["wallet"]["private_key"] = "0x" + "a" * 64

    paper = load_config(_overrides(), config_dir=config_dir)
    live = load_config(overrides, config_dir=config_dir, environment="live")

    assert paper.execution.trading_mode == "paper"
    assert live.execution.trading_mode == "live"
    assert live.sizing.percentage == paper.sizing.percentage == 4.0
    assert live.risk.max_open_positions == paper.risk.max_open_positions == 5


def test_environment_never_inferred_defaults_to_paper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Credential presence must not select an environment."""
    config_dir = _tree(
        tmp_path,
        {"environments/live/execution.yml": {"trading_mode": "live"}},
    )
    monkeypatch.delenv("PMCOPY_ENVIRONMENT", raising=False)
    overrides = _overrides()
    overrides["wallet"]["private_key"] = "0x" + "a" * 64

    config = load_config(overrides, config_dir=config_dir)

    assert config.execution.trading_mode == "paper"


def test_environment_env_var_selects_the_layer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_dir = _tree(
        tmp_path,
        {"environments/live/execution.yml": {"trading_mode": "live"}},
    )
    monkeypatch.setenv("PMCOPY_ENVIRONMENT", "live")
    overrides = _overrides()
    overrides["wallet"]["private_key"] = "0x" + "a" * 64

    config = load_config(overrides, config_dir=config_dir)

    assert config.execution.trading_mode == "live"
    assert config.execution.paper_trade_only is False
    assert config.dry_run is False


def test_unknown_environment_fails_closed(tmp_path: Path) -> None:
    config_dir = _tree(tmp_path, {"sizing.yml": {"percentage": 4.0}})

    with pytest.raises(EnvValidationError, match="environment"):
        load_config(_overrides(), config_dir=config_dir, environment="prod")


def test_secrets_populate_secret_fields_and_blank_values_are_ignored(
    tmp_path: Path,
) -> None:
    config_dir = _tree(tmp_path, {"sizing.yml": {"percentage": 4.0}})
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__DISCOVERY__API__TOKEN=top-secret-value\nPMCOPY__POLYMARKET__FALLBACK_RPC_URL=\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)

    config = load_config(_overrides(), config_dir=config_dir, secrets_path=secrets)

    assert config.discovery.api.token.get_secret_value() == "top-secret-value"
    assert "top-secret-value" not in repr(config)
    # Blank secret entries never erase lower-layer values.
    assert config.polymarket.fallback_rpc_url == ""


def test_legacy_secret_aliases_keep_working_at_the_loader_boundary(
    tmp_path: Path,
) -> None:
    """The operator's existing secrets.env uses pre-split section names."""
    config_dir = _tree(tmp_path, {})
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__NETWORK__RPC_URL=https://legacy-alias.invalid\n"
        "PMCOPY__ONCHAIN_DETECTION__ONCHAIN_WS_URL=wss://legacy.invalid\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    overrides = _overrides()
    del overrides["polymarket"]

    config = load_config(overrides, config_dir=config_dir, secrets_path=secrets)

    assert config.polymarket.rpc_url == "https://legacy-alias.invalid"
    assert config.detection.onchain.onchain_ws_url == "wss://legacy.invalid"


def test_process_env_is_highest_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "sizing.yml": {"percentage": 4.0},
            "environments/live/sizing.yml": {"percentage": 2.0},
        },
    )
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY__SIZING__PERCENTAGE=6\n", encoding="utf-8")
    secrets.chmod(0o600)
    monkeypatch.setenv("PMCOPY__SIZING__PERCENTAGE", "8")

    config = load_config(
        _overrides(),
        config_dir=config_dir,
        environment="live",
        secrets_path=secrets,
    )

    assert config.sizing.percentage == 8


def test_legacy_process_env_aliases_keep_working(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_dir = _tree(tmp_path, {})
    wallet = "0x" + "2" * 40
    monkeypatch.setenv("PMCOPY__LEADER__WALLETS", f'["{wallet}"]')

    config = load_config(_overrides(), config_dir=config_dir)

    assert config.copy_trading.enabled_wallets() == (wallet,)


def test_unknown_keys_in_canonical_service_file_fail_closed(
    tmp_path: Path,
) -> None:
    config_dir = _tree(tmp_path, {"execution.yml": {"tradeing_mode": "live"}})

    with pytest.raises(EnvValidationError, match="tradeing_mode"):
        load_config(_overrides(), config_dir=config_dir)


def test_unknown_keys_in_environment_file_fail_closed(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {"environments/live/execution.yml": {"tradeing_mode": "live"}},
    )

    with pytest.raises(EnvValidationError, match="tradeing_mode"):
        load_config(_overrides(), config_dir=config_dir, environment="live")


def test_yaml_round_trip_is_lossless_without_secrets(tmp_path: Path) -> None:
    """Every service model serializes to YAML-safe values and reloads equal."""
    config_dir = _tree(
        tmp_path,
        {
            "sizing.yml": {"percentage": 7.5},
            "risk.yml": {"max_open_positions": 9},
            "protective_exits.yml": {"stop_loss_threshold": 0.2},
        },
    )
    config = load_config(_overrides(), config_dir=config_dir)

    payload = dump_config_payload(config)
    dumped = yaml.safe_dump(payload)
    reloaded_dir = _tree(tmp_path / "reloaded", {})
    reloaded = load_config(yaml.safe_load(dumped) | _overrides(), config_dir=reloaded_dir)

    assert reloaded == config


def test_config_loads_without_runtime_clients(tmp_path: Path) -> None:
    """The config layer must be importable/loadable with no venue clients."""
    config_dir = _tree(tmp_path, {"sizing.yml": {"percentage": 4.0}})

    load_config(_overrides(), config_dir=config_dir)

    assert "ccxt" not in sys.modules


def test_leader_override_beats_environment_and_canonical(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "copy_trading.yml": {"copy_decision_min_score": 0.30},
            "environments/live/copy_trading.yml": {"copy_decision_min_score": 0.40},
            f"leaders/{LEADER_A}.yml": {"copy_decision_min_score": 0.90},
            f"leaders/{LEADER_B}.yml": {},
        },
    )

    config = load_config(_overrides(), config_dir=config_dir, environment="live")

    resolved_a = config.copy_trading.resolved_for_leader(LEADER_A)
    resolved_b = config.copy_trading.resolved_for_leader(LEADER_B)
    unknown = config.copy_trading.resolved_for_leader("0x" + "c" * 40)
    assert resolved_a.copy_decision_min_score == 0.90
    assert resolved_b.copy_decision_min_score == 0.40
    assert unknown.copy_decision_min_score == 0.40


def test_leader_files_do_not_join_the_roster(tmp_path: Path) -> None:
    """Leader files hold per-leader overrides; the roster stays explicit."""
    config_dir = _tree(
        tmp_path,
        {f"leaders/{LEADER_A}.yml": {"copy_decision_min_score": 0.9}},
    )

    config = load_config(_overrides(), config_dir=config_dir)

    assert config.copy_trading.wallets == ()


def test_legacy_runtime_local_shim_is_translated_and_below_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One deprecation shim keeps an operator's untracked runtime.local.yml
    working (live mode survives an un-migrated deployment); tracked
    environment files take precedence over it."""
    config_dir = _tree(
        tmp_path,
        {"environments/live/execution.yml": {"trading_mode": "live"}},
    )
    deployment = tmp_path / "deployment"
    (deployment / "config").mkdir(parents=True)
    (deployment / "config" / "runtime.local.yml").write_text(
        yaml.safe_dump(
            {
                "execution": {"trading_mode": "live"},
                "leader": {"wallets": [LEADER_A]},
                "network": {
                    "enable_metrics_server": True,
                    "metrics_bind_host": "127.0.0.1",
                    "metrics_port": 9102,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))
    overrides = _overrides()
    overrides["wallet"]["private_key"] = "0x" + "a" * 64

    config = load_config(overrides, config_dir=config_dir)

    # Legacy sections were translated to their new owners.
    assert config.execution.trading_mode == "live"
    assert config.copy_trading.enabled_wallets() == (LEADER_A,)
    assert config.app.enable_metrics_server is True
    assert config.app.metrics_port == 9102


def test_shim_loses_to_environment_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_dir = _tree(
        tmp_path,
        {"environments/live/sizing.yml": {"percentage": 2.0}},
    )
    deployment = tmp_path / "deployment"
    (deployment / "config").mkdir(parents=True)
    (deployment / "config" / "runtime.local.yml").write_text(
        yaml.safe_dump({"sizing": {"percentage": 9.0}}), encoding="utf-8"
    )
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config(_overrides(), config_dir=config_dir, environment="live")

    assert config.sizing.percentage == 2.0


def test_obsolete_clob_credentials_fail_with_migration_error(
    tmp_path: Path,
) -> None:
    config_dir = _tree(tmp_path, {})
    overrides = _overrides()
    overrides["clob"] = {"api_key": "obsolete"}

    with pytest.raises(EnvValidationError, match="clob.api_key"):
        load_config(overrides, config_dir=config_dir)


def test_dropped_legacy_keys_are_ignored_only_in_legacy_layers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shim tolerates (and warns about) dropped legacy keys so an
    operator's old runtime.local.yml cannot brick startup, while canonical
    files remain strict."""
    config_dir = _tree(tmp_path, {})
    deployment = tmp_path / "deployment"
    (deployment / "config").mkdir(parents=True)
    (deployment / "config" / "runtime.local.yml").write_text(
        yaml.safe_dump({"execution": {"max_retries": 5}}), encoding="utf-8"
    )
    monkeypatch.setenv("PMCOPY_DEPLOYMENT_ROOT", str(deployment))

    config = load_config(_overrides(), config_dir=config_dir)

    assert not hasattr(config.execution, "max_retries")


def test_environment_selector_in_secrets_env_is_honored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """secrets.env may carry PMCOPY_ENVIRONMENT, the documented selector."""
    monkeypatch.delenv("PMCOPY_ENVIRONMENT", raising=False)
    config_dir = _tree(
        tmp_path,
        {
            "sizing.yml": {"percentage": 4.0},
            "environments/live/sizing.yml": {"percentage": 2.0},
        },
    )
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY_ENVIRONMENT=live\n", encoding="utf-8")
    secrets.chmod(0o600)

    config = load_config(_overrides(), config_dir=config_dir, secrets_path=secrets)

    assert config.sizing.percentage == 2.0


def test_explicit_environment_beats_secrets_env_selector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PMCOPY_ENVIRONMENT", raising=False)
    config_dir = _tree(
        tmp_path,
        {
            "sizing.yml": {"percentage": 4.0},
            "environments/live/sizing.yml": {"percentage": 2.0},
        },
    )
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY_ENVIRONMENT=live\n", encoding="utf-8")
    secrets.chmod(0o600)

    config = load_config(
        _overrides(), config_dir=config_dir, environment="paper", secrets_path=secrets
    )

    assert config.sizing.percentage == 4.0


def test_process_env_environment_beats_secrets_env_selector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PMCOPY_ENVIRONMENT", "paper")
    config_dir = _tree(
        tmp_path,
        {
            "sizing.yml": {"percentage": 4.0},
            "environments/live/sizing.yml": {"percentage": 2.0},
        },
    )
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY_ENVIRONMENT=live\n", encoding="utf-8")
    secrets.chmod(0o600)

    config = load_config(_overrides(), config_dir=config_dir, secrets_path=secrets)

    assert config.sizing.percentage == 4.0


def test_entry_price_drift_resolves_leader_then_copy_trading_then_execution(
    tmp_path: Path,
) -> None:
    """The FAK entry band resolves through all three configuration layers.

    Precedence, highest first: ``config/traders/<name>.yml`` (or the legacy
    ``config/leaders/<wallet>.yml`` fallback) -> ``config/copy_trading.yml``
    -> ``config/execution.yml``. An unset leader key inherits the layer below
    it rather than the typed default.
    """
    config_dir = _tree(
        tmp_path,
        {
            "execution.yml": {"max_entry_price_drift_pct": 6.0},
            "environments/live/execution.yml": {"max_entry_price_drift_pct": 8.0},
            "copy_trading.yml": {"max_entry_price_drift_pct": 9.0},
            f"leaders/{LEADER_A}.yml": {"max_entry_price_drift_pct": 12.0},
            f"leaders/{LEADER_B}.yml": {},
        },
    )

    config = load_config(_overrides(), config_dir=config_dir, environment="live")

    assert config.entry_price_drift_pct_for_leader(LEADER_A) == 12.0
    assert config.entry_price_drift_pct_for_leader(LEADER_B) == 9.0
    assert config.entry_price_drift_pct_for_leader("0x" + "c" * 40) == 9.0


def test_entry_price_drift_falls_back_to_execution_when_copy_trading_unset(
    tmp_path: Path,
) -> None:
    """With no ``copy_trading`` value the execution layer supplies the band."""
    config_dir = _tree(
        tmp_path,
        {
            "execution.yml": {"max_entry_price_drift_pct": 6.0},
            f"leaders/{LEADER_A}.yml": {},
        },
    )

    config = load_config(_overrides(), config_dir=config_dir)

    assert config.entry_price_drift_pct_for_leader(LEADER_A) == 6.0
    assert config.entry_price_drift_pct_for_leader("0x" + "c" * 40) == 6.0


@pytest.mark.parametrize("drift", [0.5, 2.0, 12.0, 25.0])
def test_entry_price_drift_accepts_values_outside_the_legacy_window(
    tmp_path: Path, drift: float
) -> None:
    """The band is operator-tunable; the old hard 5-10 clamp is gone.

    The clamp made the knob unadjustable, which is what forced every copied
    FAK to carry the same 5% band regardless of how aggressively a given
    leader sweeps the book.
    """
    config_dir = _tree(tmp_path, {"execution.yml": {"max_entry_price_drift_pct": drift}})

    config = load_config(_overrides(), config_dir=config_dir)

    assert config.execution.max_entry_price_drift_pct == drift


@pytest.mark.parametrize("drift", [0.0, -1.0, 101.0])
def test_entry_price_drift_still_rejects_nonsense_values(
    tmp_path: Path, drift: float
) -> None:
    """Tunable is not unbounded: a non-positive or absurd band fails closed."""
    config_dir = _tree(tmp_path, {"execution.yml": {"max_entry_price_drift_pct": drift}})

    with pytest.raises(EnvValidationError):
        load_config(_overrides(), config_dir=config_dir)


def test_per_leader_entry_price_drift_is_validated(tmp_path: Path) -> None:
    """A per-leader band is validated on the same path as the global one."""
    config_dir = _tree(
        tmp_path,
        {f"leaders/{LEADER_A}.yml": {"max_entry_price_drift_pct": -3.0}},
    )

    with pytest.raises(EnvValidationError):
        load_config(_overrides(), config_dir=config_dir)
