from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml
from pydantic import SecretStr, ValidationError

from config.app_config import DiscoveryConfig, EnvValidationError, load_config
from config.env import SignatureType, secret_value

ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "1" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {
            "funder_address": ADDRESS,
            "polygon_rpc_url": "https://rpc.invalid",
        },
        "network": {"rpc_url": "https://rpc.invalid"},
    }


def test_checked_in_yaml_loads_as_immutable_typed_config() -> None:
    config = load_config(_overrides())
    assert config.execution.trading_mode == "paper"
    assert config.execution.paper_trade_only is True
    assert config.dry_run is True
    assert config.discovery.shortlist.candidate_pool_size == 250
    with pytest.raises(FrozenInstanceError):
        config.database.dsn = "changed"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        config.discovery.scheduler.enabled = True  # type: ignore[misc]


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (0, SignatureType.EOA),
        (1, SignatureType.POLY_PROXY),
        (2, SignatureType.GNOSIS_SAFE),
        (3, SignatureType.DEPOSIT_WALLET),
    ],
)
def test_all_wallet_signature_hints_are_accepted(
    raw_value: int,
    expected: SignatureType,
) -> None:
    overrides = _overrides()
    overrides["wallet"]["signature_type"] = raw_value

    config = load_config(overrides)

    assert config.wallet.signature_type == expected


def test_runtime_local_overrides_runtime_yaml(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime.yml"
    runtime.write_text((ROOT / "config/runtime.yml").read_text(), encoding="utf-8")
    local = tmp_path / "runtime.local.yml"
    local.write_text(
        yaml.safe_dump(
            {
                "wallet": {
                    "funder_address": ADDRESS,
                    "polygon_rpc_url": "https://local-rpc.invalid",
                },
                "network": {"rpc_url": "https://local-rpc.invalid"},
                "database": {"dsn": "postgresql://test:test@localhost/test"},
                "sizing": {"percentage": 7.5},
            }
        ),
        encoding="utf-8",
    )
    config = load_config(
        runtime_path=runtime,
        local_path=local,
        discovery_path=ROOT / "config/trader_discovery.yaml",
        secrets_path=tmp_path / "missing.env",
    )
    assert config.sizing.percentage == 7.5
    assert config.wallet.polygon_rpc_url == "https://local-rpc.invalid"


def test_default_local_and_secrets_paths_follow_deployment_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "runtime.local.yml").write_text(
        yaml.safe_dump({"sizing": {"percentage": 9.5}}),
        encoding="utf-8",
    )
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__DISCOVERY__API__TOKEN=deployment-token\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    monkeypatch.chdir(tmp_path)

    config = load_config(_overrides())

    assert config.sizing.percentage == 9.5
    assert config.discovery.api.token.get_secret_value() == "deployment-token"


def test_process_env_beats_mode_0600_secrets_and_yaml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__SIZING__PERCENTAGE=6\n"
        "PMCOPY__DISCOVERY__API__TOKEN=top-secret-value\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    monkeypatch.setenv("PMCOPY__SIZING__PERCENTAGE", "8")
    config = load_config(_overrides(), secrets_path=secrets)
    assert config.sizing.percentage == 8
    assert config.discovery.api.has_token is True
    assert "top-secret-value" not in repr(config.discovery.api.token)


def test_wallet_private_key_is_secret_and_clob_credentials_are_removed() -> None:
    private_key = "0x" + "a" * 64
    overrides = _overrides()
    overrides["wallet"]["private_key"] = private_key

    config = load_config(overrides)

    assert isinstance(config.wallet.private_key, SecretStr)
    assert secret_value(config.wallet.private_key) == private_key
    assert not hasattr(config.clob, "api_key")
    assert private_key not in repr(config)


def test_obsolete_clob_credentials_fail_with_migration_error() -> None:
    overrides = _overrides()
    overrides["clob"] = {"api_key": "obsolete"}

    with pytest.raises(EnvValidationError, match="clob.api_key"):
        load_config(overrides)


def test_empty_secret_entries_do_not_erase_yaml_values(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__WALLET__POLYGON_RPC_URL=\n"
        "PMCOPY__NETWORK__RPC_URL=   \n"
        "PMCOPY__DISCOVERY__API__TOKEN=\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    overrides = _overrides()
    overrides["discovery"] = {"api": {"token": "yaml-token"}}

    config = load_config(overrides, secrets_path=secrets)

    assert config.wallet.polygon_rpc_url == "https://rpc.invalid"
    assert config.network.rpc_url == "https://rpc.invalid"
    assert config.discovery.api.token.get_secret_value() == "yaml-token"


def test_secrets_file_requires_exact_0600(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text("PMCOPY__DISCOVERY__API__TOKEN=secret\n", encoding="utf-8")
    secrets.chmod(0o640)
    with pytest.raises(EnvValidationError, match="0600"):
        load_config(_overrides(), secrets_path=secrets)


def test_secrets_reject_duplicate_and_non_nested_application_keys(
    tmp_path: Path,
) -> None:
    duplicate = tmp_path / "duplicate.env"
    duplicate.write_text(
        "PMCOPY__DISCOVERY__API__TOKEN=one\n"
        "PMCOPY__DISCOVERY__API__TOKEN=two\n",
        encoding="utf-8",
    )
    duplicate.chmod(0o600)
    with pytest.raises(EnvValidationError, match="Duplicate key"):
        load_config(_overrides(), secrets_path=duplicate)

    flat = tmp_path / "flat.env"
    flat.write_text("PRIVATE_KEY=never-accepted\n", encoding="utf-8")
    flat.chmod(0o600)
    with pytest.raises(EnvValidationError, match="PMCOPY__SECTION__FIELD"):
        load_config(_overrides(), secrets_path=flat)


def test_unknown_yaml_key_fails_closed(tmp_path: Path) -> None:
    runtime = tmp_path / "runtime.yml"
    payload = yaml.safe_load((ROOT / "config/runtime.yml").read_text())
    payload["execution"]["tradeing_mode"] = "live"
    runtime.write_text(yaml.safe_dump(payload), encoding="utf-8")

    with pytest.raises(EnvValidationError, match="tradeing_mode"):
        load_config(
            _overrides(),
            runtime_path=runtime,
            discovery_path=ROOT / "config/trader_discovery.yaml",
            secrets_path=tmp_path / "missing.env",
        )


def test_unknown_nested_process_key_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PMCOPY__EXECUTION__TRADEING_MODE", "live")
    with pytest.raises(EnvValidationError, match="tradeing_mode"):
        load_config(_overrides())


def test_nested_process_json_decodes_wallet_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wallet = "0x" + "2" * 40
    monkeypatch.setenv("PMCOPY__LEADER__WALLETS", f'["{wallet}"]')
    config = load_config(_overrides())
    assert config.leader.wallets == (wallet,)


def test_runtime_network_rpc_url_is_required() -> None:
    overrides = _overrides()
    overrides["network"]["rpc_url"] = ""
    with pytest.raises(EnvValidationError, match="network.rpc_url is required"):
        load_config(overrides)


def test_secrets_template_omits_runtime_derived_clob_auth() -> None:
    template = (ROOT / "secrets.env.example").read_text(encoding="utf-8")
    assert "PMCOPY__WALLET__PRIVATE_KEY=" in template
    assert "PMCOPY__WALLET__FUNDER_ADDRESS=" not in template
    assert "PMCOPY__CLOB__API_KEY=" not in template
    assert "PMCOPY__CLOB__API_SECRET=" not in template
    assert "PMCOPY__CLOB__API_PASSPHRASE=" not in template


def test_validation_error_never_echoes_secret(
    tmp_path: Path,
) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text(
        "PMCOPY__DISCOVERY__API__TOKEN=never-print-this\n"
        "PMCOPY__DISCOVERY__API__MAX_RETRIES=not-an-int\n",
        encoding="utf-8",
    )
    secrets.chmod(0o600)
    with pytest.raises(EnvValidationError) as raised:
        load_config(_overrides(), secrets_path=secrets)
    assert "never-print-this" not in str(raised.value)


def test_flat_legacy_override_is_rejected() -> None:
    with pytest.raises(EnvValidationError, match="Flat environment overrides"):
        load_config({"FUNDER_ADDRESS": ADDRESS})


def test_scoring_and_longitudinal_weights_are_validated() -> None:
    with pytest.raises(ValidationError, match="weights must sum to 1.0"):
        DiscoveryConfig.model_validate(
            {"scoring": {"weights": {"falcon_quality": 0.99}}}
        )


def test_trading_mode_is_single_immutable_selector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PMCOPY__EXECUTION__TRADING_MODE", "live")
    overrides = _overrides()
    overrides["wallet"]["private_key"] = "0x" + "a" * 64
    config = load_config(overrides)
    assert config.execution.trading_mode == "live"
    assert config.execution.paper_trade_only is False
    assert config.dry_run is False
