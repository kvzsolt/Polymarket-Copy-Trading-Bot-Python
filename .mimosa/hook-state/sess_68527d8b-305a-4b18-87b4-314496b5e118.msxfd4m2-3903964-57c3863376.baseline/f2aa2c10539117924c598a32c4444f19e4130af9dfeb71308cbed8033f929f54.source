"""Application-schema adapter tests for frontend-safe settings writes."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import pytest

from config.app_config import (
    AppConfig,
    AppConfigSemanticValidator,
    ControlPlaneConfig,
    DatabaseConfig,
    PolymarketConfig,
    WalletConfig,
)
from pydantic import SecretStr
from services.control_plane.contracts import SettingsPath


if TYPE_CHECKING:
    from orchestrators.control_plane_settings import AppConfigSettingsValidator


def build_config() -> AppConfig:
    """Return a semantically valid paper configuration for isolated tests."""

    rpc_url = "https://polygon-rpc.example"
    return AppConfig(
        database=DatabaseConfig(dsn="postgresql://localhost/pmcopy"),
        wallet=WalletConfig(
            private_key=SecretStr(""),
            chain_id=137,
            funder_address="0x1111111111111111111111111111111111111111",
            polygon_rpc_url=rpc_url,
        ),
        polymarket=PolymarketConfig(rpc_url=rpc_url),
    )


def build_validator() -> AppConfigSettingsValidator:
    """Return the production validator with explicit semantic validation."""

    from orchestrators.control_plane_settings import AppConfigSettingsValidator

    return AppConfigSettingsValidator(
        config=build_config(),
        environment="paper",
        semantic_validator=AppConfigSemanticValidator(),
    )


def test_settings_validator_exposes_only_frontend_safe_sections_and_keys() -> None:
    """Credential stores and arbitrary paths never enter the editable schema."""

    validator = build_validator()

    assert {path.section for path in validator.known_paths()} == {
        "app",
        "control_plane",
        "copy_trading",
        "detection",
        "discovery",
        "execution",
        "logging",
        "market_data",
        "polymarket",
        "protective_exits",
        "reconciliation",
        "risk",
        "sizing",
    }
    assert "auth" not in validator.allowed_keys(SettingsPath("paper", "control_plane"))
    assert "multi_user_config_file" not in validator.allowed_keys(SettingsPath("paper", "app"))
    assert validator.allowed_keys(SettingsPath("paper", "logging")) == frozenset(
        {"level", "log_max_bytes", "log_backup_count"}
    )
    assert "rpc_url" not in validator.allowed_keys(SettingsPath("paper", "polymarket"))
    assert "fallback_rpc_url" not in validator.allowed_keys(SettingsPath("paper", "polymarket"))


def test_settings_validator_normalizes_only_supplied_nested_values() -> None:
    """A partial nested edit is typed without expanding all runtime defaults."""

    validator = build_validator()

    normalized = validator.validate(
        SettingsPath("paper", "discovery"),
        {"shortlist": {"min_win_rate_15d": "0.55"}},
    )

    assert normalized == {"shortlist": {"min_win_rate_15d": 0.55}}


@pytest.mark.parametrize(
    ("path", "payload"),
    [
        (SettingsPath("paper", "execution"), {"trading_mode": "unsafe"}),
        (SettingsPath("paper", "risk"), {"max_open_positions": "many"}),
        (SettingsPath("paper", "execution"), {"unknown_setting": True}),
        (SettingsPath("live", "execution"), {"trading_mode": "paper"}),
    ],
)
def test_settings_validator_rejects_invalid_or_out_of_scope_updates(
    path: SettingsPath,
    payload: dict[str, object],
) -> None:
    """Invalid types, extra keys, and another environment fail closed."""

    validator = build_validator()

    with pytest.raises(ValueError):
        validator.validate(path, payload)


def test_settings_validator_applies_full_config_semantics() -> None:
    """A typed update that would create an unsafe bind still fails validation."""

    validator = build_validator()

    with pytest.raises(ValueError):
        validator.validate(
            SettingsPath("paper", "control_plane"),
            {
                "enabled": True,
                "host": "0.0.0.0",
                "require_authentication": False,
            },
        )


@pytest.mark.parametrize(
    ("path", "key_path"),
    [
        (SettingsPath("paper", "discovery"), ("api", "token")),
        (SettingsPath("paper", "polymarket"), ("rpc_url",)),
        (SettingsPath("paper", "polymarket"), ("fallback_rpc_url",)),
        (SettingsPath("paper", "control_plane"), ("auth",)),
    ],
)
def test_settings_validator_classifies_secret_backed_paths(
    path: SettingsPath,
    key_path: tuple[str, ...],
) -> None:
    """Secret-backed values are omitted even if a stale file contains them."""

    validator = build_validator()

    assert validator.is_secret(path, key_path) is True


def test_semantic_validator_rejects_unsafe_candidate_without_echoing_values() -> None:
    """The reusable semantic boundary raises an actionable sanitized error."""

    config = build_config()
    unsafe = replace(
        config,
        polymarket=PolymarketConfig(rpc_url="https://credential.example/secret"),
        control_plane=ControlPlaneConfig(
            enabled=True,
            host="0.0.0.0",
            require_authentication=False,
        ),
    )

    with pytest.raises(ValueError) as captured:
        AppConfigSemanticValidator().validate(unsafe)

    assert "non-loopback" in str(captured.value)
    assert "credential.example" not in str(captured.value)
