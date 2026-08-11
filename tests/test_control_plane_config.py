"""Regression tests for service-owned control-plane configuration."""

from __future__ import annotations

from pathlib import Path

import yaml

import pytest

from config.app_config import (
    EnvValidationError,
    SERVICE_CONFIG_FILES,
    dump_config_payload,
    load_config,
)
from config.env import secret_value


def _overrides() -> dict[str, object]:
    """Return the minimum valid paper configuration for loader tests."""

    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {"funder_address": "0x" + "1" * 40},
        "polymarket": {"rpc_url": "https://rpc.example"},
    }


def test_control_plane_is_a_typed_service_config(tmp_path: Path) -> None:
    """The loader reads the control-plane YAML and keeps its fields typed."""

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "control_plane.yml").write_text(
        yaml.safe_dump(
            {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 9200,
                "require_authentication": True,
                "websocket_queue_capacity": 64,
                "portfolio_refresh_interval_seconds": 12.5,
                "portfolio_stale_after_seconds": 45.0,
            }
        ),
        encoding="utf-8",
    )

    overrides = _overrides()
    overrides["control_plane"] = {"auth": {"token": "test-control-token"}}
    config = load_config(overrides, config_dir=config_dir)

    assert "control_plane" in SERVICE_CONFIG_FILES
    assert config.control_plane.enabled is True
    assert config.control_plane.port == 9200
    assert config.control_plane.websocket_queue_capacity == 64
    assert config.control_plane.portfolio_refresh_interval_seconds == 12.5
    assert config.control_plane.portfolio_stale_after_seconds == 45.0


def test_control_plane_config_is_safe_to_serialize(tmp_path: Path) -> None:
    """Serialized settings include transport policy but no credential field."""

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    config = load_config(_overrides(), config_dir=config_dir)

    payload = config.control_plane
    assert not hasattr(payload, "token")
    assert payload.host == "127.0.0.1"


def test_control_plane_token_loads_only_through_secret_boundary(tmp_path: Path) -> None:
    """A mode-0600 nested secret enables authenticated server composition."""

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "control_plane.yml").write_text(
        "enabled: true\nrequire_authentication: true\n",
        encoding="utf-8",
    )
    secrets_path = tmp_path / "secrets.env"
    token = "operator-secret-token-value"
    secrets_path.write_text(
        f"PMCOPY__CONTROL_PLANE__AUTH__TOKEN={token}\n",
        encoding="utf-8",
    )
    secrets_path.chmod(0o600)

    config = load_config(
        _overrides(),
        config_dir=config_dir,
        secrets_path=secrets_path,
    )

    assert secret_value(config.control_plane.auth.token) == token
    assert token not in repr(config.control_plane)
    assert token not in yaml.safe_dump(dump_config_payload(config))


def test_enabled_authenticated_control_plane_requires_a_token(tmp_path: Path) -> None:
    """Startup fails closed when authenticated control has no credential."""

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "control_plane.yml").write_text(
        "enabled: true\nrequire_authentication: true\n",
        encoding="utf-8",
    )

    with pytest.raises(EnvValidationError, match="control_plane auth token"):
        load_config(_overrides(), config_dir=config_dir)


def test_non_loopback_control_plane_requires_authentication(tmp_path: Path) -> None:
    """An unauthenticated non-loopback bind is rejected before networking."""

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "control_plane.yml").write_text(
        "enabled: true\nhost: 0.0.0.0\nrequire_authentication: false\n",
        encoding="utf-8",
    )

    with pytest.raises(EnvValidationError, match="non-loopback"):
        load_config(_overrides(), config_dir=config_dir)


@pytest.mark.parametrize(
    ("field", "message"),
    (
        ("portfolio_refresh_interval_seconds", "portfolio refresh interval"),
        ("portfolio_stale_after_seconds", "portfolio stale interval"),
    ),
)
def test_portfolio_cache_intervals_must_be_positive(
    tmp_path: Path,
    field: str,
    message: str,
) -> None:
    """Invalid refresh policy fails before any background task is created."""

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "control_plane.yml").write_text(
        yaml.safe_dump({field: 0}),
        encoding="utf-8",
    )

    with pytest.raises(EnvValidationError, match=message):
        load_config(_overrides(), config_dir=config_dir)
