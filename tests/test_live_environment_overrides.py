"""Tracked live-environment overrides (migrated from the operator's
untracked ``config/runtime.local.yml``).

The live environment must carry exactly the operator's real non-secret live
values — ``trading_mode: live``, the two leader wallets, and the metrics
endpoint — scoped to their owning service files, and must never contain a
secret. Selecting ``PMCOPY_ENVIRONMENT=live`` reproduces the legacy local
overrides without the deprecation shim.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from config.app_config import load_config

ROOT = Path(__file__).resolve().parents[1]
LIVE_DIR = ROOT / "config" / "environments" / "live"
LEADER_WALLETS = (
    "0xc4ac7dee4d55d1698e88a8160d5c61ede96b9722",
    "0x31864feb9d25dee93728c6225ba891530967e9ca",
    "0xbca08c1bc204a34f2fddbe47b438b9bd42ac9705",
)


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {
            "funder_address": "0x" + "1" * 40,
            "private_key": "0x" + "a" * 64,
        },
        "polymarket": {"rpc_url": "https://rpc.invalid"},
        # The live environment selects ``detection.mode: on_chain_confirmed``
        # with ``confirmed_source: logs_only`` (topic-filtered ``logs``
        # subscriptions on any wss:// provider), which fails closed without
        # the endpoint the operator keeps in ``secrets.env``
        # (``PMCOPY__DETECTION__ONCHAIN__PENDING_TX_WS_URL``).
        "detection": {
            "onchain": {"pending_tx_ws_url": "wss://polygon.example.invalid/v2/redacted"}
        },
        # The live environment enables the operator API, which fails closed
        # without the bearer credential the operator keeps in ``secrets.env``
        # (``PMCOPY__CONTROL_PLANE__AUTH__TOKEN``). This placeholder exists only
        # here; no live YAML file may carry credential material.
        "control_plane": {"auth": {"token": "test-control-credential"}},
    }


def test_live_environment_reproduces_the_operator_overrides() -> None:
    config = load_config(_overrides(), environment="live")

    assert config.execution.trading_mode == "live"
    assert config.execution.paper_trade_only is False
    assert config.copy_trading.enabled_wallets() == LEADER_WALLETS
    assert config.app.enable_metrics_server is True
    assert config.app.metrics_bind_host == "127.0.0.1"
    assert config.app.metrics_port == 9102


def test_live_environment_files_are_scoped_to_their_owning_service() -> None:
    """live/execution.yml carries only execution keys, etc. — no cross-service
    values hide inside another service's environment file."""
    assert {path.name for path in LIVE_DIR.glob("*.yml")} == {
        "app.yml",
        "control_plane.yml",
        "copy_trading.yml",
        "detection.yml",
        "discovery.yml",
        "execution.yml",
        "market_data.yml",
        "protective_exits.yml",
        "risk.yml",
        "sizing.yml",
    }

    execution = yaml.safe_load((LIVE_DIR / "execution.yml").read_text())
    copy_trading = yaml.safe_load((LIVE_DIR / "copy_trading.yml").read_text())
    app_cfg = yaml.safe_load((LIVE_DIR / "app.yml").read_text())
    detection = yaml.safe_load((LIVE_DIR / "detection.yml").read_text())
    control_plane = yaml.safe_load((LIVE_DIR / "control_plane.yml").read_text())
    risk = yaml.safe_load((LIVE_DIR / "risk.yml").read_text())
    protective_exits = yaml.safe_load((LIVE_DIR / "protective_exits.yml").read_text())
    sizing = yaml.safe_load((LIVE_DIR / "sizing.yml").read_text())
    discovery = yaml.safe_load((LIVE_DIR / "discovery.yml").read_text())
    market_data = yaml.safe_load((LIVE_DIR / "market_data.yml").read_text())

    assert set(execution) == {
        "trading_mode",
        "reservations_enabled",
        "enable_copy_decision_gate",
        "enable_near_resolution_gate",
        "max_book_spread_bps",
        "min_book_depth_usdc",
        "max_entry_price_drift_pct",
        "kill_switch",
    }
    assert set(copy_trading) == {"wallets", "copy_tasks"}
    assert set(detection) == {"mode", "onchain"}
    assert detection["mode"] == "on_chain_confirmed"
    assert detection["onchain"]["confirmed_source"] == "logs_only"
    assert set(app_cfg) == {
        "enable_metrics_server",
        "metrics_bind_host",
        "metrics_port",
    }
    assert set(control_plane) == {"enabled"}
    # The operator API stays off in live until its credential rotation lands;
    # nothing else in the live profile depends on it.
    assert control_plane["enabled"] is False
    assert set(risk) == {
        "enable_risk_mgmt",
        "enable_balance_tiered_policy",
        "kill_switch",
        "risk_mark_max_spread_bps",
        "risk_mark_max_abs_distance",
    }
    assert set(protective_exits) == {
        "enable_stop_loss",
        "enable_near_max_exit",
        "enable_staged_tp",
        "enable_trailing_stop",
        "enable_profit_lock",
        "enable_leader_divergence_exit",
    }
    assert set(sizing) == {"enable_trust_size_multipliers"}
    assert set(discovery) == {"scheduler", "falcon"}
    assert discovery["falcon"]["candidate_limit"] == 50
    assert set(market_data) == {
        "copy_entry_requires_rest_snapshot",
        "rest_snapshot_max_spread_bps",
        "rest_snapshot_max_age_ms",
        "rest_snapshot_request_timeout_ms",
        "rest_snapshot_total_budget_ms",
    }


def test_live_environment_keeps_the_loopback_operator_api_off() -> None:
    """The live overlay pins the control plane off without moving the bind."""

    config = load_config(_overrides(), environment="live")

    assert config.control_plane.enabled is False
    assert config.control_plane.host == "127.0.0.1"
    assert config.control_plane.port == 9103
    assert config.control_plane.require_authentication is True


def test_live_environment_control_plane_needs_no_credential_while_disabled() -> None:
    """A disabled API has nothing to authenticate; startup must not fail."""

    overrides = _overrides()
    overrides.pop("control_plane")

    config = load_config(overrides, environment="live")

    assert config.control_plane.enabled is False


#: The operator's live profile: risk management on, every other engine off.
#: Each entry is a dotted path into the loaded :class:`AppConfig`.
LIVE_PROFILE_MATRIX: tuple[tuple[str, object], ...] = (
    ("execution.trading_mode", "live"),
    # Risk management: on.
    ("risk.enable_risk_mgmt", True),
    ("risk.enable_balance_tiered_policy", True),
    ("risk.kill_switch", False),
    ("risk.risk_mark_max_spread_bps", 0),
    ("protective_exits.enable_stop_loss", True),
    ("protective_exits.enable_near_max_exit", True),
    # Every other engine: off.
    ("protective_exits.enable_staged_tp", False),
    ("protective_exits.enable_trailing_stop", False),
    ("protective_exits.enable_profit_lock", False),
    ("protective_exits.enable_leader_divergence_exit", False),
    ("execution.reservations_enabled", True),
    ("execution.enable_copy_decision_gate", False),
    ("execution.enable_near_resolution_gate", False),
    ("execution.max_book_spread_bps", 0),
    ("execution.min_book_depth_usdc", 0.0),
    ("execution.max_entry_price_drift_pct", 5.0),
    ("execution.kill_switch", False),
    ("sizing.enable_trust_size_multipliers", False),
    ("discovery.scheduler.enabled", False),
    ("discovery.falcon.enabled", False),
    ("discovery.falcon.candidate_limit", 50),
    ("copy_trading.copy_tasks.enabled", False),
    ("market_data.copy_entry_requires_rest_snapshot", False),
    ("market_data.rest_snapshot_max_spread_bps", 0),
    ("market_data.rest_snapshot_max_age_ms", 1500),
    ("market_data.rest_snapshot_request_timeout_ms", 1250),
    ("market_data.rest_snapshot_total_budget_ms", 2000),
)


@pytest.mark.parametrize(("path", "expected"), LIVE_PROFILE_MATRIX)
def test_live_profile_matrix(path: str, expected: object) -> None:
    """The live overlay resolves to the operator-approved engine matrix."""

    config = load_config(_overrides(), environment="live")

    value: object = config
    for part in path.split("."):
        value = getattr(value, part)

    assert value == expected, path
    assert type(value) is type(expected), path


def test_live_environment_restates_rather_than_diverges_from_the_base_profile() -> None:
    """The live overlay is explicit but nearly redundant.

    Every live engine switch is pinned in ``config/environments/live/`` so the
    live profile is auditable in one place, yet only the risk-mark spread gate
    actually differs from the packaged base — a smaller live/paper delta means
    fewer behaviours that exist only in production.
    """
    paper = load_config(_overrides())
    live = load_config(_overrides(), environment="live")

    assert live.sizing == paper.sizing
    assert live.protective_exits == paper.protective_exits
    assert live.market_data == paper.market_data
    assert live.discovery == paper.discovery
    assert live.copy_trading.copy_tasks == paper.copy_trading.copy_tasks
    assert live.risk != paper.risk
    assert paper.risk.risk_mark_max_spread_bps == 2000
    assert live.risk.risk_mark_max_spread_bps == 0


def test_live_environment_contains_no_secret_material() -> None:
    for path in sorted(LIVE_DIR.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"0x[0-9a-fA-F]{64}", text), path.name
        assert "private_key" not in text, path.name
        assert "token" not in text.lower(), path.name


def test_leader_override_files_exist_for_the_live_roster() -> None:
    for wallet in LEADER_WALLETS:
        path = ROOT / "config" / "leaders" / f"{wallet}.yml"
        assert path.is_file(), f"missing leader override file for {wallet}"

    config = load_config(_overrides(), environment="live")
    for wallet in LEADER_WALLETS:
        assert config.copy_trading.leader_override(wallet) is not None
