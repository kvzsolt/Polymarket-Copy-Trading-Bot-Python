"""Tracked live-environment overrides (migrated from the operator's
untracked ``config/runtime.local.yml``).

The live environment must carry exactly the operator's real non-secret live
values — ``trading_mode: live`` and the metrics endpoint — scoped to their
owning service files, and must never contain a secret. Followed leaders come
from ``config/traders/<name>.yml``. Selecting ``PMCOPY_ENVIRONMENT=live``
reproduces the legacy local overrides without the deprecation shim.
"""

from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path
from shutil import copyfile

import pytest
import yaml

from pmcopy.config.app_config import SERVICE_CONFIG_FILES, load_config

ROOT = Path(__file__).resolve().parents[1]
LIVE_DIR = ROOT / "src" / "pmcopy" / "config" / "environments" / "live"
LEADER_WALLETS = (
    "0xc4ac7dee4d55d1698e88a8160d5c61ede96b9722",
    "0x31864feb9d25dee93728c6225ba891530967e9ca",
    "0xbca08c1bc204a34f2fddbe47b438b9bd42ac9705",
)


@pytest.fixture(autouse=True)
def packaged_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Keep real service/live overlays while isolating the followed-trader fixtures."""
    config_root = tmp_path / "packaged-config"
    live_root = config_root / "environments" / "live"
    live_root.mkdir(parents=True)
    for service in SERVICE_CONFIG_FILES:
        source = ROOT / "src" / "pmcopy" / "config" / f"{service}.yml"
        if source.exists():
            copyfile(source, config_root / source.name)
        live_source = LIVE_DIR / f"{service}.yml"
        if live_source.exists():
            copyfile(live_source, live_root / live_source.name)
    traders = config_root / "traders"
    traders.mkdir()
    for index, wallet in enumerate(LEADER_WALLETS):
        name = f"synthetic-trader-{index}"
        (traders / f"{name}.yml").write_text(
            yaml.safe_dump({"name": name, "wallet": wallet, "enabled": True}),
            encoding="utf-8",
        )
    monkeypatch.setattr("pmcopy.config.app_config._CONFIG_ROOT", config_root)
    return config_root


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {
            "funder_address": "0x" + "1" * 40,
            "private_key": "0x" + "a" * 64,
        },
        "detection": {
            "onchain": {
                "alchemy_wss_url": "wss://polygon-mainnet.g.alchemy.com/v2/test-key-123"
            }
        },
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
        "min_balance_headroom_usdc",
    }
    assert set(copy_trading) == {"copy_tasks"}
    assert set(detection) == {"mode", "onchain"}
    assert detection["mode"] == "on_chain_logs_only"
    assert detection["onchain"]["pending_tx_provider"] == "alchemy"
    assert set(app_cfg) == {
        "enable_metrics_server",
        "metrics_bind_host",
        "metrics_port",
    }
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
    # Live discovery equals paper (operator decision 2026-10-11): the former
    # `scheduler.enabled: false` override was removed, so the file carries only
    # the falcon keys, which already match the packaged base values.
    assert set(discovery) == {"falcon"}
    assert discovery["falcon"]["candidate_limit"] == 50
    assert set(market_data) == {
        "rest_snapshot_max_spread_bps",
        "rest_snapshot_max_age_ms",
        "rest_snapshot_request_timeout_ms",
        "rest_snapshot_total_budget_ms",
    }


#: The operator's live profile: risk management on, every other engine off,
#: except discovery which equals the paper profile (decision 2026-10-11).
#: Each entry is a dotted path into the loaded :class:`AppConfig`.
LIVE_PROFILE_MATRIX: tuple[tuple[str, object], ...] = (
    ("execution.trading_mode", "live"),
    # Risk management: on.
    ("risk.enable_risk_mgmt", True),
    ("risk.enable_balance_tiered_policy", False),
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
    # Live discovery equals paper (operator decision 2026-10-11): the scheduler
    # override is gone, so live inherits the packaged base value like paper.
    ("discovery.scheduler.enabled", True),
    ("discovery.falcon.enabled", False),
    ("discovery.falcon.candidate_limit", 50),
    ("copy_trading.copy_tasks.enabled", False),
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


def test_live_environment_has_only_the_explicit_operator_policy_differences() -> None:
    """Pin the preserved paper/live policy differences without changing YAML."""
    paper = load_config(_overrides())
    live = load_config(_overrides(), environment="live")

    assert live.sizing == paper.sizing
    assert paper.protective_exits.enable_profit_lock is True
    assert live.protective_exits == replace(paper.protective_exits, enable_profit_lock=False)
    assert live.market_data == paper.market_data
    assert live.discovery == paper.discovery
    assert paper.copy_trading.copy_tasks.enabled is True
    assert live.copy_trading.copy_tasks == paper.copy_trading.copy_tasks.model_copy(
        update={"enabled": False}
    )
    assert paper.risk.enable_risk_mgmt is False
    assert paper.risk.risk_mark_max_spread_bps == 2000
    assert paper.risk.risk_mark_max_abs_distance == 0.03
    assert live.risk == replace(
        paper.risk,
        enable_risk_mgmt=True,
        risk_mark_max_spread_bps=0,
        risk_mark_max_abs_distance=0.05,
    )


def test_live_environment_contains_no_secret_material() -> None:
    for path in sorted(LIVE_DIR.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        assert not re.search(r"0x[0-9a-fA-F]{64}", text), path.name
        assert "private_key" not in text, path.name
        assert "token" not in text.lower(), path.name


def test_trader_files_exist_for_the_live_roster(packaged_config: Path) -> None:
    trader_files = sorted((packaged_config / "traders").glob("*.yml"))
    assert len(trader_files) == len(LEADER_WALLETS)

    config = load_config(_overrides(), environment="live")
    for wallet in LEADER_WALLETS:
        assert config.copy_trading.trader_for_wallet(wallet) is not None
        assert config.copy_trading.leader_override(wallet) is not None
