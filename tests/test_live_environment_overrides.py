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

import yaml

from config.app_config import load_config

ROOT = Path(__file__).resolve().parents[1]
LIVE_DIR = ROOT / "config" / "environments" / "live"
LEADER_WALLETS = (
    "0xc4ac7dee4d55d1698e88a8160d5c61ede96b9722",
    "0x31864feb9d25dee93728c6225ba891530967e9ca",
)


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {
            "funder_address": "0x" + "1" * 40,
            "private_key": "0x" + "a" * 64,
        },
        "polymarket": {"rpc_url": "https://rpc.invalid"},
        # The live environment selects ``detection.mode: on_chain_confirmed``,
        # which fails closed without the Alchemy wss:// endpoint the operator
        # keeps in ``secrets.env`` (``PMCOPY__DETECTION__ONCHAIN__PENDING_TX_WS_URL``).
        "detection": {
            "onchain": {"pending_tx_ws_url": "wss://polygon.example.invalid/v2/redacted"}
        },
    }


def test_live_environment_reproduces_the_operator_overrides() -> None:
    config = load_config(_overrides(), environment="live")

    assert config.execution.trading_mode == "live"
    assert config.execution.paper_trade_only is False
    assert config.copy_trading.wallets == LEADER_WALLETS
    assert config.app.enable_metrics_server is True
    assert config.app.metrics_bind_host == "127.0.0.1"
    assert config.app.metrics_port == 9102


def test_live_environment_files_are_scoped_to_their_owning_service() -> None:
    """live/execution.yml carries only execution keys, etc. — no cross-service
    values hide inside another service's environment file."""
    execution = yaml.safe_load((LIVE_DIR / "execution.yml").read_text())
    copy_trading = yaml.safe_load((LIVE_DIR / "copy_trading.yml").read_text())
    app_cfg = yaml.safe_load((LIVE_DIR / "app.yml").read_text())
    detection = yaml.safe_load((LIVE_DIR / "detection.yml").read_text())

    assert set(execution) == {"trading_mode"}
    assert set(copy_trading) == {"wallets"}
    assert set(detection) == {"mode", "onchain"}
    assert detection["mode"] == "on_chain_confirmed"
    assert set(app_cfg) == {
        "enable_metrics_server",
        "metrics_bind_host",
        "metrics_port",
    }


def test_live_environment_does_not_change_other_services() -> None:
    paper = load_config(_overrides())
    live = load_config(_overrides(), environment="live")

    assert live.sizing == paper.sizing
    assert live.risk == paper.risk
    assert live.protective_exits == paper.protective_exits
    assert live.market_data == paper.market_data
    assert live.discovery == paper.discovery


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
