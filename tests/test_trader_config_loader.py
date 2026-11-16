"""Canonical config/traders/<name>.yml loader contracts."""

from __future__ import annotations

import asyncio
import dataclasses
from pathlib import Path

import pytest
import yaml

from pmcopy.config.app_config import AppConfig, CopyTradingConfig, EnvValidationError, load_config
from pmcopy.config.trader_config import TraderConfig
from pmcopy.orchestrators.v1_runtime_bootstrap import _seed_tasks_from_trader_configs

WALLET = "0x" + "a" * 40
WALLET_B = "0x" + "b" * 40


def _overrides() -> dict:
    return {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {"funder_address": "0x" + "1" * 40},
        "detection": {
            "onchain": {
                "alchemy_wss_url": "wss://polygon-mainnet.g.alchemy.com/v2/test-key-123"
            }
        },
    }


def _tree(tmp_path: Path, files: dict[str, dict]) -> Path:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    for relative, payload in files.items():
        target = config_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return config_dir


def test_trader_file_defines_wallet_override_and_task(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "copy_trading.yml": {"copy_decision_min_score": 0.3},
            "traders/steady.yml": {
                "wallet": WALLET,
                "enabled": True,
                "max_entry_price_drift_pct": 9.0,
                "task": {
                    "sizing_mode": "percentage",
                    "trigger": {"enabled": True, "min_notional": 1, "max_notional": 100},
                    "sizing": {"mode": "proportional", "percentage": 7},
                },
            },
        },
    )

    config = load_config(_overrides(), config_dir=config_dir)

    assert config.copy_trading.enabled_wallets() == (WALLET,)
    assert config.copy_trading.leader_override(WALLET) is not None
    assert config.entry_price_drift_pct_for_leader(WALLET) == 9.0
    trader = config.copy_trading.trader_for_wallet(WALLET)
    assert trader is not None
    assert trader.name == "steady"
    assert trader.task is not None
    assert trader.task.sizing_mode == "proportional"
    assert trader.task.trigger.max_notional == 100
    assert trader.task.sizing.percentage == 7


def test_trader_files_are_projected_into_legacy_wallets_shape(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "traders/alpha.yml": {"wallet": WALLET, "enabled": False},
            "traders/beta.yml": {"wallet": WALLET_B, "enabled": True},
        },
    )

    config = load_config(_overrides(), config_dir=config_dir)

    assert config.copy_trading.enabled_wallets() == (WALLET_B,)
    assert [(item.wallet, item.enabled) for item in config.copy_trading.wallets] == [
        (WALLET, False),
        (WALLET_B, True),
    ]


def test_duplicate_wallet_across_trader_files_fails_closed(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "traders/alpha.yml": {"wallet": WALLET},
            "traders/beta.yml": {"wallet": WALLET},
        },
    )

    with pytest.raises(EnvValidationError, match="defined in both"):
        load_config(_overrides(), config_dir=config_dir)


def test_trader_file_without_wallet_fails_closed(tmp_path: Path) -> None:
    config_dir = _tree(tmp_path, {"traders/alpha.yml": {"enabled": True}})

    with pytest.raises(EnvValidationError, match="valid wallet address"):
        load_config(_overrides(), config_dir=config_dir)


def test_trader_task_rejects_unknown_keys(tmp_path: Path) -> None:
    config_dir = _tree(
        tmp_path,
        {
            "traders/alpha.yml": {
                "wallet": WALLET,
                "task": {"surprise": True},
            },
        },
    )

    with pytest.raises(EnvValidationError, match="surprise"):
        load_config(_overrides(), config_dir=config_dir)


class _SeedRepository:
    """Minimal durable task repository for the file-task seeding test."""

    def __init__(self) -> None:
        self.tasks: dict[str, object] = {}

    async def get(self, task_id: str) -> object | None:
        return self.tasks.get(task_id)

    async def create(self, task: object) -> None:
        self.tasks[task.task_id] = task


def test_trader_task_is_seedable_into_a_copy_task() -> None:
    """The YAML task block converts into a valid durable CopyTask seed."""

    async def scenario() -> None:
        trader = TraderConfig.model_validate(
            {
                "name": "steady",
                "wallet": WALLET,
                "enabled": True,
                "task": {"sizing_mode": "percentage", "sizing": {"percentage": 7}},
            }
        )
        cfg = dataclasses.replace(
            AppConfig(),
            copy_trading=CopyTradingConfig.model_validate({"traders": [trader]}),
        )
        repo = _SeedRepository()
        created = await _seed_tasks_from_trader_configs(
            config=cfg,
            task_repository=repo,
            contexts=[],
        )

        assert created == 1
        task = repo.tasks["steady"]
        assert task.task_id == "steady"
        assert task.leader_wallet == WALLET
        assert task.sizing_mode == "proportional"
        assert task.sizing.percentage == 7

    asyncio.run(scenario())
