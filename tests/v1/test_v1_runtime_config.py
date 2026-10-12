"""Typed runtime configuration contracts for copy-task v1."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from pmcopy.config.v1_runtime import V1RuntimeConfig
from tests.config_helpers import load_test_config


def test_v1_runtime_defaults_are_bounded_and_safety_features_enabled() -> None:
    config = V1RuntimeConfig()

    # Paper-safe composition default: the v1 engine is an explicit opt-in.
    # The in-engine safety features below stay enabled once it is opted in.
    assert config.copy_tasks.enabled is False
    assert config.copy_tasks.workers.enabled is True
    assert config.copy_tasks.workers.queue_size == 64
    assert config.copy_tasks.workers.global_concurrency == 4
    assert config.copy_tasks.workers.max_leader_workers == 64
    assert config.copy_tasks.shared_price_cache.enabled is True
    assert config.copy_tasks.shared_price_cache.max_entries == 5000
    assert config.copy_tasks.task_budget.enabled is True
    assert config.copy_tasks.task_budget.reservation_ttl_seconds == 120
    assert config.copy_tasks.reentry_lock.enabled is True
    assert config.copy_tasks.protective_exits.enabled is True


def test_v1_runtime_rejects_unknown_or_unbounded_values() -> None:
    with pytest.raises(ValidationError):
        V1RuntimeConfig.model_validate({"copy_tasks": {"surprise": True}})
    with pytest.raises(ValidationError):
        V1RuntimeConfig.model_validate({"copy_tasks": {"workers": {"queue_size": 0}}})
    with pytest.raises(ValidationError):
        V1RuntimeConfig.model_validate({"copy_tasks": {"shared_price_cache": {"max_entries": 0}}})


def test_packaged_copy_trading_yaml_copy_tasks_is_paper_safe() -> None:
    """The packaged copy_trading.yml carries the v1 engine settings now."""
    payload = yaml.safe_load(
        (Path(__file__).parents[2] / "src" / "pmcopy" / "config/copy_trading.yml").read_text(encoding="utf-8")
    )
    config = V1RuntimeConfig.model_validate({"copy_tasks": payload["copy_tasks"]})

    assert config.copy_tasks.enabled is True
    assert config.copy_tasks.workers.global_concurrency == 4
    assert config.copy_tasks.task_budget.reservation_ttl_seconds == 120
    assert config.copy_tasks.reentry_lock.enabled is True

    application = load_test_config()
    assert application.execution.trading_mode == "paper"
    assert application.execution.paper_trade_only is True
