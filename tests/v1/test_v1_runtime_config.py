"""Typed runtime configuration contracts for copy-task v1."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from config.v1_runtime import V1RuntimeConfig


def test_v1_runtime_defaults_are_bounded_and_safety_features_enabled() -> None:
    config = V1RuntimeConfig()

    assert config.copy_tasks.enabled is True
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
        V1RuntimeConfig.model_validate(
            {"copy_tasks": {"workers": {"queue_size": 0}}}
        )
    with pytest.raises(ValidationError):
        V1RuntimeConfig.model_validate(
            {"copy_tasks": {"shared_price_cache": {"max_entries": 0}}}
        )


def test_runtime_v1_yaml_matches_typed_defaults() -> None:
    payload = yaml.safe_load(
        (Path(__file__).parents[2] / "config/runtime_v1.yml").read_text(
            encoding="utf-8"
        )
    )
    config = V1RuntimeConfig.model_validate(payload)

    assert config.copy_tasks.workers.global_concurrency == 4
    assert config.copy_tasks.task_budget.reservation_ttl_seconds == 120
    assert config.copy_tasks.reentry_lock.unlock_on_market_resolution is True