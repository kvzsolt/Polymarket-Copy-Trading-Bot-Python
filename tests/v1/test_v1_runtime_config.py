"""Typed runtime configuration contracts for copy-task v1."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from config.v1_runtime import V1RuntimeConfig


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

    assert config.copy_tasks.enabled is False
    assert config.copy_tasks.workers.global_concurrency == 4
    assert config.copy_tasks.task_budget.reservation_ttl_seconds == 120
    assert config.copy_tasks.reentry_lock.unlock_on_market_resolution is True

# ---------------------------------------------------------------------------
# Production loader (Phase B slice 2): the packaged YAML is the startup source
# of truth and must be paper-safe (copy tasks disabled) by default.
# ---------------------------------------------------------------------------

from config.env import EnvValidationError  # noqa: E402
from config.v1_runtime import load_v1_runtime_config  # noqa: E402


def test_load_v1_runtime_config_reads_packaged_yaml() -> None:
    config = load_v1_runtime_config()

    assert config.copy_tasks.workers.queue_size == 64
    assert config.copy_tasks.workers.global_concurrency == 4
    assert config.copy_tasks.task_budget.reservation_ttl_seconds == 120


def test_packaged_v1_runtime_yaml_is_paper_safe_disabled_by_default() -> None:
    """The v1 engine must be an explicit operator opt-in, never a default."""
    config = load_v1_runtime_config()

    assert config.copy_tasks.enabled is False


def test_load_v1_runtime_config_missing_file_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(EnvValidationError, match="missing"):
        load_v1_runtime_config(tmp_path / "does-not-exist.yml")


def test_load_v1_runtime_config_rejects_unknown_keys(tmp_path: Path) -> None:
    bad = tmp_path / "runtime_v1.yml"
    bad.write_text("copy_tasks:\n  surprise: true\n", encoding="utf-8")

    with pytest.raises(EnvValidationError, match="surprise"):
        load_v1_runtime_config(bad)


def test_load_v1_runtime_config_rejects_non_mapping_root(tmp_path: Path) -> None:
    bad = tmp_path / "runtime_v1.yml"
    bad.write_text("- just\n- a\n- list\n", encoding="utf-8")

    with pytest.raises(EnvValidationError, match="mapping"):
        load_v1_runtime_config(bad)


def test_load_v1_runtime_config_empty_file_yields_paper_safe_defaults(
    tmp_path: Path,
) -> None:
    empty = tmp_path / "runtime_v1.yml"
    empty.write_text("", encoding="utf-8")

    config = load_v1_runtime_config(empty)

    assert config.copy_tasks.enabled is False
