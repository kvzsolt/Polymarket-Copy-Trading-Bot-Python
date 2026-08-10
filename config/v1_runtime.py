"""Strict additive runtime configuration for copy-task v1.

These pydantic models are intentionally separate from the branch's frozen
dataclass AppConfig so v1 settings can be layered in without forcing unknown
keys through the existing config schema. The composition root merges them at
startup.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from config.env import EnvValidationError


class _FrozenModel(BaseModel):
    """Shared strict immutable settings base."""

    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)


class LeaderWorkerRuntimeConfig(_FrozenModel):
    """Bounded serial-per-leader worker settings."""

    enabled: bool = True
    queue_size: int = Field(64, ge=1, le=100_000)
    global_concurrency: int = Field(4, ge=1, le=1_000)
    max_leader_workers: int = Field(64, ge=1, le=10_000)
    shutdown_timeout_seconds: float = Field(30.0, gt=0, le=600)


class SharedPriceCacheRuntimeConfig(_FrozenModel):
    """One shared bounded quote cache for every leader worker."""

    enabled: bool = True
    max_entries: int = Field(5_000, ge=1, le=1_000_000)
    max_age_seconds: float = Field(2.0, ge=0, le=300)


class TaskBudgetRuntimeConfig(_FrozenModel):
    """Task-level cap and one-entry reservation settings."""

    enabled: bool = True
    reservation_ttl_seconds: int = Field(120, ge=1, le=86_400)
    sweep_interval_seconds: float = Field(30.0, gt=0, le=3_600)


class ReentryLockRuntimeConfig(_FrozenModel):
    """Post-protective-exit entry lock settings."""

    enabled: bool = True
    unlock_on_market_resolution: bool = True
    manual_unlock_enabled: bool = True


class ProtectiveExitRuntimeConfig(_FrozenModel):
    """Per-fill protective exit lifecycle settings."""

    enabled: bool = True
    require_authoritative_fill_for_lock: bool = True


class CopyTasksRuntimeConfig(_FrozenModel):
    """Top-level copy-task v1 engine settings.

    ``enabled`` defaults to False so the composition root stays paper-safe:
    the v1 engine (workers, durable budgets, reentry locks) is an explicit
    operator opt-in via ``config/runtime_v1.yml``. The nested safety features
    default to enabled so opting in cannot silently shed a protection.
    """

    enabled: bool = False
    workers: LeaderWorkerRuntimeConfig = LeaderWorkerRuntimeConfig()
    shared_price_cache: SharedPriceCacheRuntimeConfig = SharedPriceCacheRuntimeConfig()
    task_budget: TaskBudgetRuntimeConfig = TaskBudgetRuntimeConfig()
    reentry_lock: ReentryLockRuntimeConfig = ReentryLockRuntimeConfig()
    protective_exits: ProtectiveExitRuntimeConfig = ProtectiveExitRuntimeConfig()


class V1RuntimeConfig(_FrozenModel):
    """Additive v1 settings merged into the repository runtime configuration."""

    copy_tasks: CopyTasksRuntimeConfig = CopyTasksRuntimeConfig()


_V1_RUNTIME_YAML = Path(__file__).resolve().parent / "runtime_v1.yml"


def load_v1_runtime_config(
    path: str | os.PathLike[str] | None = None,
) -> V1RuntimeConfig:
    """Load the strict v1 runtime configuration from the packaged YAML.

    Args:
        path: Optional explicit YAML path (tests/composition roots). Defaults
            to the packaged ``config/runtime_v1.yml``.

    Returns:
        The immutable validated configuration. An empty file yields the
        paper-safe typed defaults (copy tasks disabled).

    Raises:
        EnvValidationError: If the file is missing, unreadable, has a
            non-mapping root, or contains unknown/out-of-bounds keys.
    """

    file = Path(path) if path is not None else _V1_RUNTIME_YAML
    if not file.exists():
        raise EnvValidationError(f"Required configuration file is missing: {file}")
    try:
        payload = yaml.safe_load(file.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise EnvValidationError(
            f"Unable to read YAML configuration {file}: {exc}"
        ) from exc
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise EnvValidationError(f"Configuration root must be a mapping: {file}")
    try:
        return V1RuntimeConfig.model_validate(payload)
    except ValidationError as exc:
        # Render locations/messages only; never echo raw input values.
        messages = [
            ".".join(str(part) for part in error.get("loc", ()))
            + f": {error.get('msg', 'invalid value')}"
            for error in exc.errors(include_url=False, include_input=False)
        ]
        raise EnvValidationError(
            "Invalid v1 runtime configuration:\n- " + "\n- ".join(messages)
        ) from exc


__all__ = [
    "CopyTasksRuntimeConfig",
    "LeaderWorkerRuntimeConfig",
    "ProtectiveExitRuntimeConfig",
    "ReentryLockRuntimeConfig",
    "SharedPriceCacheRuntimeConfig",
    "TaskBudgetRuntimeConfig",
    "V1RuntimeConfig",
    "load_v1_runtime_config",
]