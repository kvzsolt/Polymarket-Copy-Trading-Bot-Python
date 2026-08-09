"""Strict additive runtime configuration for copy-task v1.

These pydantic models are intentionally separate from the branch's frozen
dataclass AppConfig so v1 settings can be layered in without forcing unknown
keys through the existing config schema. The composition root merges them at
startup.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


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
    """Top-level copy-task v1 engine settings."""

    enabled: bool = True
    workers: LeaderWorkerRuntimeConfig = LeaderWorkerRuntimeConfig()
    shared_price_cache: SharedPriceCacheRuntimeConfig = SharedPriceCacheRuntimeConfig()
    task_budget: TaskBudgetRuntimeConfig = TaskBudgetRuntimeConfig()
    reentry_lock: ReentryLockRuntimeConfig = ReentryLockRuntimeConfig()
    protective_exits: ProtectiveExitRuntimeConfig = ProtectiveExitRuntimeConfig()


class V1RuntimeConfig(_FrozenModel):
    """Additive v1 settings merged into the repository runtime configuration."""

    copy_tasks: CopyTasksRuntimeConfig = CopyTasksRuntimeConfig()


__all__ = [
    "CopyTasksRuntimeConfig",
    "LeaderWorkerRuntimeConfig",
    "ProtectiveExitRuntimeConfig",
    "ReentryLockRuntimeConfig",
    "SharedPriceCacheRuntimeConfig",
    "TaskBudgetRuntimeConfig",
    "V1RuntimeConfig",
]