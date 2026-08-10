"""Composition contracts for the copy-task v1 runtime bundle."""

from __future__ import annotations

import pytest

from config.v1_runtime import CopyTasksRuntimeConfig
from orchestrators.v1_runtime import CopyTaskV1RuntimeBuilder
from services.copy_tasks.budget import InMemoryTaskBudgetRepository
from services.copy_tasks.reentry_lock import InMemoryMarketReentryLockRepository
from services.copy_tasks.service import InMemoryCopyTaskRepository


class Sizing:
    """Minimal sizing dependency."""

    def size(self, _task: object, _request: object) -> object:
        return object()


async def handler(_leader: str, _event: object) -> None:
    """No-op worker handler."""


@pytest.mark.asyncio
async def test_builder_creates_one_shared_cache_and_bounded_worker_supervisor() -> None:
    builder = CopyTaskV1RuntimeBuilder(
        task_repository=InMemoryCopyTaskRepository(),
        task_budget_repository=InMemoryTaskBudgetRepository(),
        reentry_lock_repository=InMemoryMarketReentryLockRepository(),
        sizing=Sizing(),
        leader_handler=handler,
    )
    config = CopyTasksRuntimeConfig.model_validate(
        {
            "enabled": True,
            "workers": {
                "queue_size": 3,
                "global_concurrency": 2,
                "max_leader_workers": 4,
            },
            "shared_price_cache": {"max_entries": 7},
        }
    )

    bundle = builder.build(config)

    assert bundle.enabled is True
    assert bundle.workers is not None
    assert bundle.price_cache is not None
    assert bundle.coordinator is not None
    assert bundle.lifecycle is not None
    await bundle.workers.start()
    await bundle.workers.submit("0xleader", object())
    await bundle.workers.close()


@pytest.mark.asyncio
async def test_disabled_builder_creates_no_worker_or_price_cache() -> None:
    builder = CopyTaskV1RuntimeBuilder(
        task_repository=InMemoryCopyTaskRepository(),
        task_budget_repository=InMemoryTaskBudgetRepository(),
        reentry_lock_repository=InMemoryMarketReentryLockRepository(),
        sizing=Sizing(),
        leader_handler=handler,
    )

    bundle = builder.build(CopyTasksRuntimeConfig(enabled=False))

    assert bundle.enabled is False
    assert bundle.workers is None
    assert bundle.price_cache is None
    assert bundle.coordinator is not None
    decision = await bundle.coordinator.process(
        task=None,  # type: ignore[arg-type]
        source_trade=None,  # type: ignore[arg-type]
        admission_context=None,  # type: ignore[arg-type]
        sizing_request=None,
        downstream=lambda _envelope: None,
    )
    assert decision.reason == "copy_tasks_disabled"


@pytest.mark.asyncio
async def test_disabled_workers_yields_no_supervisor() -> None:
    builder = CopyTaskV1RuntimeBuilder(
        task_repository=InMemoryCopyTaskRepository(),
        task_budget_repository=InMemoryTaskBudgetRepository(),
        reentry_lock_repository=InMemoryMarketReentryLockRepository(),
        sizing=Sizing(),
        leader_handler=handler,
    )
    config = CopyTasksRuntimeConfig.model_validate(
        {
            "enabled": True,
            "workers": {"enabled": False},
            "shared_price_cache": {"enabled": False},
        }
    )
    bundle = builder.build(config)
    assert bundle.enabled is True
    assert bundle.workers is None
    assert bundle.price_cache is None
    assert bundle.coordinator is not None