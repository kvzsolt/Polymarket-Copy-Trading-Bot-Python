"""Composition root for the v1 copy-task application slice.

The builder constructs the shared runtime services (task service, coordinator,
lifecycle, price cache, leader workers) from narrow injected repositories and
a sizing port. It performs no venue or network I/O and imports no vendor
libraries.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, TypeVar

from pmcopy.config.v1_runtime import CopyTasksRuntimeConfig
from pmcopy.services.copy_tasks.admission import CopyTaskAdmissionEngine
from pmcopy.services.copy_tasks.budget import (
    TaskBudgetRepository,
    TaskBudgetReservationService,
)
from pmcopy.services.copy_tasks.entry_price import CopyTaskEntryPriceService
from pmcopy.services.copy_tasks.lifecycle import CopyTaskFillLifecycleService
from pmcopy.services.copy_tasks.lots import RiskLotRepository, RiskLotService
from pmcopy.services.copy_tasks.monitoring import CopyMonitoringModePolicy
from pmcopy.services.copy_tasks.price_cache import MarketPriceCache
from pmcopy.services.copy_tasks.reaction import CopyReactionEngine
from pmcopy.services.copy_tasks.reentry_lock import (
    MarketReentryLockRepository,
    MarketReentryLockService,
)
from pmcopy.services.copy_tasks.runtime import CopyTaskRuntimeCoordinator, SizingPort
from pmcopy.services.copy_tasks.service import CopyTaskRepository, CopyTaskService
from pmcopy.services.copy_tasks.worker import LeaderHandler, LeaderWorkerSupervisor

EventT = TypeVar("EventT")


@dataclass(frozen=True, slots=True)
class CopyTaskV1RuntimeBundle(Generic[EventT]):
    """Shared v1 services injected into the existing branch orchestration.

    Attributes:
        enabled: Whether the v1 copy-task slice is active.
        tasks: Copy-task lifecycle application service.
        coordinator: Lock-first task orchestration seam.
        lifecycle: Entry-fill budget commit and protective-exit lock coordinator.
        risk_lots: Restart-safe per-fill protective-risk lot service.
        price_cache: Shared bounded market-price cache (None when disabled).
        workers: Bounded serial-per-leader worker supervisor (None when disabled).
    """

    enabled: bool
    tasks: CopyTaskService
    coordinator: CopyTaskRuntimeCoordinator
    lifecycle: CopyTaskFillLifecycleService
    risk_lots: RiskLotService
    price_cache: MarketPriceCache | None
    workers: LeaderWorkerSupervisor[EventT] | None


class CopyTaskV1RuntimeBuilder(Generic[EventT]):
    """Build one shared copy-task runtime without venue-specific imports.

    The builder is generic over the leader-worker event payload so the
    composition root can wire the canonical LeaderTradeEvent or any other
    event type without dragging venue types into this module.
    """

    def __init__(
        self,
        *,
        task_repository: CopyTaskRepository,
        task_budget_repository: TaskBudgetRepository,
        reentry_lock_repository: MarketReentryLockRepository,
        risk_lot_repository: RiskLotRepository,
        sizing: SizingPort,
        leader_handler: LeaderHandler[EventT],
    ) -> None:
        self._task_repository = task_repository
        self._task_budget_repository = task_budget_repository
        self._reentry_lock_repository = reentry_lock_repository
        self._risk_lot_repository = risk_lot_repository
        self._sizing = sizing
        self._leader_handler = leader_handler

    def build(
        self,
        config: CopyTasksRuntimeConfig,
    ) -> CopyTaskV1RuntimeBundle[EventT]:
        """Build bounded workers, shared cache, and durable task services.

        Args:
            config: Immutable v1 runtime configuration.

        Returns:
            Runtime bundle. When ``config.enabled`` is False, workers and the
            price cache are not constructed so disabled means no background
            tasks and no network I/O.
        """

        tasks = CopyTaskService(self._task_repository)
        locks = MarketReentryLockService(self._reentry_lock_repository)
        budgets = TaskBudgetReservationService(self._task_budget_repository)
        risk_lots = RiskLotService(self._risk_lot_repository)
        active_budgets = budgets if config.enabled and config.task_budget.enabled else None
        lifecycle = CopyTaskFillLifecycleService(
            task_budgets=budgets,
            reentry_locks=locks,
            risk_lots=risk_lots,
            enabled=config.enabled,
        )
        coordinator = CopyTaskRuntimeCoordinator(
            locks=locks,
            monitoring=CopyMonitoringModePolicy(),
            admission=CopyTaskAdmissionEngine(),
            reactions=CopyReactionEngine(),
            entry_prices=CopyTaskEntryPriceService(),
            sizing=self._sizing,
            task_budgets=active_budgets,
            task_budget_lifecycle=lifecycle if active_budgets is not None else None,
            task_budget_ttl_seconds=config.task_budget.reservation_ttl_seconds,
            enabled=config.enabled,
            reentry_lock_enabled=config.reentry_lock.enabled,
        )
        price_cache = (
            MarketPriceCache(max_entries=config.shared_price_cache.max_entries)
            if config.enabled and config.shared_price_cache.enabled
            else None
        )
        workers = (
            LeaderWorkerSupervisor(
                handler=self._leader_handler,
                queue_size=config.workers.queue_size,
                max_concurrent_leaders=config.workers.global_concurrency,
                max_leaders=config.workers.max_leader_workers,
                shutdown_timeout_seconds=config.workers.shutdown_timeout_seconds,
            )
            if config.enabled and config.workers.enabled
            else None
        )
        return CopyTaskV1RuntimeBundle(
            enabled=config.enabled,
            tasks=tasks,
            coordinator=coordinator,
            lifecycle=lifecycle,
            risk_lots=risk_lots,
            price_cache=price_cache,
            workers=workers,
        )


__all__ = ["CopyTaskV1RuntimeBuilder", "CopyTaskV1RuntimeBundle"]
