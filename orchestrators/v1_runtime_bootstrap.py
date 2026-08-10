"""Startup wiring for the copy-task v1 runtime.

Bridges the venue-neutral :class:`orchestrators.v1_runtime.CopyTaskV1RuntimeBuilder`
to the deployed PostgreSQL repositories and the branch sizing configuration.
Called once from ``app.main()`` after startup reconciliation/drift checks and
before any market-data subscription, matching the canonical startup order
(config -> DB/migrations -> venue deps -> startup reconciliation ->
task/budget/lock restoration -> subscriptions -> detection -> workers).

Disabled configuration means nothing is constructed: no repositories, no
background tasks, and zero pool access.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

from config.app_config import AppConfig
from config.v1_runtime import V1RuntimeConfig
from orchestrators.v1_runtime import CopyTaskV1RuntimeBuilder, CopyTaskV1RuntimeBundle
from services.copy_sizing_v5.policy import copy_sizing_policy_from_config
from services.copy_tasks.budget import TaskBudgetReservationService
from services.copy_tasks.sizing import CopyTaskSizingService
from storage.postgres.copy_task_repository import PostgresCopyTaskRepository
from storage.postgres.market_reentry_lock_repository import (
    PostgresMarketReentryLockRepository,
)
from storage.postgres.task_budget_repository import PostgresTaskBudgetRepository
from utils.logger import get_logger

logger = get_logger("V1CopyTaskRuntime")

Clock = Callable[[], datetime]


async def _unrouted_leader_event_handler(leader: str, _event: object) -> None:
    """Fail-visible seam for the future detection -> task routing.

    No production event source submits into the v1 worker supervisor yet
    (recorded Phase B remainder: canonical leader-event routing plus a
    downstream execution adapter need their own designed slice). Any event
    that does arrive is logged and dropped rather than silently swallowed.
    """

    logger.warning("v1_leader_event_unrouted | leader=%s", leader)


class CopyTaskV1RuntimeHandle:
    """Own the lifecycle of one enabled v1 copy-task runtime.

    Attributes:
        bundle: The constructed shared v1 services.
        restored_task_count: Active durable tasks found at startup.
        swept_reservation_count: Expired budget reservations released at
            startup before any worker could accept an event.
    """

    def __init__(
        self,
        *,
        bundle: CopyTaskV1RuntimeBundle,
        restored_task_count: int,
        swept_reservation_count: int,
    ) -> None:
        self.bundle = bundle
        self.restored_task_count = int(restored_task_count)
        self.swept_reservation_count = int(swept_reservation_count)
        self._closed = False

    async def start(self) -> None:
        """Mark the worker supervisor ready to accept leader events."""

        if self.bundle.workers is not None:
            await self.bundle.workers.start()

    async def close(self) -> None:
        """Drain and stop the worker supervisor exactly once."""

        if self._closed:
            return
        self._closed = True
        if self.bundle.workers is not None:
            await self.bundle.workers.close()


async def start_v1_copy_task_runtime(
    *,
    pool: object,
    config: AppConfig,
    v1_config: V1RuntimeConfig,
    clock: Clock | None = None,
) -> CopyTaskV1RuntimeHandle | None:
    """Construct, restore, and start the v1 copy-task runtime when enabled.

    Args:
        pool: The shared asyncpg-compatible pool (already migrated).
        config: The immutable application configuration (sizing base policy).
        v1_config: The validated v1 runtime configuration.
        clock: Injectable UTC clock for the startup budget sweep (tests).

    Returns:
        A started runtime handle, or ``None`` when ``copy_tasks.enabled`` is
        False — in which case nothing was constructed and the pool was never
        touched.
    """

    tasks_config = v1_config.copy_tasks
    if not tasks_config.enabled:
        logger.info("v1_copy_task_runtime | status=disabled")
        return None

    now = (clock or (lambda: datetime.now(timezone.utc)))()
    task_repository = PostgresCopyTaskRepository(pool)
    budget_repository = PostgresTaskBudgetRepository(pool)
    lock_repository = PostgresMarketReentryLockRepository(pool)
    sizing = CopyTaskSizingService(
        base_policy=copy_sizing_policy_from_config(config.sizing),
    )
    builder: CopyTaskV1RuntimeBuilder[object] = CopyTaskV1RuntimeBuilder(
        task_repository=task_repository,
        task_budget_repository=budget_repository,
        reentry_lock_repository=lock_repository,
        sizing=sizing,
        leader_handler=_unrouted_leader_event_handler,
    )
    bundle = builder.build(tasks_config)

    # Durable restoration BEFORE any worker accepts events: count active
    # tasks and release expired budget reservations left by a prior run.
    active_tasks = await task_repository.list_active()
    swept = await TaskBudgetReservationService(budget_repository).sweep_expired(now)

    handle = CopyTaskV1RuntimeHandle(
        bundle=bundle,
        restored_task_count=len(active_tasks),
        swept_reservation_count=swept,
    )
    await handle.start()
    logger.info(
        "v1_copy_task_runtime | status=started active_tasks=%d "
        "swept_reservations=%d workers=%s price_cache=%s",
        handle.restored_task_count,
        handle.swept_reservation_count,
        bundle.workers is not None,
        bundle.price_cache is not None,
    )
    return handle


__all__ = ["CopyTaskV1RuntimeHandle", "start_v1_copy_task_runtime"]
