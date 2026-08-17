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

from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

import asyncpg

from config.app_config import AppConfig
from config.v1_runtime import CopyTasksRuntimeConfig
from orchestrators.v1_event_adapters import (
    CopyTaskRuntimeIngress,
    DeferredCopyTaskEventHandler,
    FollowerBalanceReader,
    LeaderEventNormalizer,
    TradingContextTaskExecutionAdapter,
    TradingContextTaskSnapshotProvider,
)
from orchestrators.v1_runtime import CopyTaskV1RuntimeBuilder, CopyTaskV1RuntimeBundle
from services.copy_sizing_v5.policy import copy_sizing_policy_from_config
from services.copy_tasks.budget import TaskBudgetReservationService
from services.copy_tasks.domain import CopyTask
from services.copy_tasks.event_routing import CopyTaskEventProcessor
from services.copy_tasks.price_cache import MarketPriceCacheSink
from services.copy_tasks.reconciliation import TaskBudgetReconciliationService
from services.copy_tasks.sizing import CopyTaskSizingService
from storage.postgres.copy_task_repository import (
    DatabasePool as CopyTaskDatabasePool,
    PostgresCopyTaskRepository,
)
from storage.postgres.market_reentry_lock_repository import (
    DatabasePool as ReentryLockDatabasePool,
    PostgresMarketReentryLockRepository,
)
from storage.postgres.risk_lot_repository import (
    DatabasePool as RiskLotDatabasePool,
    PostgresRiskLotRepository,
)
from storage.postgres.task_budget_repository import (
    DatabasePool as TaskBudgetDatabasePool,
    PostgresTaskBudgetRepository,
)
from utils.logger import get_logger

logger = get_logger("V1CopyTaskRuntime")

Clock = Callable[[], datetime]
BalanceFunction = Callable[[str], Awaitable[Decimal | None]]


class _CallableClock:
    """Adapt the bootstrap's callable clock to the normalizer clock port."""

    def __init__(self, function: Clock) -> None:
        self._function = function

    def now(self) -> datetime:
        """Return one timezone-aware instant from the injected callable."""

        return self._function()


class RuntimeFollowerBalanceReader:
    """Adapt the existing collateral reader to the task snapshot port."""

    def __init__(self, reader: BalanceFunction) -> None:
        self._reader = reader

    async def read(self, wallet: str) -> Decimal | None:
        """Return available follower collateral without coercing failures."""

        return await self._reader(wallet)


class CopyTaskV1RuntimeHandle:
    """Own the lifecycle of one enabled v1 copy-task runtime.

    Attributes:
        bundle: The constructed shared v1 services.
        restored_task_count: Active durable tasks found at startup.
        swept_reservation_count: Expired budget reservations released at
            startup before any worker could accept an event.
        restored_lot_count: Open protective-risk lots rehydrated before event
            processing starts.
    """

    def __init__(
        self,
        *,
        bundle: CopyTaskV1RuntimeBundle,
        restored_task_count: int,
        swept_reservation_count: int,
        restored_lot_count: int,
        ingress: CopyTaskRuntimeIngress | None,
        snapshot_provider: TradingContextTaskSnapshotProvider,
        execution_adapter: TradingContextTaskExecutionAdapter,
        quote_sink: MarketPriceCacheSink | None,
    ) -> None:
        self.bundle = bundle
        self.restored_task_count = int(restored_task_count)
        self.swept_reservation_count = int(swept_reservation_count)
        self.restored_lot_count = int(restored_lot_count)
        self._ingress = ingress
        self._snapshot_provider = snapshot_provider
        self._execution_adapter = execution_adapter
        self.quote_sink = quote_sink
        self._started = False
        self._closed = False

    @property
    def worker_count(self) -> int:
        """Return the current low-cardinality leader-worker count."""

        workers = self.bundle.workers
        return 0 if workers is None else workers.worker_count

    @property
    def queue_depth(self) -> int:
        """Return the aggregate queued copy-event count."""

        workers = self.bundle.workers
        return 0 if workers is None else workers.queue_depth

    async def start(self) -> None:
        """Mark the worker supervisor ready to accept leader events."""

        if self._started:
            return
        self._started = True
        if self.bundle.workers is not None:
            await self.bundle.workers.start()

    async def close(self) -> None:
        """Drain and stop the worker supervisor exactly once."""

        if self._closed:
            return
        self._closed = True
        if self.bundle.workers is not None:
            await self.bundle.workers.close()

    async def submit_raw_batch(
        self,
        leader: str,
        rows: Iterable[Mapping[str, object]],
    ) -> int:
        """Route one polling batch into the canonical copy-task workers."""

        if self._ingress is None:
            raise RuntimeError("copy-task workers are disabled")
        return await self._ingress.submit_raw_batch(leader, rows)

    async def submit_onchain(self, signal: object) -> None:
        """Route one on-chain signal into the canonical copy-task workers."""

        if self._ingress is None:
            raise RuntimeError("copy-task workers are disabled")
        await self._ingress.submit_onchain(signal)  # type: ignore[arg-type]

    def add_contexts(self, contexts: Iterable[object]) -> None:
        """Register contexts created by dynamic leader discovery."""

        values = tuple(contexts)
        self._snapshot_provider.add_contexts(values)
        self._execution_adapter.add_contexts(values)

    def has_task_binding(self, task: CopyTask) -> bool:
        """Return whether a task has both evidence and execution bindings."""

        return self._snapshot_provider.has_task_binding(
            task
        ) and self._execution_adapter.has_task_binding(task)


async def start_v1_copy_task_runtime(
    *,
    pool: asyncpg.Pool,
    config: AppConfig,
    copy_tasks_config: CopyTasksRuntimeConfig,
    contexts: Iterable[object] = (),
    balance_reader: FollowerBalanceReader | None = None,
    clock: Clock | None = None,
    start_workers: bool = True,
) -> CopyTaskV1RuntimeHandle | None:
    """Construct, restore, and start the v1 copy-task runtime when enabled.

    Args:
        pool: The shared asyncpg-compatible pool (already migrated).
        config: The immutable application configuration (sizing base policy).
        copy_tasks_config: The validated copy-task engine configuration
            (``config.copy_trading.copy_tasks``).
        clock: Injectable UTC clock for the startup budget sweep (tests).

    Returns:
        A started runtime handle, or ``None`` when ``copy_tasks.enabled`` is
        False — in which case nothing was constructed and the pool was never
        touched.
    """

    tasks_config = copy_tasks_config
    if not tasks_config.enabled:
        logger.info("v1_copy_task_runtime | status=disabled")
        return None

    active_clock = clock or (lambda: datetime.now(timezone.utc))
    now = active_clock()
    task_repository = PostgresCopyTaskRepository(cast(CopyTaskDatabasePool, pool))
    budget_repository = PostgresTaskBudgetRepository(cast(TaskBudgetDatabasePool, pool))
    lock_repository = PostgresMarketReentryLockRepository(cast(ReentryLockDatabasePool, pool))
    risk_lot_repository = PostgresRiskLotRepository(cast(RiskLotDatabasePool, pool))
    sizing = CopyTaskSizingService(
        base_policy=copy_sizing_policy_from_config(config.sizing),
    )
    deferred_handler = DeferredCopyTaskEventHandler()
    builder = CopyTaskV1RuntimeBuilder(
        task_repository=task_repository,
        task_budget_repository=budget_repository,
        reentry_lock_repository=lock_repository,
        risk_lot_repository=risk_lot_repository,
        sizing=sizing,
        leader_handler=deferred_handler,
    )
    bundle = builder.build(tasks_config)
    context_values = tuple(contexts)
    from storage.postgres.orders_repo import OrdersRepo

    fallback_order_reader = OrdersRepo(pool)
    for context in context_values:
        extra = getattr(context, "extra", None)
        if not isinstance(extra, dict):
            continue
        settlement = TaskBudgetReconciliationService(
            lifecycle=bundle.lifecycle,
            order_reader=getattr(context, "orders_repo", fallback_order_reader),
            clock=_CallableClock(active_clock),
        )
        extra["task_budget_settlement"] = settlement
        existing_reconciler = extra.get("_reconciliation_service")
        if existing_reconciler is not None:
            existing_reconciler.task_budget_settlement = settlement
    if balance_reader is None:
        from utils.getMyBalance import get_my_balance

        balance_reader = RuntimeFollowerBalanceReader(get_my_balance)
    snapshot_provider = TradingContextTaskSnapshotProvider(
        contexts=context_values,
        balance_reader=balance_reader,
    )
    execution_adapter = TradingContextTaskExecutionAdapter(contexts=context_values)
    processor = CopyTaskEventProcessor(
        task_reader=task_repository,
        task_budgets=TaskBudgetReservationService(budget_repository),
        coordinator=bundle.coordinator,
        snapshot_provider=snapshot_provider,
        execution=execution_adapter,
    )
    deferred_handler.bind(processor)
    ingress = (
        CopyTaskRuntimeIngress(
            workers=bundle.workers,
            normalizer=LeaderEventNormalizer(clock=_CallableClock(active_clock)),
        )
        if bundle.workers is not None
        else None
    )

    # Durable restoration BEFORE any worker accepts events: count active
    # tasks and release expired budget reservations left by a prior run.
    active_tasks = await task_repository.list_active()
    swept = await TaskBudgetReservationService(budget_repository).sweep_expired(now)
    open_lots = await risk_lot_repository.list_all_open()

    handle = CopyTaskV1RuntimeHandle(
        bundle=bundle,
        restored_task_count=len(active_tasks),
        swept_reservation_count=swept,
        restored_lot_count=len(open_lots),
        ingress=ingress,
        snapshot_provider=snapshot_provider,
        execution_adapter=execution_adapter,
        quote_sink=(
            MarketPriceCacheSink(
                cache=bundle.price_cache,
                clock=_CallableClock(active_clock),
            )
            if bundle.price_cache is not None
            else None
        ),
    )
    if start_workers:
        await handle.start()
    logger.info(
        "v1_copy_task_runtime | status=started active_tasks=%d "
        "swept_reservations=%d restored_lots=%d workers=%s price_cache=%s",
        handle.restored_task_count,
        handle.swept_reservation_count,
        handle.restored_lot_count,
        bundle.workers is not None,
        bundle.price_cache is not None,
    )
    return handle


__all__ = [
    "CopyTaskV1RuntimeHandle",
    "RuntimeFollowerBalanceReader",
    "start_v1_copy_task_runtime",
]
