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

import asyncio

from collections.abc import Awaitable, Callable, Iterable, Mapping
from datetime import datetime, timezone
from decimal import Decimal
from typing import cast

import asyncpg
from pmcopy.orchestrators.task_protection import TaskProtectiveRuntimeBindings

from pmcopy.config.app_config import AppConfig
from pmcopy.config.v1_runtime import CopyTasksRuntimeConfig
from pmcopy.orchestrators.v1_event_adapters import (
    CopyTaskRuntimeIngress,
    DeferredCopyTaskEventHandler,
    FollowerBalanceReader,
    LeaderEventNormalizer,
    TradingContextTaskExecutionAdapter,
    TradingContextTaskSnapshotProvider,
)
from pmcopy.orchestrators.v1_runtime import CopyTaskV1RuntimeBuilder, CopyTaskV1RuntimeBundle
from pmcopy.services.copy_sizing_v5.policy import copy_sizing_policy_from_config
from pmcopy.services.copy_tasks.budget import TaskBudgetReservationService
from pmcopy.services.copy_tasks.domain import CopyTask
from pmcopy.services.copy_tasks.account_dispatch import AccountTaskDispatcher
from pmcopy.services.copy_tasks.event_routing import CopyTaskEventProcessor, TaskDecisionObserver
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.services.copy_tasks.price_cache import MarketPriceCacheSink
from pmcopy.services.copy_tasks.reconciliation import TaskBudgetReconciliationService
from pmcopy.services.copy_tasks.service import CopyTaskConflict
from pmcopy.services.copy_tasks.sizing import CopyTaskSizingService
from pmcopy.services.onchain_detection.leader_sell_scope_guard import LeaderSellBuyScopeGuard
from pmcopy.storage.postgres.copy_task_repository import (
    CopyTaskCodec,
    DatabasePool as CopyTaskDatabasePool,
    PostgresCopyTaskRepository,
)
from pmcopy.storage.postgres.market_reentry_lock_repository import (
    DatabasePool as ReentryLockDatabasePool,
    PostgresMarketReentryLockRepository,
)
from pmcopy.storage.postgres.risk_lot_repository import (
    DatabasePool as RiskLotDatabasePool,
    PostgresRiskLotRepository,
)
from pmcopy.storage.postgres.task_budget_repository import (
    DatabasePool as TaskBudgetDatabasePool,
    PostgresTaskBudgetRepository,
)
from pmcopy.utils.logger import get_logger

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


def _json_safe(value: object) -> object:
    """Convert a trader-file task model into codec-compatible JSON values."""

    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _trader_task_rows(
    trader: object,
    *,
    contexts: Iterable[object],
    default_follower_account: str,
) -> list[dict[str, object]]:
    """Build one durable task row per follower binding for a trader file.

    File-defined tasks use the trader's fantasy name as the task id when the
    CLI has a single follower account. With multiple accounts the account is
    appended so every binding gets its own durable task row.
    """

    task = getattr(trader, "task", None)
    if task is None:
        return []
    wallet = str(getattr(trader, "wallet", "") or "").strip().lower()
    accounts: list[str] = []
    for context in contexts:
        extra = getattr(context, "extra", None)
        if isinstance(extra, Mapping):
            leader = str(extra.get("leader_wallet") or "").strip().lower()
        else:
            leader = ""
        if leader == wallet:
            account = str(
                getattr(context, "user_id", "") or getattr(context, "wallet_address", "") or ""
            ).strip()
            if account and account.casefold() not in {item.casefold() for item in accounts}:
                accounts.append(account)
    if not accounts:
        default = str(default_follower_account or "").strip()
        if default:
            accounts.append(default)
    if not accounts:
        accounts.append("default")

    task_dump = task.model_dump(mode="python")
    configuration = _json_safe(
        {
            key: task_dump[key]
            for key in (
                "trigger",
                "spend",
                "price",
                "reaction",
                "market_filter",
                "schedule",
                "sizing",
            )
        }
    )
    rows: list[dict[str, object]] = []
    for account in accounts:
        task_id = str(getattr(trader, "name", "") or "").strip()
        if len(accounts) > 1:
            task_id = f"{task_id}-{account.casefold()}"
        rows.append(
            {
                "task_id": task_id,
                "name": str(getattr(trader, "name", "") or "").strip() or task_id,
                "leader_wallet": str(getattr(trader, "wallet", "") or "").strip(),
                "follower_account": account,
                "status": "active" if bool(getattr(trader, "enabled", True)) else "paused",
                "monitoring_mode": task.monitoring_mode,
                "sizing_mode": task.sizing_mode,
                "notifications_muted": task.notifications_muted,
                "version": 1,
                "configuration": configuration,
            }
        )
    return rows


async def _seed_tasks_from_trader_configs(
    *,
    config: AppConfig,
    task_repository: PostgresCopyTaskRepository,
    contexts: Iterable[object],
) -> int:
    """Seed file-defined copy tasks from ``config/traders/*.yml``.

    Existing task ids are never overwritten: file-defined tasks are a startup
    seed, and an existing durable task keeps its current configuration.
    Returns the number of tasks created.
    """

    codec = CopyTaskCodec()
    created = 0
    default_account = config.wallet.funder_address or "default"
    for trader in config.copy_trading.traders:
        rows = _trader_task_rows(
            trader,
            contexts=contexts,
            default_follower_account=default_account,
        )
        for row in rows:
            task = codec.decode_row(row)
            task_id = str(task.task_id)
            if await task_repository.get(task_id) is not None:
                logger.info(
                    "v1_copy_task_seed | task_id=%s status=already_exists",
                    task_id,
                )
                continue
            try:
                await task_repository.create(task)
            except CopyTaskConflict:
                logger.info(
                    "v1_copy_task_seed | task_id=%s status=conflict_skipped",
                    task_id,
                )
                continue
            created += 1
            logger.info(
                "v1_copy_task_seed | task_id=%s leader=%s follower=%s",
                task_id,
                task.leader_wallet,
                task.follower_account,
            )
    return created


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
        protective_bindings: TaskProtectiveRuntimeBindings | None = None,
        account_dispatcher: AccountTaskDispatcher | None = None,
        shutdown_timeout_seconds: float = 30.0,
    ) -> None:
        self.bundle = bundle
        self.restored_task_count = int(restored_task_count)
        self.swept_reservation_count = int(swept_reservation_count)
        self.restored_lot_count = int(restored_lot_count)
        self._ingress = ingress
        self._snapshot_provider = snapshot_provider
        self._execution_adapter = execution_adapter
        self.quote_sink = quote_sink
        self._protective_bindings = protective_bindings
        self._account_dispatcher = account_dispatcher
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._started = False
        self._closed = False

    @property
    def worker_count(self) -> int:
        """Return the allocated leader and account consumer count."""

        workers = self.bundle.workers
        leader_count = 0 if workers is None else workers.worker_count
        account_count = (
            0 if self._account_dispatcher is None else self._account_dispatcher.worker_count
        )
        return leader_count + account_count

    @property
    def queue_depth(self) -> int:
        """Return the aggregate queued copy-event count."""

        workers = self.bundle.workers
        leader_depth = 0 if workers is None else workers.queue_depth
        account_depth = (
            0 if self._account_dispatcher is None else self._account_dispatcher.queue_depth
        )
        return leader_depth + account_depth

    async def start(self) -> None:
        """Mark the worker supervisor ready to accept leader events."""

        if self._started:
            return
        self._started = True
        if self._account_dispatcher is not None:
            await self._account_dispatcher.start()
        if self.bundle.workers is not None:
            await self.bundle.workers.start()

    async def close(self) -> None:
        """Drain and stop the worker supervisor exactly once."""

        if self._closed:
            return
        self._closed = True
        try:
            async with asyncio.timeout(self._shutdown_timeout_seconds):
                if self.bundle.workers is not None:
                    await self.bundle.workers.close()
                if self._account_dispatcher is not None:
                    await self._account_dispatcher.close()
        except TimeoutError:
            logger.warning("copy_task_runtime_drain_timeout")
        finally:
            if self.bundle.workers is not None:
                await self.bundle.workers.abort()
            if self._account_dispatcher is not None:
                await self._account_dispatcher.abort()

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
        if self._protective_bindings is not None:
            self._protective_bindings.bind(values)

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
    buy_scope_guard: LeaderSellBuyScopeGuard | None = None,
    decision_observer: TaskDecisionObserver | None = None,
    latency: PipelineLatency | None = None,
) -> CopyTaskV1RuntimeHandle | None:
    """Construct, restore, and start the v1 copy-task runtime when enabled.

    Args:
        pool: The shared asyncpg-compatible pool (already migrated).
        config: The immutable application configuration (sizing base policy).
        copy_tasks_config: The validated copy-task engine configuration
            (``config.copy_trading.copy_tasks``).
        clock: Injectable UTC clock for the startup budget sweep (tests).
        start_workers: Whether bounded workers accept events immediately.
        buy_scope_guard: Shared durable emergency SELL scope guard.
        decision_observer: Existing non-blocking telemetry owner for task decisions.

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
    budget_repository = PostgresTaskBudgetRepository(
        cast(TaskBudgetDatabasePool, pool), latency=latency
    )
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
    await _seed_tasks_from_trader_configs(
        config=config,
        task_repository=task_repository,
        contexts=context_values,
    )
    from pmcopy.storage.postgres.orders_repo import OrdersRepo

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
        from pmcopy.utils.getMyBalance import get_my_balance

        balance_reader = RuntimeFollowerBalanceReader(get_my_balance)
    snapshot_provider = TradingContextTaskSnapshotProvider(
        contexts=context_values,
        balance_reader=balance_reader,
        risk_lots=bundle.risk_lots,
    )
    protective_bindings = TaskProtectiveRuntimeBindings(
        bundle.risk_lots, slippage_bps=int(config.execution.slippage_tolerance * 100)
    )
    protective_bindings.bind(context_values)
    execution_adapter = TradingContextTaskExecutionAdapter(contexts=context_values)
    processor = CopyTaskEventProcessor(
        task_reader=task_repository,
        task_budgets=TaskBudgetReservationService(budget_repository),
        coordinator=bundle.coordinator,
        snapshot_provider=snapshot_provider,
        execution=execution_adapter,
        clock=_CallableClock(active_clock),
        max_signal_age_seconds=config.copy_trading.copy_max_signal_age_seconds,
        decision_observer=decision_observer,
        latency=latency,
    )
    account_dispatcher = (
        AccountTaskDispatcher(
            processor=processor,
            account_key=execution_adapter.account_key,
            queue_size=tasks_config.workers.queue_size,
            max_concurrent_accounts=tasks_config.workers.global_concurrency,
            max_accounts=tasks_config.workers.max_leader_workers,
            shutdown_timeout_seconds=tasks_config.workers.shutdown_timeout_seconds,
        )
        if bundle.workers is not None
        else None
    )
    deferred_handler.bind(account_dispatcher if account_dispatcher is not None else processor)
    ingress = (
        CopyTaskRuntimeIngress(
            workers=bundle.workers,
            normalizer=LeaderEventNormalizer(clock=_CallableClock(active_clock), latency=latency),
            buy_scope_guard=buy_scope_guard,
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
        protective_bindings=protective_bindings,
        account_dispatcher=account_dispatcher,
        shutdown_timeout_seconds=tasks_config.workers.shutdown_timeout_seconds,
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
