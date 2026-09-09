"""Account fan-out preserves bounded queues, aliases and cancellation ownership."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from orchestrators.v1_event_adapters import TradingContextTaskExecutionAdapter
from services.copy_tasks.account_dispatch import AccountTaskDispatcher
from services.copy_tasks.budget import InMemoryTaskBudgetRepository, TaskBudgetReservationService
from services.copy_tasks.event_routing import CopyTaskEventProcessor
from services.copy_tasks.lifecycle import TaskExecutionDisposition
from services.copy_tasks.reentry_lock import (
    InMemoryMarketReentryLockRepository,
    MarketReentryLockService,
)
from services.copy_tasks.service import InMemoryCopyTaskRepository
from tests.v1.test_copy_task_event_routing import (
    _LEADER,
    ExecutionRecorder,
    coordinator,
    source_event,
    task,
)


class AccountSnapshots:
    """Expose exact scheduling barriers at the injected account I/O boundary."""

    def __init__(self) -> None:
        self.slow_started = asyncio.Event()
        self.slow_release = asyncio.Event()
        self.slow_cancelled = asyncio.Event()
        self.fast_progress: asyncio.Queue[str] = asyncio.Queue()

    async def snapshot_for(self, allocation, event):
        """Wait for one account, letting the other report observed source events."""
        if allocation.follower_account == "account-a":
            self.slow_started.set()
            try:
                await self.slow_release.wait()
            except asyncio.CancelledError:
                self.slow_cancelled.set()
                raise
        else:
            self.fast_progress.put_nowait(event.trade.transaction_hash)
        raise LookupError("Synthetic account snapshot failure")


class Decisions:
    """Capture the existing decision port without replacing routing policy."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str]] = []

    def task_decision(self, allocation, event, result, evaluated_at, max_signal_age_seconds):
        """Record the real processor decision and acknowledge capture."""
        self.records.append((allocation.task_id, result.reason))
        return True


async def dispatcher_fixture(*, resolve_accounts=True):
    repository = InMemoryCopyTaskRepository()
    for task_id, account in (("a-slow", "account-a"), ("b-fast", "account-b")):
        await repository.create(replace(task(task_id=task_id), follower_account=account))
    budgets = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    locks = MarketReentryLockService(InMemoryMarketReentryLockRepository())
    snapshots = AccountSnapshots()
    decisions = Decisions()
    execution = ExecutionRecorder(TaskExecutionDisposition.PRE_POST_REJECTED)
    processor = CopyTaskEventProcessor(
        task_reader=repository,
        task_budgets=budgets,
        coordinator=coordinator(budgets, locks),
        snapshot_provider=snapshots,
        execution=execution,
        decision_observer=decisions,
    )
    dispatcher = AccountTaskDispatcher(
        processor=processor,
        account_key=lambda allocation: allocation.follower_account if resolve_accounts else None,
        queue_size=1,
        max_concurrent_accounts=2,
        max_accounts=2,
        shutdown_timeout_seconds=30,
    )
    await dispatcher.start()
    return dispatcher, snapshots, decisions, execution


def event(transaction: str):
    source = source_event()
    return replace(source, trade=replace(source.trade, transaction_hash=transaction))


@pytest.mark.asyncio
async def test_full_account_queue_does_not_drop_other_account_work() -> None:
    dispatcher, snapshots, decisions, execution = await dispatcher_fixture()
    try:
        await dispatcher(_LEADER, event("first"))
        await snapshots.slow_started.wait()
        assert await snapshots.fast_progress.get() == "first"
        await dispatcher(_LEADER, event("second"))
        assert await snapshots.fast_progress.get() == "second"
        await dispatcher(_LEADER, event("third"))
        assert await snapshots.fast_progress.get() == "third"
        assert ("a-slow", "account_queue_full") in decisions.records
        assert ("b-fast", "account_queue_full") not in decisions.records
        assert dispatcher.queue_depth == 1
        assert execution.commands == []
    finally:
        snapshots.slow_release.set()
        await dispatcher.close()
    assert dispatcher.queue_depth == 0
    assert dispatcher.worker_count == 0


@pytest.mark.asyncio
async def test_cancelled_account_drain_awaits_all_consumers() -> None:
    dispatcher, snapshots, _, _ = await dispatcher_fixture()
    await dispatcher(_LEADER, event("first"))
    await snapshots.slow_started.wait()
    close_started = asyncio.Event()

    async def close():
        close_started.set()
        await dispatcher.close()

    closing = asyncio.create_task(close())
    await close_started.wait()
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert snapshots.slow_cancelled.is_set()
    assert dispatcher.worker_count == 0
    assert dispatcher.queue_depth == 0


@pytest.mark.asyncio
async def test_missing_account_binding_reports_rejection_without_allocating_workers() -> None:
    dispatcher, _, decisions, execution = await dispatcher_fixture(resolve_accounts=False)
    try:
        await dispatcher(_LEADER, event("first"))
        assert decisions.records == [
            ("a-slow", "account_runtime_unavailable"),
            ("b-fast", "account_runtime_unavailable"),
        ]
        assert dispatcher.worker_count == 0
        assert execution.commands == []
    finally:
        await dispatcher.close()


def test_execution_account_aliases_share_one_queue_identity() -> None:
    adapter = TradingContextTaskExecutionAdapter(
        contexts=(SimpleNamespace(user_id="account-a", wallet_address="0xwallet-alias"),)
    )
    assert (
        adapter.account_key(replace(task(task_id="alias-task"), follower_account="account-a"))
        == "account-a"
    )
    assert (
        adapter.account_key(replace(task(task_id="alias-task"), follower_account="0xwallet-alias"))
        == "account-a"
    )
    assert (
        adapter.account_key(replace(task(task_id="alias-task"), follower_account="unknown")) is None
    )
