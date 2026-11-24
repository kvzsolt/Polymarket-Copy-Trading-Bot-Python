"""Task-budget pool waits are measured independently from statement time."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from pmcopy.services.copy_tasks.budget import TaskBudgetKey
from pmcopy.services.observability.metrics import MetricsRegistry, TELEMETRY_METRIC_SCHEMAS
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.storage.postgres.task_budget_repository import PostgresTaskBudgetRepository
from tests.telemetry.test_pipeline_latency import ReceiptClock


class TimedAcquire:
    """Advance a fake ingress clock during acquisition and during a query."""

    def __init__(self, clock: ReceiptClock, failure: BaseException | None) -> None:
        self.clock = clock
        self.failure = failure
        self.releases = 0
        self.connection = AsyncMock()
        self.connection.fetchrow.side_effect = self.query

    def acquire(self) -> "TimedAcquire":
        return self

    async def __aenter__(self):
        self.clock.nanoseconds += 25_000_000
        if self.failure is not None:
            raise self.failure
        return self.connection

    async def __aexit__(self, *args: object) -> None:
        self.releases += 1

    async def query(self, *args: object):
        self.clock.nanoseconds += 100_000_000
        return {
            "task_total": "0",
            "task_daily": "0",
            "task_market": "0",
            "task_has_entry": False,
            "global_market_has_entry": False,
            "task_has_sports_type_entry": False,
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, TimeoutError(), asyncio.CancelledError()])
async def test_acquisition_records_wait_even_on_failure_without_query_or_release_time(failure):
    clock = ReceiptClock()
    registry = MetricsRegistry(schemas=TELEMETRY_METRIC_SCHEMAS)
    pool = TimedAcquire(clock, failure)
    repository = PostgresTaskBudgetRepository(
        pool, latency=PipelineLatency(clock=clock, metrics=registry)
    )
    key = TaskBudgetKey(follower_account="account", task_id="task", market_id="market")
    if failure is None:
        await repository.snapshot(key, datetime(2026, 9, 8, tzinfo=UTC))
    else:
        with pytest.raises(type(failure)):
            await repository.snapshot(key, datetime(2026, 9, 8, tzinfo=UTC))
    metric = registry.histograms['telemetry_latency_seconds{phase="database_acquisition"}']
    assert metric.count == 1
    assert metric.total == 0.025
    assert pool.releases == (1 if failure is None else 0)
