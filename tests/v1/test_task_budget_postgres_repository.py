"""Contracts for durable task-budget reservations."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from services.copy_tasks.budget import (
    TaskBudgetInvariantError,
    TaskBudgetKey,
    TaskBudgetReservationRequest,
)
from services.copy_tasks.domain import TaskSpendPolicy
from storage.postgres.task_budget_repository import PostgresTaskBudgetRepository


class FakeTransaction:
    """Record transaction boundaries for one fake connection."""

    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> None:
        self._connection.transaction_entries += 1

    async def __aexit__(self, *_args: object) -> None:
        self._connection.transaction_exits += 1


class FakeConnection:
    """Minimal asyncpg-like connection with scripted results."""

    def __init__(self) -> None:
        self.fetchrow_results: list[Mapping[str, object] | None] = []
        self.fetchval_results: list[object] = []
        self.execute_results: list[str] = []
        self.calls: list[tuple[str, str, tuple[object, ...]]] = []
        self.transaction_entries = 0
        self.transaction_exits = 0

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    async def fetchrow(
        self,
        sql: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        self.calls.append(("fetchrow", sql, args))
        return self.fetchrow_results.pop(0)

    async def fetchval(self, sql: str, *args: object) -> object:
        self.calls.append(("fetchval", sql, args))
        return self.fetchval_results.pop(0)

    async def execute(self, sql: str, *args: object) -> str:
        self.calls.append(("execute", sql, args))
        if self.execute_results:
            return self.execute_results.pop(0)
        return "OK"


class FakeAcquire:
    """Async context manager returning the scripted connection."""

    def __init__(self, connection: FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self._connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakePool:
    """Pool exposing one deterministic connection."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def acquire(self) -> FakeAcquire:
        return FakeAcquire(self.connection)


def request() -> TaskBudgetReservationRequest:
    """Return one deterministic reservation request."""

    now = datetime(2026, 8, 8, 12, tzinfo=UTC)
    return TaskBudgetReservationRequest(
        reservation_id="intent-a",
        key=TaskBudgetKey(
            follower_account="account-a",
            task_id="task-a",
            market_id="market-a",
        ),
        amount=Decimal("7.50"),
        reserved_at=now,
        expires_at=now + timedelta(minutes=2),
    )


def snapshot_row(**overrides: object) -> dict[str, object]:
    """Return one task-budget snapshot database row."""

    value: dict[str, object] = {
        "task_total": Decimal("0"),
        "task_daily": Decimal("0"),
        "task_market": Decimal("0"),
        "task_has_entry": False,
        "global_market_has_entry": False,
        "task_has_sports_type_entry": False,
    }
    value.update(overrides)
    return value


def reservation_row(*, status: str = "reserved") -> dict[str, object]:
    """Return one persisted reservation row."""

    item = request()
    return {
        "reservation_id": item.reservation_id,
        "follower_account": item.key.follower_account,
        "task_id": item.key.task_id,
        "market_id": item.key.market_id,
        "sports_market_type": item.key.sports_market_type,
        "amount": item.amount,
        "reserved_at": item.reserved_at,
        "expires_at": item.expires_at,
        "status": status,
        "fill_id": None,
        "committed_notional": None,
    }


@pytest.mark.asyncio
async def test_reserve_holds_transaction_scoped_identity_and_budget_locks() -> None:
    connection = FakeConnection()
    connection.fetchrow_results = [None, snapshot_row(), reservation_row()]
    repository = PostgresTaskBudgetRepository(FakePool(connection))
    policy = TaskSpendPolicy(
        max_total=Decimal("8"),
        max_daily=Decimal("8"),
        max_per_market=Decimal("8"),
        one_entry_per_market=True,
    )

    result = await repository.reserve(request(), policy)

    assert result.allowed is True
    assert result.created is True
    assert result.reservation is not None
    assert result.reservation.request.amount == Decimal("7.50")
    assert connection.transaction_entries == connection.transaction_exits == 1
    advisory_calls = [item for item in connection.calls if "pg_advisory_xact_lock" in item[1]]
    assert len(advisory_calls) == 3
    assert any(
        "INSERT INTO copy_task_budget_reservations" in item[1] for item in connection.calls
    )
    insert = next(
        item for item in connection.calls if "INSERT INTO copy_task_budget_reservations" in item[1]
    )
    assert insert[2][4] == Decimal("7.50")


@pytest.mark.asyncio
async def test_reserve_rejects_from_atomic_snapshot_without_insert() -> None:
    connection = FakeConnection()
    connection.fetchrow_results = [None, snapshot_row(task_total=Decimal("5"))]
    repository = PostgresTaskBudgetRepository(FakePool(connection))

    result = await repository.reserve(
        request(),
        TaskSpendPolicy(max_total=Decimal("10")),
    )

    assert result.allowed is False
    assert result.reason == "task_max_total_spend_exceeded"
    assert result.reservation is None
    assert not any(
        "INSERT INTO copy_task_budget_reservations" in item[1] for item in connection.calls
    )


@pytest.mark.asyncio
async def test_existing_reserved_request_is_idempotent_but_committed_is_closed() -> None:
    item = request()
    connection = FakeConnection()
    connection.fetchrow_results = [
        reservation_row(),
        snapshot_row(task_total=item.amount, task_has_entry=True),
        reservation_row(status="committed"),
        snapshot_row(task_total=item.amount, task_has_entry=True),
    ]
    repository = PostgresTaskBudgetRepository(FakePool(connection))
    policy = TaskSpendPolicy(max_total=Decimal("100"))

    reserved = await repository.reserve(item, policy)
    committed = await repository.reserve(item, policy)

    assert reserved.allowed is True
    assert reserved.created is False
    assert reserved.reason == "task_budget_already_reserved"
    assert committed.allowed is False
    assert committed.reason == "task_budget_already_committed"


@pytest.mark.asyncio
async def test_commit_maps_idempotency_and_invariant_outcomes() -> None:
    item = request()
    connection = FakeConnection()
    connection.fetchrow_results = [
        None,
        reservation_row(),
        {"fill_id": "fill-a", "reservation_id": "intent-a", "notional": Decimal("7")},
        reservation_row(status="committed")
        | {"fill_id": "fill-a", "committed_notional": Decimal("7")},
        None,
        reservation_row(),
    ]
    repository = PostgresTaskBudgetRepository(FakePool(connection))

    assert (
        await repository.commit(
            reservation_id="intent-a",
            fill_id="fill-a",
            filled_notional=Decimal("7"),
            occurred_at=item.reserved_at,
        )
        is True
    )
    assert (
        await repository.commit(
            reservation_id="intent-a",
            fill_id="fill-a",
            filled_notional=Decimal("7"),
            occurred_at=item.reserved_at,
        )
        is False
    )
    with pytest.raises(TaskBudgetInvariantError, match="exceeds"):
        await repository.commit(
            reservation_id="intent-a",
            fill_id="fill-b",
            filled_notional=Decimal("9"),
            occurred_at=item.reserved_at,
        )

    assert any("copy_task_fill_ledger" in item[1] for item in connection.calls)


@pytest.mark.asyncio
async def test_release_and_sweep_are_state_guarded() -> None:
    connection = FakeConnection()
    connection.fetchval_results = [1, 0, 3]
    repository = PostgresTaskBudgetRepository(FakePool(connection))

    assert await repository.release("intent-a") is True
    assert await repository.release("intent-a") is False
    assert await repository.sweep_expired(request().expires_at) == 3

    assert any("status = 'released'" in item[1] for item in connection.calls)
    assert any("status = 'expired'" in item[1] for item in connection.calls)


@pytest.mark.asyncio
async def test_postgres_reservation_locks_and_persists_sports_market_type() -> None:
    """PostgreSQL must serialize one-entry-per-sports-type decisions."""

    connection = FakeConnection()
    pool = FakePool(connection)
    connection.fetchrow_results = [
        None,
        {
            "task_total": Decimal("0"),
            "task_daily": Decimal("0"),
            "task_market": Decimal("0"),
            "task_has_entry": False,
            "global_market_has_entry": False,
            "task_has_sports_type_entry": False,
        },
        {
            "reservation_id": "sports-a",
            "follower_account": "account-a",
            "task_id": "task-a",
            "market_id": "game-a-moneyline",
            "sports_market_type": "moneyline",
            "amount": Decimal("5"),
            "reserved_at": datetime(2026, 8, 8, tzinfo=UTC),
            "expires_at": datetime(2026, 8, 8, 0, 2, tzinfo=UTC),
            "status": "reserved",
            "fill_id": None,
            "committed_notional": None,
        },
    ]
    repository = PostgresTaskBudgetRepository(pool)
    now = datetime(2026, 8, 8, tzinfo=UTC)

    decision = await repository.reserve(
        TaskBudgetReservationRequest(
            reservation_id="sports-a",
            key=TaskBudgetKey(
                follower_account="account-a",
                task_id="task-a",
                market_id="game-a-moneyline",
                sports_market_type="moneyline",
            ),
            amount=Decimal("5"),
            reserved_at=now,
            expires_at=now + timedelta(seconds=120),
        ),
        TaskSpendPolicy(one_entry_per_sports_type=True),
    )

    assert decision.allowed is True
    executed_sql = "\n".join(
        sql for method, sql, _args in connection.calls if method == "execute"
    )
    fetched_sql = "\n".join(
        sql for method, sql, _args in connection.calls if method == "fetchrow"
    )
    assert "sports:task-a:moneyline" in str(connection.calls)
    assert "sports_market_type" in executed_sql + fetched_sql