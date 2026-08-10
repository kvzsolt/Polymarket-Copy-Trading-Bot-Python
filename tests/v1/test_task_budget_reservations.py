"""Task-level spend and one-entry reservation contracts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from services.copy_tasks.budget import (
    InMemoryTaskBudgetRepository,
    TaskBudgetInvariantError,
    TaskBudgetKey,
    TaskBudgetReservationRequest,
    TaskBudgetReservationService,
)
from services.copy_tasks.domain import TaskSpendPolicy


def request(
    reservation_id: str,
    *,
    task_id: str = "task-a",
    amount: str = "10",
    market_id: str = "market-a",
    now: datetime | None = None,
) -> TaskBudgetReservationRequest:
    """Return one deterministic reservation request."""

    current = now or datetime(2026, 8, 8, 12, tzinfo=UTC)
    return TaskBudgetReservationRequest(
        reservation_id=reservation_id,
        key=TaskBudgetKey(
            follower_account="account-a",
            task_id=task_id,
            market_id=market_id,
        ),
        amount=Decimal(amount),
        reserved_at=current,
        expires_at=current + timedelta(minutes=2),
    )


@pytest.mark.asyncio
async def test_reserve_is_idempotent_for_same_identity_and_payload() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(max_total=Decimal("100"))
    item = request("reservation-a")

    first = await service.reserve(item, policy)
    second = await service.reserve(item, policy)

    assert first.allowed is True
    assert first.created is True
    assert second.allowed is True
    assert second.created is False
    assert second.reservation == first.reservation


@pytest.mark.asyncio
async def test_reservation_identity_conflict_fails_closed() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(max_total=Decimal("100"))

    await service.reserve(request("reservation-a", amount="10"), policy)
    conflict = await service.reserve(request("reservation-a", amount="11"), policy)

    assert conflict.allowed is False
    assert conflict.reason == "task_budget_reservation_identity_conflict"


@pytest.mark.asyncio
async def test_total_daily_and_market_caps_include_pending_reservations() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(
        max_total=Decimal("15"),
        max_daily=Decimal("15"),
        max_per_market=Decimal("15"),
    )

    assert (await service.reserve(request("one", amount="10"), policy)).allowed is True
    rejected = await service.reserve(request("two", amount="6"), policy)

    assert rejected.allowed is False
    assert rejected.reason == "task_max_total_spend_exceeded"


@pytest.mark.asyncio
async def test_task_one_entry_blocks_second_reservation_for_market() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(one_entry_per_market=True)

    await service.reserve(request("one"), policy)
    result = await service.reserve(request("two"), policy)

    assert result.allowed is False
    assert result.reason == "task_one_entry_per_market"


@pytest.mark.asyncio
async def test_global_one_entry_blocks_other_task_on_same_account_market() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    first_policy = TaskSpendPolicy(global_one_entry_per_market=True)
    second_policy = TaskSpendPolicy(global_one_entry_per_market=True)

    await service.reserve(request("one", task_id="task-a"), first_policy)
    result = await service.reserve(request("two", task_id="task-b"), second_policy)

    assert result.allowed is False
    assert result.reason == "global_one_entry_per_market"


@pytest.mark.asyncio
async def test_release_frees_pending_budget() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(max_total=Decimal("10"))

    await service.reserve(request("one", amount="10"), policy)
    assert await service.release("one") is True
    result = await service.reserve(request("two", amount="10"), policy)

    assert result.allowed is True


@pytest.mark.asyncio
async def test_commit_moves_reservation_to_fill_ledger_once() -> None:
    repository = InMemoryTaskBudgetRepository()
    service = TaskBudgetReservationService(repository)
    policy = TaskSpendPolicy(max_total=Decimal("10"))
    item = request("one", amount="10")

    await service.reserve(item, policy)
    assert (
        await service.commit(
            reservation_id="one",
            fill_id="fill-a",
            filled_notional=Decimal("8"),
            occurred_at=item.reserved_at,
        )
        is True
    )
    assert (
        await service.commit(
            reservation_id="one",
            fill_id="fill-a",
            filled_notional=Decimal("8"),
            occurred_at=item.reserved_at,
        )
        is False
    )

    result = await service.reserve(request("two", amount="3"), policy)
    assert result.allowed is False
    assert result.reason == "task_max_total_spend_exceeded"


@pytest.mark.asyncio
async def test_expired_reservation_does_not_consume_budget_or_one_entry() -> None:
    repository = InMemoryTaskBudgetRepository()
    service = TaskBudgetReservationService(repository)
    policy = TaskSpendPolicy(max_total=Decimal("10"), one_entry_per_market=True)
    old = datetime(2026, 8, 8, 10, tzinfo=UTC)

    await service.reserve(request("old", amount="10", now=old), policy)
    swept = await service.sweep_expired(datetime(2026, 8, 8, 12, tzinfo=UTC))
    result = await service.reserve(request("new", amount="10"), policy)

    assert swept == 1
    assert result.allowed is True


@pytest.mark.asyncio
async def test_released_identity_cannot_be_reactivated_by_same_request() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(max_total=Decimal("100"))
    item = request("one")

    await service.reserve(item, policy)
    await service.release("one")
    replay = await service.reserve(item, policy)

    assert replay.allowed is False
    assert replay.reason == "task_budget_reservation_not_active"


@pytest.mark.asyncio
async def test_committed_identity_is_not_treated_as_new_submission_permission() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(max_total=Decimal("100"))
    item = request("one")

    await service.reserve(item, policy)
    await service.commit(
        reservation_id="one",
        fill_id="fill-one",
        filled_notional=Decimal("8"),
        occurred_at=item.reserved_at,
    )
    replay = await service.reserve(item, policy)

    assert replay.allowed is False
    assert replay.reason == "task_budget_already_committed"


@pytest.mark.asyncio
async def test_one_entry_per_sports_type_is_atomic_across_different_markets() -> None:
    """A task must not reserve two live markets of the same sports line type."""

    repository = InMemoryTaskBudgetRepository()
    service = TaskBudgetReservationService(repository)
    policy = TaskSpendPolicy(one_entry_per_sports_type=True)
    now = datetime(2026, 8, 8, tzinfo=UTC)

    first = await service.reserve(
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
        policy,
    )
    blocked = await service.reserve(
        TaskBudgetReservationRequest(
            reservation_id="sports-b",
            key=TaskBudgetKey(
                follower_account="account-a",
                task_id="task-a",
                market_id="game-b-moneyline",
                sports_market_type="moneyline",
            ),
            amount=Decimal("5"),
            reserved_at=now,
            expires_at=now + timedelta(seconds=120),
        ),
        policy,
    )
    other_type = await service.reserve(
        TaskBudgetReservationRequest(
            reservation_id="sports-c",
            key=TaskBudgetKey(
                follower_account="account-a",
                task_id="task-a",
                market_id="game-b-total",
                sports_market_type="total",
            ),
            amount=Decimal("5"),
            reserved_at=now,
            expires_at=now + timedelta(seconds=120),
        ),
        policy,
    )

    assert first.allowed is True
    assert blocked.allowed is False
    assert blocked.reason == "task_one_entry_per_sports_type"
    assert other_type.allowed is True


@pytest.mark.asyncio
async def test_commit_rejects_fill_exceeding_reservation() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(max_total=Decimal("100"))
    item = request("one", amount="5")

    await service.reserve(item, policy)
    with pytest.raises(TaskBudgetInvariantError):
        await service.commit(
            reservation_id="one",
            fill_id="fill-x",
            filled_notional=Decimal("10"),
            occurred_at=item.reserved_at,
        )


@pytest.mark.asyncio
async def test_commit_rejects_conflicting_fill_identity() -> None:
    service = TaskBudgetReservationService(InMemoryTaskBudgetRepository())
    policy = TaskSpendPolicy(max_total=Decimal("100"))
    item = request("one")

    await service.reserve(item, policy)
    await service.commit(
        reservation_id="one",
        fill_id="fill-x",
        filled_notional=Decimal("8"),
        occurred_at=item.reserved_at,
    )
    with pytest.raises(TaskBudgetInvariantError):
        await service.commit(
            reservation_id="one",
            fill_id="fill-x",
            filled_notional=Decimal("9"),
            occurred_at=item.reserved_at,
        )
