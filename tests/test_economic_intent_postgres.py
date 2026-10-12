"""Economic intent identity survives repricing, concurrency, and lost acknowledgements."""

import asyncio
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.integrations.polymarket.models import VenueSubmitResult
from pmcopy.interfaces.clob_gateway import ClobGateway
from pmcopy.storage.postgres.orders_repo import OrdersRepo
from pmcopy.storage.postgres.fills_repo import FillRecord, FillsRepo
from pmcopy.storage.postgres.positions_repo import PositionsRepo
from pmcopy.services.execution.fill_accounting import FillAccountingConflict
from tests.test_execution_fak_repost_ambiguity import _build_svc, _req
from tests.test_reservation_service import _pg_pool

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_repriced_restarted_intent_cannot_submit_a_second_economic_order():
    """Changing a tick or cap is not a new allocation of the source fill."""
    async with _pg_pool() as pool:
        gateway = MagicMock(spec=ClobGateway)
        gateway.submit_marketable_order = AsyncMock(
            return_value=VenueSubmitResult(
                success=True,
                order_id="unconfirmed-order",
                status="submitted",
            )
        )
        service = _build_svc(gateway=gateway)
        service.order_repo = OrdersRepo(pool)
        await service.execute(_req())
        restarted = _build_svc(gateway=gateway)
        restarted.order_repo = OrdersRepo(pool)
        await restarted.execute(
            replace(
                _req(),
                price_limit=Decimal("0.56"),
                tick_size=Decimal("0.005"),
            )
        )
        gateway.submit_marketable_order.assert_awaited_once()


async def test_concurrent_effective_keys_share_one_durable_intent_owner():
    """Distinct task allocations remain independent while repricings collide."""
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        request = {"signal_id": "task-a:source", "token_id": "123", "side": "BUY"}
        claims = await asyncio.gather(
            *(
                repo.try_acquire_submission_lock(f"price-{index}", "account", "leader", request)
                for index in range(6)
            )
        )
        assert sum(claims) == 1
        assert await repo.try_acquire_submission_lock(
            "task-b-key",
            "account",
            "leader",
            {**request, "signal_id": "task-b:source"},
        )


async def test_concurrent_cumulative_fill_observers_cannot_double_count():
    """A stale reconciliation watermark loses atomically and refreshes from the ledger."""
    async with _pg_pool() as pool:
        fills = FillsRepo(pool)
        positions = PositionsRepo(pool)
        base = FillRecord(
            user_id="account",
            leader_id="leader",
            condition_id="condition",
            token_id="123",
            order_id="order",
            side="BUY",
            size_tokens=Decimal("5"),
            price=Decimal("0.4"),
            fill_ts=1000,
            accounted_shares=Decimal("0"),
        )
        outcomes = await asyncio.gather(
            fills.append_fill_and_apply(base, positions),
            fills.append_fill_and_apply(replace(base, size_tokens=Decimal("10")), positions),
            return_exceptions=True,
        )
        assert sum(isinstance(result, FillAccountingConflict) for result in outcomes) == 1
        accounted = await fills.accounted_order_fill("account", "order")
        if accounted.shares < Decimal("10"):
            await fills.append_fill_and_apply(
                replace(
                    base,
                    size_tokens=Decimal("10") - accounted.shares,
                    accounted_shares=accounted.shares,
                    price=Decimal("0.6"),
                    fill_ts=1001,
                ),
                positions,
            )
        restored = await FillsRepo(pool).accounted_order_fill("account", "order")
        assert restored.shares == Decimal("10")
        assert len(await positions.list_open_positions("account")) == 1


async def test_fee_ledger_preserves_explicit_zero_missing_and_cumulative_totals():
    """An unknown fee remains nullable after restart; known fees retain exact units."""
    async with _pg_pool() as pool:
        fills = FillsRepo(pool)
        base = FillRecord(
            user_id="account",
            leader_id="leader",
            condition_id="condition",
            token_id="123",
            order_id="known",
            side="BUY",
            size_tokens=Decimal("5"),
            price=Decimal("0.4"),
            fill_ts=1000,
            fee=Decimal("0"),
        )
        await fills.append_fill(base)
        assert (await FillsRepo(pool).accounted_order_fill("account", "known")).fees == Decimal("0")
        await fills.append_fill(replace(base, fill_ts=1001, fee=Decimal("0.02")))
        totals = await fills.accounted_order_fill("account", "known")
        assert totals.fees == Decimal("0.02")
        assert totals.incremental_fee("0.03") == Decimal("0.01")
        await fills.append_fill(replace(base, order_id="unknown", fee=None))
        assert (await FillsRepo(pool).accounted_order_fill("account", "unknown")).fees is None


@pytest.mark.parametrize("pending_first", [True, False])
async def test_source_promotion_is_durable_without_collapsing_several_fills(pending_first):
    async with _pg_pool() as pool:
        repository = OrdersRepo(pool)
        base = {"token_id": "123", "side": "BUY", "condition_id": "condition"}
        pending = {
            **base,
            "signal_id": "task-a:pending",
            "recovery_metadata": {
                "source_promotion_id": "task-a:pending",
                "source_is_provisional": True,
            },
        }
        confirmed = {
            **base,
            "signal_id": "task-a:log7",
            "recovery_metadata": {
                "source_promotion_id": "task-a:pending",
                "source_is_provisional": False,
            },
        }
        first, second = (pending, confirmed) if pending_first else (confirmed, pending)
        assert await repository.try_acquire_submission_lock("first", "account", "leader", first)
        restarted = OrdersRepo(pool)
        assert not await restarted.try_acquire_submission_lock(
            "second", "account", "leader", second
        )
        if not pending_first:
            assert await restarted.try_acquire_submission_lock(
                "log8",
                "account",
                "leader",
                {
                    **confirmed,
                    "signal_id": "task-a:log8",
                },
            )
