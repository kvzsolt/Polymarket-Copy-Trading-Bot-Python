"""Copied and protective exits reserve exact owned lots through one authority."""

import json
from dataclasses import replace
from decimal import Decimal

import pytest

from domain.order_lifecycle import IntentReason, IntentSide
from services.copy_tasks.domain import CopyTask
from services.execution.reservation_service import InsufficientFreeShares
from storage.postgres.copy_task_repository import PostgresCopyTaskRepository
from storage.postgres.risk_lot_repository import PostgresRiskLotRepository
from storage.postgres.fills_repo import FillRecord, FillsRepo
from storage.postgres.task_fill_projection import PostgresTaskFillProjection
from tests.test_reservation_service import _pg_pool, _build_service, MockClock, create_mock_intent
from tests.v1.test_durable_risk_lots import _lot
from services.copy_tasks.lots import ProtectiveLotExitFill
from services.copy_tasks.protective_exits import ProtectiveExitEngine

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.mark.parametrize("task_scope", ["task-a", None])
async def test_protective_reservation_cannot_be_reused_by_another_exit_intent(task_scope):
    """Account headroom in other lots is not permission to reserve this lot twice."""
    async with _pg_pool() as pool:
        service = _build_service(pool, MockClock(1000))
        await PostgresCopyTaskRepository(pool).create(
            CopyTask(
                task_id="task-a",
                name="A",
                follower_account="u1",
                leader_wallet=_lot().leader_wallet,
            )
        )
        lots = PostgresRiskLotRepository(pool)
        base = replace(
            _lot(),
            follower_account="u1",
            lot=replace(
                _lot().lot,
                market_id="cond1",
                outcome_id="1" * 70,
            ),
        )
        await lots.create_if_absent(base)
        await lots.create_if_absent(
            replace(
                base,
                entry_fill_id="buy-fill-b",
                lot=replace(base.lot, lot_id="lot-b"),
            )
        )
        await service.positions_repo.apply_fill(
            user_id="u1",
            leader_id=base.leader_wallet,
            condition_id="cond1",
            token_id="1" * 70,
            side="BUY",
            fill_size=Decimal("20"),
            fill_price=Decimal("0.4"),
        )
        first = replace(
            create_mock_intent(IntentSide.SELL, size=Decimal("6"), source_event_id="protect-a"),
            task_id="task-a",
            protective_lot_id="lot-a",
        )
        reservation = await service.reserve_for_intent(first)
        second = replace(first, key=replace(first.key, source_event_id="protect-b"))
        with pytest.raises(InsufficientFreeShares):
            await service.reserve_for_intent(second)
        copied = replace(
            first,
            key=replace(
                first.key,
                source_event_id="copy",
                reason=IntentReason.RISK_EXIT if task_scope is None else IntentReason.COPY_SELL,
            ),
            protective_lot_id=None,
            size=Decimal("14"),
            task_id=task_scope,
        )
        copied_reservation = await service.reserve_for_intent(copied)
        async with pool.acquire() as connection:
            raw = await connection.fetchval(
                "SELECT metadata_json FROM order_reservations WHERE id = $1",
                copied_reservation.id,
            )
        assert json.loads(raw)["lot_allocations"] == {"lot-a": "4", "lot-b": "10"}
        assert reservation.id != copied_reservation.id
        async with pool.acquire() as connection:
            await connection.execute(
                "INSERT INTO orders (idempotency_key, user_id, order_id, request_json) "
                "VALUES ('copy-order', 'u1', 'copy-order', $1::jsonb)",
                json.dumps(
                    {
                        "recovery_metadata": {
                            "task_id": task_scope,
                            "reservation_id": copied_reservation.id,
                        }
                    }
                ),
            )
        fills = FillsRepo(pool, task_projection=PostgresTaskFillProjection(lots))
        await fills.append_fill_and_apply(
            FillRecord(
                user_id="u1",
                leader_id=base.leader_wallet,
                order_id="copy-order",
                condition_id="cond1",
                token_id="1" * 70,
                side="SELL",
                size_tokens=Decimal("14"),
                price=Decimal("0.5"),
                fill_ts=1000,
            ),
            service.positions_repo,
        )
        assert (await lots.get("lot-a")).lot.remaining_shares == Decimal("6")
        assert (await lots.get("lot-b")).lot.remaining_shares == 0


async def test_partial_layer_target_is_durable_and_fill_replay_is_idempotent():
    """Restore the three unfilled shares of a five-share layer from PostgreSQL."""
    async with _pg_pool() as pool:
        lots = PostgresRiskLotRepository(pool)
        base = _lot()
        await PostgresCopyTaskRepository(pool).create(
            CopyTask(
                task_id="task-a",
                name="A",
                follower_account=base.follower_account,
                leader_wallet=base.leader_wallet,
            )
        )
        await lots.create_if_absent(base)
        fill = ProtectiveLotExitFill(
            fill_id="partial",
            lot_id="lot-a",
            shares=Decimal("2"),
            price=Decimal("0.5"),
            occurred_at=base.lot.opened_at,
            level_id="tp:0",
            level_target_shares=Decimal("5"),
        )
        await lots.record_exit_fill(fill)
        restored = PostgresRiskLotRepository(pool)
        assert not (await restored.record_exit_fill(fill)).created
        current = await restored.get("lot-a")
        decision = ProtectiveExitEngine().evaluate(
            current.lot, current.policy, current_price=Decimal("0.5")
        )
        assert decision.shares_to_exit == Decimal("3")
        await restored.record_exit_fill(
            replace(
                fill,
                fill_id="final-layer",
                shares=Decimal("3"),
                level_target_shares=Decimal("3"),
            )
        )
        finished = await lots.get("lot-a")
        assert finished.lot.remaining_shares == Decimal("5")
        assert finished.lot.triggered_levels == ("tp:0",)
        assert finished.lot.pending_exits == ()


async def test_legacy_copied_sell_cannot_reserve_task_owned_inventory():
    """An absent task ID is not authority over all tasks sharing the token."""
    async with _pg_pool() as pool:
        service = _build_service(pool, MockClock(1000))
        base = replace(
            _lot(),
            follower_account="u1",
            lot=replace(_lot().lot, market_id="cond1", outcome_id="1" * 70),
        )
        await PostgresCopyTaskRepository(pool).create(
            CopyTask(
                task_id="task-a",
                name="A",
                follower_account="u1",
                leader_wallet=base.leader_wallet,
            )
        )
        await PostgresRiskLotRepository(pool).create_if_absent(base)
        await service.positions_repo.apply_fill(
            user_id="u1",
            leader_id=base.leader_wallet,
            condition_id="cond1",
            token_id="1" * 70,
            side="BUY",
            fill_size=Decimal("10"),
            fill_price=Decimal(".4"),
        )
        legacy_sell = create_mock_intent(
            IntentSide.SELL, size=Decimal("1"), source_event_id="legacy-copy"
        )
        with pytest.raises(InsufficientFreeShares):
            await service.reserve_for_intent(legacy_sell)
        assert await service.reservations_repo.list_active() == []


@pytest.mark.parametrize("owner", [None, "another-leader"])
async def test_legacy_copied_sell_requires_matching_inventory_owner(owner):
    async with _pg_pool() as pool:
        service = _build_service(pool, MockClock(1000))
        await service.positions_repo.apply_fill(
            user_id="u1",
            leader_id=owner,
            condition_id="cond1",
            token_id="1" * 70,
            side="BUY",
            fill_size=Decimal(10),
            fill_price=Decimal(".4"),
        )
        with pytest.raises(InsufficientFreeShares):
            await service.reserve_for_intent(
                create_mock_intent(IntentSide.SELL, size=Decimal(1), source_event_id="copy")
            )
        assert await service.reservations_repo.list_active() == []
