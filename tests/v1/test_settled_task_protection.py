"""Settled canonical claims do not request task protective quotes or exits."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest

from pmcopy.application.trading_context import TradingContext
from pmcopy.domain.position_lifecycle import PositionLifecycle
from pmcopy.orchestrators.copy_tasks.task_protection import TaskProtectiveRuntimeBindings
from pmcopy.services.copy_tasks.lots import InMemoryRiskLotRepository, RiskLotService
from pmcopy.services.copy_tasks.protective_exits import ProtectiveRiskPolicy
from pmcopy.services.execution.coordinator import ExecutionCoordinator
from pmcopy.services.market_data.market_data_service import MarketDataService
from tests.v1.test_durable_risk_lots import _lot


@pytest.fixture
async def protective_context():
    """Bind the real protective factory to deterministic canonical ports."""
    lot = replace(_lot(), policy=ProtectiveRiskPolicy(take_profit_price=Decimal("0.7")))
    lots = RiskLotService(InMemoryRiskLotRepository())
    await lots.create_from_entry_fill(lot)
    positions = SimpleNamespace(get_position=AsyncMock())
    market = SimpleNamespace(
        require_fresh_data=AsyncMock(
            return_value=SimpleNamespace(quote=SimpleNamespace(best_bid=Decimal("0.75")))
        )
    )
    coordinator = SimpleNamespace(submit_protective_exit=AsyncMock())
    context = TradingContext(
        user_id=lot.follower_account,
        wallet_address=lot.leader_wallet,
        client=None,
        positions_repo=positions,
        fills_repo=None,
        orders_repo=None,
        risk_blocks_repo=None,
        pnl_tracker=None,
        risk_engine=None,
        accounting_service=None,
        position_manager=None,
        market_data_service=cast(MarketDataService, market),
        execution_coordinator=cast(ExecutionCoordinator, coordinator),
    )
    TaskProtectiveRuntimeBindings(lots, slippage_bps=100).bind([context])
    return context, lot, lots, positions, market, coordinator


class TestSettledTaskProtection:
    """Preserve durable ownership while filtering explicit settlement evidence."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("payout", [Decimal(0), Decimal(1), None])
    async def test_settled_lot_skips_quote_and_exit_without_mutating_history(
        self, protective_context, payout
    ) -> None:
        """Lifecycle alone excludes settled claims, including unknown payout marks."""
        context, lot, lots, positions, market, coordinator = protective_context
        positions.get_position.return_value = SimpleNamespace(
            lifecycle=PositionLifecycle.SETTLED, settlement_price=payout
        )

        await context.task_position_manager.run_once()

        market.require_fresh_data.assert_not_awaited()
        coordinator.submit_protective_exit.assert_not_awaited()
        positions.get_position.assert_awaited_once_with(
            lot.follower_account, lot.lot.market_id, token_id=lot.lot.outcome_id
        )
        assert await lots.get(lot.lot.lot_id) == lot

    @pytest.mark.asyncio
    async def test_settled_lot_does_not_prevent_other_active_lot_protection(
        self, protective_context
    ) -> None:
        """An active lot still evaluates and submits through the canonical owner."""
        context, lot, lots, positions, market, coordinator = protective_context
        active = replace(
            lot,
            entry_fill_id="buy-active",
            lot=replace(lot.lot, lot_id="lot-active", outcome_id="token-active"),
        )
        await lots.create_from_entry_fill(active)

        async def position_for_lot(user_id, condition_id, *, token_id):
            return SimpleNamespace(
                lifecycle=(
                    PositionLifecycle.SETTLED
                    if token_id == lot.lot.outcome_id
                    else PositionLifecycle.ACTIVE
                )
            )

        positions.get_position.side_effect = position_for_lot
        await context.task_position_manager.run_once()

        market.require_fresh_data.assert_awaited_once_with(
            active.lot.outcome_id, side="SELL", condition_id=active.lot.market_id
        )
        coordinator.submit_protective_exit.assert_awaited_once()
        assert (
            coordinator.submit_protective_exit.call_args.args[0].token_id == active.lot.outcome_id
        )
        assert await lots.get(lot.lot.lot_id) == lot

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "position",
        [None, SimpleNamespace(), SimpleNamespace(lifecycle=PositionLifecycle.UNKNOWN)],
    )
    async def test_missing_lifecycle_cannot_hide_unavailable_protective_evidence(
        self, protective_context, position
    ) -> None:
        """Unknown or absent canonical rows retain existing quote failure behavior."""
        context, lot, lots, positions, market, coordinator = protective_context
        positions.get_position.return_value = position
        market.require_fresh_data.side_effect = RuntimeError("book unavailable")

        with pytest.raises(RuntimeError, match="Protective evidence or submission unavailable"):
            await context.task_position_manager.run_once()

        market.require_fresh_data.assert_awaited_once()
        coordinator.submit_protective_exit.assert_not_awaited()
        assert await lots.get(lot.lot.lot_id) == lot

    @pytest.mark.asyncio
    async def test_lifecycle_read_failure_is_visible_and_sanitized(
        self, protective_context, caplog
    ) -> None:
        """A failed canonical read neither suppresses protection nor submits an exit."""
        context, lot, lots, positions, market, coordinator = protective_context
        positions.get_position.side_effect = RuntimeError("synthetic-private-detail")

        with pytest.raises(RuntimeError, match="Protective evidence or submission unavailable"):
            await context.task_position_manager.run_once()

        market.require_fresh_data.assert_not_awaited()
        coordinator.submit_protective_exit.assert_not_awaited()
        assert await lots.get(lot.lot.lot_id) == lot
        assert "synthetic-private-detail" not in caplog.text
