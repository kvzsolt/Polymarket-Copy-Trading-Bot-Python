"""Connect per-fill protective decisions to the canonical market and execution ports."""

from decimal import Decimal
from collections.abc import Iterable

from application.trading_context import TradingContext
from domain.execution import ExecutionOrderType
from services.copy_tasks.lots import DurableRiskLot, RiskLotService
from services.copy_tasks.protective_runtime import TaskProtectivePositionManager
from services.copy_tasks.protective_exits import ProtectiveExitDecision
from services.execution.coordinator import ExecutionCoordinator, ExecutionIntent
from services.market_data_service import MarketDataService


class TaskProtectiveExecutionAdapter:
    """Translate owned lot exits without creating another submission authority."""

    def __init__(
        self,
        *,
        market_data: MarketDataService,
        coordinator: ExecutionCoordinator,
        slippage_bps: int,
    ) -> None:
        """Inject the account's shared market-data and reservation services."""
        self._market_data = market_data
        self._coordinator = coordinator
        self._slippage_bps = slippage_bps

    async def executable_bid(self, lot: DurableRiskLot) -> Decimal:
        """Return a fresh best bid from the existing executable-book authority."""
        bundle = await self._market_data.require_fresh_data(
            lot.lot.outcome_id,
            side="SELL",
            condition_id=lot.lot.market_id,
        )
        bid = bundle.quote.best_bid
        if bid is None:
            raise RuntimeError("Protective exit requires an executable bid")
        return bid

    async def submit(
        self,
        lot: DurableRiskLot,
        decision: ProtectiveExitDecision,
        bid: Decimal,
    ) -> None:
        """Reserve exact task shares and submit an explicit bounded protective FAK."""
        signal_id = (
            f"protective:{lot.lot.lot_id}:{decision.level_id or decision.reason}:{lot.version}"
        )
        floor = bid * (Decimal(1) - Decimal(self._slippage_bps) / Decimal(10000))
        if not Decimal(0) < floor < Decimal(1):
            raise ValueError("Protective price floor must be in (0, 1)")
        await self._coordinator.submit_protective_exit(
            ExecutionIntent(
                user_id=lot.follower_account,
                leader_wallet=lot.leader_wallet,
                condition_id=lot.lot.market_id,
                token_id=lot.lot.outcome_id,
                side="SELL",
                size=decision.shares_to_exit,
                price=bid,
                notional_usdc=decision.shares_to_exit * bid,
                signal_id=signal_id,
                idempotency_key=signal_id,
                source="protective_exit",
                reason=decision.reason,
                task_id=lot.lot.task_id,
                protective_lot_id=lot.lot.lot_id,
                order_type=ExecutionOrderType.FAK,
                price_limit=floor,
                max_slippage_bps=self._slippage_bps,
                recovery_metadata={
                    "task_id": lot.lot.task_id,
                    "protective_lot_id": lot.lot.lot_id,
                    "protective_level_id": decision.level_id,
                    "protective_level_target_shares": str(decision.shares_to_exit)
                    if decision.level_id
                    else None,
                    "protective_reason": decision.reason,
                },
            )
        )


class TaskProtectiveRuntimeBindings:
    """Compose one protective owner per follower, including dynamically added leaders."""

    def __init__(self, lots: RiskLotService, *, slippage_bps: int) -> None:
        """Retain shared persistence and the existing execution slippage policy."""
        self._lots = lots
        self._slippage_bps = slippage_bps
        self._managers: dict[str, TaskProtectivePositionManager] = {}

    def bind(self, contexts: Iterable[object]) -> None:
        """Attach canonical account management without creating per-leader exit owners."""
        for context in contexts:
            if not isinstance(context, TradingContext):
                continue
            manager = self._managers.get(context.user_id)
            if manager is None:
                if context.market_data_service is None or context.execution_coordinator is None:
                    continue
                adapter = TaskProtectiveExecutionAdapter(
                    market_data=context.market_data_service,
                    coordinator=context.execution_coordinator,
                    slippage_bps=self._slippage_bps,
                )
                manager = TaskProtectivePositionManager(
                    follower_account=context.user_id,
                    lots=self._lots,
                    quotes=adapter,
                    exits=adapter,
                )
                self._managers[context.user_id] = manager
            context.task_position_manager = manager
