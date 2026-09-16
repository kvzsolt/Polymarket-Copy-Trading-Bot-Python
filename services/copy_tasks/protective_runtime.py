"""Manage durable task allocations through the existing protective evaluator."""

import asyncio
import logging
from decimal import Decimal
from typing import Protocol

from services.copy_tasks.lots import DurableRiskLot, RiskLotService
from services.copy_tasks.protective_exits import ProtectiveExitDecision, ProtectiveExitEngine


class ExecutableLotQuoteReader(Protocol):
    """Read a fresh executable SELL quote for an owned allocation."""

    async def executable_bid(self, lot: DurableRiskLot) -> Decimal:
        """Return a validated best bid, raising when evidence is unavailable."""
        ...


class ProtectiveLotSubmitter(Protocol):
    """Submit an allocation through the shared reservation/execution authority."""

    async def submit(
        self,
        lot: DurableRiskLot,
        decision: ProtectiveExitDecision,
        bid: Decimal,
    ) -> None:
        """Submit the evaluated shares with an explicit price floor."""
        ...


class ProtectiveLotLifecycleReader(Protocol):
    """Identify settled claims from canonical account inventory."""

    async def is_settled(self, lot: DurableRiskLot) -> bool:
        """Return True only for an explicitly settled canonical holding."""
        ...


class TaskProtectivePositionManager:
    """Own one account's restored lots independently of task/leader activation."""

    def __init__(
        self,
        *,
        follower_account: str,
        lots: RiskLotService,
        quotes: ExecutableLotQuoteReader,
        exits: ProtectiveLotSubmitter,
        lifecycle: ProtectiveLotLifecycleReader | None = None,
    ) -> None:
        """Inject persistence, quote, and execution ports without owning transports."""
        self._follower_account = follower_account
        self._lots = lots
        self._quotes = quotes
        self._exits = exits
        self._lifecycle = lifecycle
        self._engine = ProtectiveExitEngine()
        self._lock = asyncio.Lock()

    async def run_once(self) -> None:
        """Evaluate owned lots against fresh bids; leave failed evidence visible.

        The existing risk-loop lifecycle schedules this operation. Task pause,
        deletion, and leader monitoring do not change ownership of open lots.
        """
        async with self._lock:
            failed = 0
            for current in await self._lots.list_open(self._follower_account):
                if not current.policy.enabled:
                    continue
                policy = current.policy
                if not (
                    policy.layered_stop_loss
                    or policy.layered_take_profit
                    or any(
                        value is not None
                        for value in (
                            policy.stop_loss_percent,
                            policy.stop_loss_price,
                            policy.take_profit_percent,
                            policy.take_profit_price,
                            policy.trailing_stop_percent,
                        )
                    )
                ):
                    continue
                try:
                    if self._lifecycle is not None and await self._lifecycle.is_settled(current):
                        continue
                    bid = await self._quotes.executable_bid(current)
                    if not bid.is_finite() or not Decimal(0) < bid < Decimal(1):
                        raise ValueError("Executable protective bid must be in (0, 1)")
                    current = await self._lots.observe_price(current.lot.lot_id, bid)
                    decision = self._engine.evaluate(current.lot, current.policy, current_price=bid)
                    if decision.should_exit:
                        await self._exits.submit(current, decision, bid)
                except Exception as exc:
                    failed += 1
                    logging.getLogger(__name__).warning(
                        "task_protective_lot_unavailable",
                        extra={"lot_id": current.lot.lot_id, "error_type": type(exc).__name__},
                    )
            if failed:
                raise RuntimeError(
                    f"Protective evidence or submission unavailable for {failed} lots"
                )
