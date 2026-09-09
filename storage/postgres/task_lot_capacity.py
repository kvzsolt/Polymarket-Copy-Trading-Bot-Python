"""Reserve exact task-owned lots inside the canonical account reservation transaction."""

import json
from dataclasses import dataclass
from decimal import Decimal

import asyncpg

from domain.order_lifecycle import IntentReason, OrderIntent


@dataclass(frozen=True, slots=True)
class AvailableLotShares:
    """Free shares in one durable lot after active claims."""

    lot_id: str
    shares: Decimal


@dataclass(frozen=True, slots=True)
class LotCapacitySnapshot:
    """Gross attributed holdings and free lot claims within one locked scope."""

    lots: tuple[AvailableLotShares, ...]
    attributed_shares: Decimal


class PostgresTaskLotCapacity:
    """Resolve lot ownership using the caller's transaction and token-scope lock."""

    async def available(
        self,
        connection: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
        intent: OrderIntent,
    ) -> LotCapacitySnapshot:
        """Return FIFO lot capacities; unknown existing ownership blocks admission."""
        lots = await connection.fetch(
            "SELECT lot_id, remaining_shares FROM copy_task_risk_lots "
            "WHERE follower_account = $1 AND ($2::text IS NULL OR task_id = $2) AND market_id = $3 "
            "AND outcome_id = $4 AND status = 'open' ORDER BY opened_at, lot_id FOR SHARE",
            intent.key.user_id,
            intent.task_id,
            intent.key.condition_id,
            intent.key.token_id,
        )
        active = await connection.fetch(
            "SELECT metadata_json FROM order_reservations "
            "WHERE user_id = $1 AND condition_id = $2 AND token_id = $3 "
            "AND side = 'SELL' AND status = 'active'",
            intent.key.user_id,
            intent.key.condition_id,
            intent.key.token_id,
        )
        claimed: dict[str, Decimal] = {}
        attributed = sum((row["remaining_shares"] for row in lots), Decimal(0))
        if intent.task_id is None and intent.key.reason is IntentReason.COPY_SELL:
            return LotCapacitySnapshot((), attributed)
        for row in active:
            metadata = json.loads(row["metadata_json"])
            if intent.task_id is not None and metadata.get("task_id") not in (None, intent.task_id):
                continue
            allocations = metadata.get("lot_allocations")
            if not isinstance(allocations, dict):
                return LotCapacitySnapshot((), attributed)
            for lot_id, raw in allocations.items():
                quantity = Decimal(str(raw))
                if not quantity.is_finite() or quantity < 0:
                    raise ValueError("Persisted lot reservation is invalid")
                claimed[lot_id] = claimed.get(lot_id, Decimal(0)) + quantity
        available = tuple(
            AvailableLotShares(
                row["lot_id"],
                max(Decimal(0), row["remaining_shares"] - claimed.get(row["lot_id"], Decimal(0))),
            )
            for row in lots
            if intent.protective_lot_id is None or row["lot_id"] == intent.protective_lot_id
        )
        return LotCapacitySnapshot(available, attributed)

    @staticmethod
    def allocate(lots: tuple[AvailableLotShares, ...], shares: Decimal) -> dict[str, str]:
        """Claim exactly the requested shares in deterministic lot order."""
        allocations: dict[str, str] = {}
        remaining = shares
        if shares == 0:
            return allocations
        for lot in lots:
            allocated = min(remaining, lot.shares)
            if allocated > 0:
                allocations[lot.lot_id] = format(allocated.normalize(), "f")
                remaining -= allocated
            if remaining == 0:
                return allocations
        raise ValueError("Insufficient attributable lot capacity")
