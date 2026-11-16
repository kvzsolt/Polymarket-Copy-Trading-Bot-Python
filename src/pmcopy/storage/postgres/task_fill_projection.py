"""Project confirmed task fills into ownership lots in the accounting transaction."""

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import asyncpg

from pmcopy.services.copy_tasks.lots import DurableRiskLot, ProtectiveLotExitFill
from pmcopy.services.copy_tasks.protective_exits import FillRiskLot
from pmcopy.services.copy_tasks.reentry_lock import ProtectiveExitReason
from pmcopy.storage.postgres.risk_lot_repository import PostgresRiskLotRepository, RiskLotCodec
from pmcopy.storage.postgres.task_lot_capacity import AvailableLotShares

if TYPE_CHECKING:
    from pmcopy.storage.postgres.fills_repo import FillRecord


class PostgresTaskFillProjection:
    """Share the existing lot repository and the caller's atomic fill transaction."""

    def __init__(self, lots: PostgresRiskLotRepository) -> None:
        """Use the application-owned lot repository."""
        self._lots = lots
        self._codec = RiskLotCodec()

    async def apply(
        self,
        connection: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
        fill_id: int,
        record: "FillRecord",
    ) -> None:
        """Create exact task ownership from newly inserted authoritative BUY fills.

        Orders without task attribution remain legacy account allocations.
        Missing required task identity fails the encompassing transaction.
        """
        if record.order_id is None:
            return
        raw = await connection.fetchval(
            "SELECT request_json FROM orders WHERE user_id = $1 AND order_id = $2",
            record.user_id,
            record.order_id,
        )
        request = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(request, Mapping):
            return
        recovery = request.get("recovery_metadata")
        if not isinstance(recovery, Mapping):
            recovery = {}
        if record.side == "SELL":
            await self._apply_sell(connection, fill_id, record, recovery)
            return
        if not recovery.get("task_id"):
            return
        task_id = str(recovery["task_id"])
        task = await connection.fetchrow(
            "SELECT follower_account, leader_wallet FROM copy_tasks WHERE task_id = $1",
            task_id,
        )
        follower = recovery.get("task_follower_account", record.user_id)
        if task is None or task["follower_account"] != follower:
            raise ValueError("Fill task account attribution is unavailable")
        if str(task["leader_wallet"]).lower() != str(record.leader_id).lower():
            raise ValueError("Fill task leader attribution is inconsistent")
        policy = recovery.get("protective_risk", {})
        if not isinstance(policy, Mapping):
            raise ValueError("Fill protective policy snapshot is invalid")
        if record.fill_ts is None:
            raise ValueError("Confirmed task fill timestamp is required")
        await self._lots.create_in_connection(
            connection,
            DurableRiskLot(
                follower_account=record.user_id,
                leader_wallet=task["leader_wallet"],
                entry_fill_id=f"fill:{fill_id}",
                policy=self._codec.decode_policy(policy),
                lot=FillRiskLot(
                    lot_id=f"fill:{fill_id}",
                    task_id=task_id,
                    market_id=record.condition_id,
                    outcome_id=record.token_id,
                    entry_price=record.price,
                    original_shares=record.size_tokens,
                    remaining_shares=record.size_tokens,
                    peak_price=record.price,
                    opened_at=datetime.fromtimestamp(record.fill_ts, UTC),
                ),
            ),
        )

    async def _apply_sell(
        self,
        connection: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
        fill_id: int,
        record: "FillRecord",
        recovery: Mapping[str, object],
    ) -> None:
        task_id = recovery.get("task_id")
        lot_id = recovery.get("protective_lot_id")
        rows = await connection.fetch(
            "SELECT lot_id, remaining_shares FROM copy_task_risk_lots "
            "WHERE follower_account = $1 AND market_id = $2 AND outcome_id = $3 "
            "AND status = 'open' AND ($4::text IS NULL OR task_id = $4) "
            "AND ($5::text IS NULL OR lot_id = $5) ORDER BY opened_at, lot_id FOR UPDATE",
            record.user_id,
            record.condition_id,
            record.token_id,
            task_id,
            lot_id,
        )
        capacities = tuple(
            AvailableLotShares(row["lot_id"], row["remaining_shares"]) for row in rows
        )
        if recovery.get("reservation_id") is not None or task_id is not None:
            reservation_id = recovery.get("reservation_id")
            raw = await connection.fetchval(
                "SELECT metadata_json FROM order_reservations WHERE id = $1 AND user_id = $2",
                reservation_id,
                record.user_id,
            )
            metadata = json.loads(raw) if isinstance(raw, str) else raw
            allocations = metadata.get("lot_allocations") if isinstance(metadata, Mapping) else None
            if allocations is None:
                fallback_allocs = recovery.get("lot_allocations")
                if isinstance(fallback_allocs, dict):
                    allocations = fallback_allocs
            if allocations is None and task_id is None:
                allocations = {lot.lot_id: str(lot.shares) for lot in capacities}
            if not isinstance(allocations, dict):
                raise ValueError("Confirmed task SELL has no durable lot allocation")
            used = await connection.fetch(
                "SELECT x.lot_id, SUM(x.shares) AS shares "
                "FROM copy_task_protective_exit_fill_ledger x "
                "JOIN fills f ON x.fill_id = ('fill:' || f.id::text || ':' || x.lot_id) "
                "WHERE f.user_id = $1 AND f.order_id = $2 GROUP BY x.lot_id",
                record.user_id,
                record.order_id,
            )
            applied_claims = {row["lot_id"]: row["shares"] for row in used}
            claimed_lots: list[AvailableLotShares] = []
            for row in rows:
                allocated = Decimal(str(allocations.get(row["lot_id"], "0")))
                free_claim = allocated - applied_claims.get(row["lot_id"], Decimal(0))
                if not free_claim.is_finite() or free_claim < 0:
                    raise ValueError("Confirmed task SELL allocation is inconsistent")
                if free_claim > 0:
                    claimed_lots.append(
                        AvailableLotShares(
                            row["lot_id"],
                            min(row["remaining_shares"], free_claim),
                        )
                    )
            capacities = tuple(claimed_lots)
        remaining = record.size_tokens
        owned = sum((lot.shares for lot in capacities), Decimal(0))
        if task_id is not None and owned < remaining:
            raise ValueError("Confirmed SELL exceeds attributable task inventory")
        if record.fill_ts is None:
            raise ValueError("Confirmed SELL timestamp is required")
        occurred_at = datetime.fromtimestamp(record.fill_ts, UTC)
        for lot in capacities:
            if remaining <= 0:
                break
            shares = min(remaining, lot.shares)
            applied = await self._lots.record_exit_in_connection(
                connection,
                ProtectiveLotExitFill(
                    fill_id=f"fill:{fill_id}:{lot.lot_id}",
                    lot_id=lot.lot_id,
                    shares=shares,
                    price=record.price,
                    occurred_at=occurred_at,
                    level_target_shares=(
                        Decimal(str(recovery["protective_level_target_shares"]))
                        if recovery.get("protective_level_target_shares") is not None
                        else None
                    ),
                    level_id=str(recovery["protective_level_id"])
                    if recovery.get("protective_level_id") is not None
                    else None,
                ),
            )
            if lot_id is not None and applied.lot.lot.remaining_shares == 0:
                open_market_shares = await connection.fetchval(
                    "SELECT COALESCE(SUM(remaining_shares), 0) "
                    "FROM copy_task_risk_lots "
                    "WHERE follower_account = $1 AND task_id = $2 "
                    "AND market_id = $3 AND status = 'open'",
                    record.user_id,
                    applied.lot.lot.task_id,
                    record.condition_id,
                )
                if Decimal(str(open_market_shares or 0)) == 0:
                    reason = ProtectiveExitReason(str(recovery.get("protective_reason")))
                    await connection.execute(
                        "INSERT INTO market_reentry_locks "
                        "(follower_account, task_id, leader_wallet, market_id, reason, fill_id, locked_at) "
                        "SELECT follower_account, task_id, leader_wallet, $2, $3, $4, $5 "
                        "FROM copy_tasks WHERE task_id = $1 ON CONFLICT DO NOTHING",
                        applied.lot.lot.task_id,
                        record.condition_id,
                        reason.value,
                        f"fill:{fill_id}:{lot.lot_id}",
                        occurred_at,
                    )
            remaining -= shares
