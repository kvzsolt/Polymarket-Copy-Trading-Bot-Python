"""PostgreSQL adapter for the singleton operator trading control."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class TradingControlState:
    enabled: bool
    generation: int
    reason: str
    updated_at: datetime
    updated_by: str


class TradingControlRepo:
    def __init__(self, pool) -> None:
        self._pool = pool

    @staticmethod
    def _state(row) -> TradingControlState:
        if row is None:
            raise RuntimeError("trading_control singleton row is missing")
        return TradingControlState(
            enabled=bool(row["enabled"]),
            generation=int(row["generation"]),
            reason=str(row["reason"]),
            updated_at=row["updated_at"],
            updated_by=str(row["updated_by"]),
        )

    async def get(self) -> TradingControlState:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT enabled, generation, reason, updated_at, updated_by
                FROM trading_control
                WHERE singleton_id = 1
                """
            )
        return self._state(row)

    async def set_enabled(
        self,
        enabled: bool,
        *,
        reason: str,
        updated_by: str,
    ) -> TradingControlState:
        normalized_reason = reason.strip()
        normalized_actor = updated_by.strip()
        if not normalized_reason:
            raise ValueError("trading control reason is required")
        if not normalized_actor:
            raise ValueError("trading control operator is required")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE trading_control
                SET enabled = $1,
                    generation = generation + 1,
                    reason = $2,
                    updated_at = now(),
                    updated_by = $3
                WHERE singleton_id = 1
                RETURNING enabled, generation, reason, updated_at, updated_by
                """,
                bool(enabled),
                normalized_reason,
                normalized_actor,
            )
        return self._state(row)


__all__ = ["TradingControlRepo", "TradingControlState"]
