"""Async asyncpg port of storage/sqlite/risk_triggers_repo.py.

Translation notes:
  * ``cursor.lastrowid`` → ``INSERT ... RETURNING id`` + ``fetchval``.
  * ``cursor.rowcount`` → parse the asyncpg execute status string.
  * jsonb metadata column needs ``$N::jsonb`` cast on write.
"""

from __future__ import annotations

import json
import time
from typing import Any

import asyncpg


def _affected(status: str) -> int:
    if not status:
        return 0
    parts = status.split()
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0


def _row_to_trigger(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "id": row["id"],
        "user_id": row["user_id"],
        "condition_id": row["condition_id"],
        "token_id": row["token_id"],
        "reason": row["reason"],
        "source": row["source"],
        "triggered_at": row["triggered_at"],
        "fill_order_id": row["fill_order_id"],
        "fill_status": row["fill_status"],
        "metadata": json.loads(row["metadata_json"] or "{}"),
    }


class RiskTriggersRepo:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def record_trigger(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        reason: str,
        source: str = "risk_monitor",
        fill_status: str = "pending",
        fill_order_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        metadata_json = json.dumps(
            metadata or {}, sort_keys=True, separators=(",", ":"), default=str
        )
        now = time.time()
        async with self._pool.acquire() as conn:
            new_id = await conn.fetchval(
                """
                INSERT INTO risk_triggers (
                    user_id, condition_id, token_id, reason, source,
                    triggered_at, fill_order_id, fill_status, metadata_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
                RETURNING id
                """,
                user_id,
                condition_id,
                token_id,
                reason,
                source,
                now,
                fill_order_id,
                fill_status,
                metadata_json,
            )
        return int(new_id)

    async def update_fill_status(
        self,
        trigger_id: int,
        fill_status: str,
        fill_order_id: str | None = None,
    ) -> bool:
        async with self._pool.acquire() as conn:
            if fill_order_id is not None:
                status = await conn.execute(
                    """
                    UPDATE risk_triggers
                    SET fill_status = $1, fill_order_id = $2
                    WHERE id = $3
                    """,
                    fill_status,
                    fill_order_id,
                    trigger_id,
                )
            else:
                status = await conn.execute(
                    """
                    UPDATE risk_triggers
                    SET fill_status = $1
                    WHERE id = $2
                    """,
                    fill_status,
                    trigger_id,
                )
        return _affected(status) > 0

    async def list_triggers_for_position(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
    ) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, user_id, condition_id, token_id, reason, source,
                       triggered_at, fill_order_id, fill_status, metadata_json
                FROM risk_triggers
                WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
                ORDER BY triggered_at DESC
                """,
                user_id,
                condition_id,
                token_id,
            )
        return [_row_to_trigger(r) for r in rows]

    async def has_trigger_for_reason(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        reason: str,
        exclude_fill_statuses: tuple[str, ...] = ("failed",),
    ) -> bool:
        """True when a trigger with *reason* exists for the position scope.

        ``fill_status`` values in *exclude_fill_statuses* do not count — a
        failed trim never happened at the venue, so it must re-arm.
        ``pending`` counts: a crash between trigger and status update must
        not re-arm a sell.
        """
        async with self._pool.acquire() as conn:
            row = await conn.fetchval(
                """
                SELECT 1
                FROM risk_triggers
                WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
                  AND reason = $4
                  AND fill_status != ALL($5::text[])
                LIMIT 1
                """,
                user_id,
                condition_id,
                token_id,
                reason,
                list(exclude_fill_statuses),
            )
        return row is not None

    async def get_trigger(self, trigger_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, user_id, condition_id, token_id, reason, source,
                       triggered_at, fill_order_id, fill_status, metadata_json
                FROM risk_triggers
                WHERE id = $1
                """,
                trigger_id,
            )
        return _row_to_trigger(row) if row else None
