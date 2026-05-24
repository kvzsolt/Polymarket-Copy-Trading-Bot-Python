"""Async asyncpg port of storage/sqlite/market_structure_repo.py.

Translation notes:
  * SQLite ``remapped`` int (0/1) → Postgres ``boolean`` column. Pass
    Python bool, not 0/1.
  * ``cursor.lastrowid`` → ``RETURNING id`` + ``fetchval``.
  * ``cursor.rowcount`` → asyncpg execute status string.
  * ``INSERT OR IGNORE`` → ``INSERT ... ON CONFLICT DO NOTHING``.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any

import asyncpg


@dataclass(frozen=True)
class MarketStructureEvent:
    event_type: str  # split|merge|resolution
    condition_id: str
    old_token_id: str | None
    new_token_id: str | None
    ratio: str | None
    timestamp: float
    payload_json: str


def _affected(status: str) -> int:
    if not status:
        return 0
    parts = status.split()
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0


class MarketStructureRepo:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def record_event(self, event: MarketStructureEvent) -> int:
        async with self._pool.acquire() as conn:
            new_id = await conn.fetchval(
                """
                INSERT INTO market_structure_events (
                    event_type, condition_id, old_token_id, new_token_id,
                    ratio, event_ts, payload_json, remapped, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, FALSE, $8)
                RETURNING id
                """,
                event.event_type,
                event.condition_id,
                event.old_token_id,
                event.new_token_id,
                event.ratio,
                float(event.timestamp),
                event.payload_json,
                time.time(),
            )
        if new_id is None:
            raise RuntimeError("Insert failed: RETURNING id was NULL")
        return int(new_id)

    async def get_events_for_condition(
        self, condition_id: str
    ) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, event_type, condition_id, old_token_id, new_token_id,
                       ratio, event_ts, payload_json, remapped, created_at
                FROM market_structure_events
                WHERE condition_id = $1
                ORDER BY event_ts ASC, id ASC
                """,
                condition_id,
            )
        return [dict(r) for r in rows]

    async def get_pending_remappings(
        self, user_id: str | None = None
    ) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            if user_id is not None:
                rows = await conn.fetch(
                    """
                    SELECT mse.id, mse.event_type, mse.condition_id, mse.old_token_id,
                           mse.new_token_id, mse.ratio, mse.event_ts, mse.payload_json,
                           mse.remapped, mse.created_at
                    FROM market_structure_events mse
                    LEFT JOIN market_structure_event_applied msea
                        ON mse.id = msea.event_id AND msea.user_id = $1
                    WHERE mse.remapped = FALSE
                      AND mse.event_type IN ('split', 'merge')
                      AND (mse.old_token_id IS NOT NULL OR mse.payload_json IS NOT NULL)
                      AND mse.new_token_id IS NOT NULL
                      AND msea.event_id IS NULL
                    ORDER BY mse.event_ts ASC, mse.id ASC
                    """,
                    user_id,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT id, event_type, condition_id, old_token_id, new_token_id,
                           ratio, event_ts, payload_json, remapped, created_at
                    FROM market_structure_events
                    WHERE remapped = FALSE
                      AND event_type IN ('split', 'merge')
                      AND (old_token_id IS NOT NULL OR payload_json IS NOT NULL)
                      AND new_token_id IS NOT NULL
                    ORDER BY event_ts ASC, id ASC
                    """,
                )
        return [dict(r) for r in rows]

    async def mark_remapped(self, event_id: int) -> bool:
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                "UPDATE market_structure_events SET remapped = TRUE WHERE id = $1",
                event_id,
            )
        return _affected(status) > 0

    async def mark_applied_for_user(self, user_id: str, event_id: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO market_structure_event_applied (user_id, event_id, applied_at)
                VALUES ($1, $2, $3)
                ON CONFLICT DO NOTHING
                """,
                user_id,
                event_id,
                time.time(),
            )

    @staticmethod
    def payload(data: dict[str, Any]) -> str:
        return json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
