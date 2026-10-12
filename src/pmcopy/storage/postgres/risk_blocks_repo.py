"""Async asyncpg port of storage/sqlite/risk_blocks_repo.py.

Behaviour invariants preserved verbatim:
  * ``mark_risk_block`` UPSERT preserves operator-prefixed clears so a
    re-trigger does not silently revert a manual override
    (review #23 / Batch 4).
  * ``clear_risk_block`` accepts optional ``triggered_at`` to pin the
    update to the exact row read by ``get_active_risk_block`` — closes
    the TOCTOU window (review #22 / Batch 4).

Translation notes:
  * jsonb metadata column → cast parameter with ``$N::jsonb`` on
    write; ``json.loads(row[...])`` on read because asyncpg returns
    jsonb as a JSON-encoded string by default.
  * ``cursor.rowcount`` → asyncpg ``execute`` returns ``"UPDATE N"``;
    parse the trailing integer.
"""

from __future__ import annotations

import json
import time
from typing import Any

import asyncpg


OPERATOR_CLEAR_PREFIX = "operator:"


def _row_to_block(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "user_id": row["user_id"],
        "condition_id": row["condition_id"],
        "token_id": row["token_id"],
        "reason": row["reason"],
        "source": row["source"],
        "triggered_at": row["triggered_at"],
        "expires_at": row["expires_at"],
        "cleared_at": row["cleared_at"],
        "clear_reason": row["clear_reason"],
        "leader_anchor": row["leader_anchor"],
        "metadata": json.loads(row["metadata_json"] or "{}"),
    }


def _affected(status: str) -> int:
    """Extract the row count from an asyncpg ``execute`` status string.

    Format examples: ``"UPDATE 1"``, ``"INSERT 0 3"``, ``"DELETE 0"``.
    Returns 0 when the status does not parse (defensive).
    """
    if not status:
        return 0
    parts = status.split()
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0


class RiskBlocksRepo:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def mark_risk_block(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        reason: str,
        source: str,
        leader_anchor: str | None = None,
        metadata: dict[str, Any] | None = None,
        expires_at: float | None = None,
    ) -> None:
        metadata_json = json.dumps(
            metadata or {}, sort_keys=True, separators=(",", ":"), default=str
        )
        now = time.time()

        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO risk_blocks (
                    user_id, condition_id, token_id, reason, source,
                    triggered_at, expires_at, cleared_at, clear_reason,
                    leader_anchor, metadata_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8, $9::jsonb)
                ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                    reason       = excluded.reason,
                    source       = excluded.source,
                    triggered_at = excluded.triggered_at,
                    expires_at   = excluded.expires_at,
                    -- Operator clears stay sticky through re-trigger UPSERT.
                    cleared_at = CASE
                        WHEN risk_blocks.clear_reason LIKE 'operator:%' THEN risk_blocks.cleared_at
                        ELSE NULL
                    END,
                    clear_reason = CASE
                        WHEN risk_blocks.clear_reason LIKE 'operator:%' THEN risk_blocks.clear_reason
                        ELSE NULL
                    END,
                    leader_anchor = COALESCE(excluded.leader_anchor, risk_blocks.leader_anchor),
                    metadata_json = excluded.metadata_json
                """,
                user_id,
                condition_id,
                token_id,
                reason,
                source,
                now,
                expires_at,
                leader_anchor,
                metadata_json,
            )

    async def get_active_risk_block(
        self, *, user_id: str, condition_id: str, token_id: str
    ) -> dict[str, Any] | None:
        now = time.time()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT user_id, condition_id, token_id, reason, source,
                       triggered_at, expires_at, cleared_at, clear_reason,
                       leader_anchor, metadata_json
                FROM risk_blocks
                WHERE user_id = $1 AND condition_id = $2 AND token_id = $3
                  AND cleared_at IS NULL
                  AND (expires_at IS NULL OR expires_at > $4)
                LIMIT 1
                """,
                user_id,
                condition_id,
                token_id,
                now,
            )
        if not row:
            return None
        return _row_to_block(row)

    async def clear_risk_block(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        clear_reason: str,
        triggered_at: float | None = None,
    ) -> bool:
        now = time.time()
        async with self._pool.acquire() as conn:
            if triggered_at is not None:
                status = await conn.execute(
                    """
                    UPDATE risk_blocks
                    SET cleared_at = $1,
                        clear_reason = $2
                    WHERE user_id = $3 AND condition_id = $4 AND token_id = $5
                      AND cleared_at IS NULL
                      AND (expires_at IS NULL OR expires_at > $6)
                      AND triggered_at = $7
                    """,
                    now,
                    clear_reason,
                    user_id,
                    condition_id,
                    token_id,
                    now,
                    triggered_at,
                )
            else:
                status = await conn.execute(
                    """
                    UPDATE risk_blocks
                    SET cleared_at = $1,
                        clear_reason = $2
                    WHERE user_id = $3 AND condition_id = $4 AND token_id = $5
                      AND cleared_at IS NULL
                      AND (expires_at IS NULL OR expires_at > $6)
                    """,
                    now,
                    clear_reason,
                    user_id,
                    condition_id,
                    token_id,
                    now,
                )
        return _affected(status) > 0

    async def list_active_risk_blocks(self, user_id: str | None = None) -> list[dict[str, Any]]:
        now = time.time()
        async with self._pool.acquire() as conn:
            if user_id is None:
                rows = await conn.fetch(
                    """
                    SELECT user_id, condition_id, token_id, reason, source,
                           triggered_at, expires_at, cleared_at, clear_reason,
                           leader_anchor, metadata_json
                    FROM risk_blocks
                    WHERE cleared_at IS NULL
                      AND (expires_at IS NULL OR expires_at > $1)
                    """,
                    now,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT user_id, condition_id, token_id, reason, source,
                           triggered_at, expires_at, cleared_at, clear_reason,
                           leader_anchor, metadata_json
                    FROM risk_blocks
                    WHERE cleared_at IS NULL
                      AND (expires_at IS NULL OR expires_at > $1)
                      AND user_id = $2
                    """,
                    now,
                    user_id,
                )
        return [_row_to_block(row) for row in rows]
