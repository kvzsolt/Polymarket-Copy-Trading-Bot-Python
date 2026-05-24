"""Market-structure event ingestion + position reconciliation.

PG-A5 port: every public method is ``async def``. Uses
``storage.postgres.market_structure_repo.MarketStructureRepo`` for
event lifecycle, and the asyncpg pool for the per-user reconciliation
SQL that mutates the ``positions`` table directly (split → token_id
swap; merge → multi-row coalesce into target token).

Behaviour invariants preserved:
  * Decimal cost-basis aggregation for merge. Float would accumulate
    IEEE-754 error across many rows and corrupt PnL / re-entry
    decisions. ``str()`` coercion through asyncpg keeps the on-disk
    Decimal exact.
  * ``INSERT OR IGNORE INTO market_structure_event_applied`` →
    ``ON CONFLICT DO NOTHING`` — idempotent per (user_id, event_id).
  * Whole per-user reconciliation runs inside one
    ``async with conn.transaction()`` so a mid-batch crash either
    applies every event for that user or none.
"""

from __future__ import annotations

import json
import time
from decimal import Decimal

import asyncpg

from storage.postgres.connection import get_pool
from storage.postgres.market_structure_repo import (
    MarketStructureEvent,
    MarketStructureRepo,
)


class MarketStructureException(Exception):
    """Exception for market structure operation failures."""


def _affected(status: str) -> int:
    if not status:
        return 0
    parts = status.split()
    try:
        return int(parts[-1])
    except (ValueError, IndexError):
        return 0


class MarketStructureService:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        self.repo = MarketStructureRepo(pool)

    @classmethod
    async def create(cls) -> "MarketStructureService":
        pool = await get_pool()
        return cls(pool)

    async def handle_split(
        self,
        *,
        condition_id: str,
        old_token_id: str,
        new_token_id: str,
        ratio: str,
    ) -> int:
        event = MarketStructureEvent(
            event_type="split",
            condition_id=condition_id,
            old_token_id=old_token_id,
            new_token_id=new_token_id,
            ratio=ratio,
            timestamp=time.time(),
            payload_json=self.repo.payload(
                {
                    "kind": "split",
                    "condition_id": condition_id,
                    "old_token_id": old_token_id,
                    "new_token_id": new_token_id,
                    "ratio": ratio,
                }
            ),
        )
        return await self.repo.record_event(event)

    async def handle_merge(
        self,
        *,
        condition_id: str,
        source_token_ids: list[str],
        target_token_id: str,
    ) -> int:
        event = MarketStructureEvent(
            event_type="merge",
            condition_id=condition_id,
            old_token_id=None,
            new_token_id=target_token_id,
            ratio=None,
            timestamp=time.time(),
            payload_json=self.repo.payload(
                {
                    "kind": "merge",
                    "condition_id": condition_id,
                    "source_token_ids": source_token_ids,
                    "target_token_id": target_token_id,
                }
            ),
        )
        return await self.repo.record_event(event)

    async def handle_resolution(
        self, *, condition_id: str, winning_outcome: int
    ) -> int:
        event = MarketStructureEvent(
            event_type="resolution",
            condition_id=condition_id,
            old_token_id=None,
            new_token_id=None,
            ratio=None,
            timestamp=time.time(),
            payload_json=self.repo.payload(
                {
                    "kind": "resolution",
                    "condition_id": condition_id,
                    "winning_outcome": winning_outcome,
                }
            ),
        )
        return await self.repo.record_event(event)

    async def get_remap_for_token(self, token_id: str) -> str | None:
        pending = await self.repo.get_pending_remappings()
        for event in pending:
            if str(event.get("old_token_id") or "") == token_id:
                return str(event.get("new_token_id") or "") or None
        return None

    async def reconcile_positions(self, user_id: str) -> int:
        pending = await self.repo.get_pending_remappings(user_id=user_id)
        if not pending:
            return 0
        updated = 0

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for event in pending:
                    event_type = str(event.get("event_type") or "")
                    condition_id = str(event.get("condition_id") or "")
                    if event_type == "split":
                        old_token = str(event.get("old_token_id") or "")
                        new_token = str(event.get("new_token_id") or "")
                        if not old_token or not new_token or not condition_id:
                            continue
                        status = await conn.execute(
                            """
                            UPDATE positions
                            SET token_id = $1
                            WHERE user_id = $2 AND condition_id = $3 AND token_id = $4
                            """,
                            new_token,
                            user_id,
                            condition_id,
                            old_token,
                        )
                        updated += _affected(status)
                    elif event_type == "merge":
                        try:
                            payload_obj = json.loads(
                                str(event.get("payload_json") or "{}")
                            )
                        except Exception:
                            continue
                        source_token_ids = [
                            str(token_id).strip()
                            for token_id in (
                                payload_obj.get("source_token_ids") or []
                            )
                            if str(token_id).strip()
                        ]
                        target_token_id = str(
                            payload_obj.get("target_token_id")
                            or event.get("new_token_id")
                            or ""
                        ).strip()
                        if (
                            not condition_id
                            or not source_token_ids
                            or not target_token_id
                        ):
                            continue
                        rows = await conn.fetch(
                            """
                            SELECT user_id, condition_id, token_id, leader_id,
                                   size_tokens, avg_entry_price
                            FROM positions
                            WHERE user_id = $1 AND condition_id = $2
                                  AND token_id = ANY($3::text[])
                            """,
                            user_id,
                            condition_id,
                            source_token_ids,
                        )
                        if not rows:
                            continue
                        # Decimal accumulation: float would accumulate IEEE-754
                        # error across many rows and corrupt PnL / re-entry
                        # decisions. str() through asyncpg keeps the on-disk
                        # Decimal exact.
                        total_tokens = Decimal("0")
                        weighted_cost = Decimal("0")
                        leader_id = None
                        for row in rows:
                            size = Decimal(str(row["size_tokens"] or "0"))
                            entry = Decimal(str(row["avg_entry_price"] or "0"))
                            total_tokens += size
                            weighted_cost += size * entry
                            leader_id = leader_id or row["leader_id"]
                        if total_tokens <= 0:
                            continue
                        avg_entry_price = weighted_cost / total_tokens
                        await conn.execute(
                            """
                            DELETE FROM positions
                            WHERE user_id = $1 AND condition_id = $2
                                  AND token_id = ANY($3::text[])
                            """,
                            user_id,
                            condition_id,
                            source_token_ids,
                        )
                        await conn.execute(
                            """
                            INSERT INTO positions (
                                user_id, condition_id, token_id, leader_id,
                                size_tokens, avg_entry_price, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, now())
                            ON CONFLICT(user_id, condition_id, token_id) DO UPDATE SET
                                leader_id       = excluded.leader_id,
                                size_tokens     = excluded.size_tokens,
                                avg_entry_price = excluded.avg_entry_price,
                                updated_at      = excluded.updated_at
                            """,
                            user_id,
                            condition_id,
                            target_token_id,
                            leader_id,
                            str(total_tokens),
                            str(avg_entry_price),
                        )
                        updated += len(rows)

                    await conn.execute(
                        """
                        INSERT INTO market_structure_event_applied
                            (user_id, event_id, applied_at)
                        VALUES ($1, $2, $3)
                        ON CONFLICT DO NOTHING
                        """,
                        user_id,
                        event.get("id"),
                        time.time(),
                    )
        return updated
