"""Async asyncpg port of storage/sqlite/fills_repo.py.

Translation:
  * ``INSERT OR IGNORE`` → ``INSERT … ON CONFLICT DO NOTHING``.
  * ``cur.rowcount`` → asyncpg's ``execute`` return string ``"INSERT 0 N"``
    (N = rows affected). Easier: use ``RETURNING id`` and check whether
    ``fetchval`` returned None to detect the dedup-skipped case.
  * ``CURRENT_TIMESTAMP`` default supplied by the column DEFAULT clause —
    drop from the INSERT column list entirely.
  * ``fill_json`` column is jsonb in PG; asyncpg auto-encodes a dict
    when the column is jsonb, but the legacy SQLite payload is a
    json-string. Pass ``json.dumps(...)`` so behaviour matches.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from decimal import Decimal

import asyncpg

from domain.ports.repositories import AbstractFillRepository


@dataclass
class FillRecord:
    user_id: str
    leader_id: str | None
    order_id: str | None
    condition_id: str
    token_id: str
    side: str
    size_tokens: Decimal
    price: Decimal
    fee: Decimal = Decimal("0")
    fill_ts: int | None = None


class FillsRepo(AbstractFillRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def append_fill(
        self, record: FillRecord, *, raw_payload: dict | None = None
    ) -> int:
        payload = raw_payload or {
            "user_id": record.user_id,
            "leader_id": record.leader_id,
            "order_id": record.order_id,
            "condition_id": record.condition_id,
            "token_id": record.token_id,
            "side": record.side,
            "size_tokens": str(record.size_tokens),
            "price": str(record.price),
            "fee": str(record.fee),
            "fill_ts": int(record.fill_ts or time.time()),
        }

        async with self._pool.acquire() as conn:
            # ON CONFLICT DO NOTHING + RETURNING id is the Postgres-native
            # equivalent of SQLite's INSERT OR IGNORE + lastrowid. When the
            # unique index (user_id, order_id, condition_id, side, fill_ts)
            # already covers this row, RETURNING yields nothing → fetchval
            # is None → return 0 so callers can distinguish duplicate from
            # fresh insert.
            new_id = await conn.fetchval(
                """
                INSERT INTO fills (
                    user_id, leader_id, order_id, condition_id, token_id, side,
                    size_tokens, price, fee, fill_ts, fill_json
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
                ON CONFLICT DO NOTHING
                RETURNING id
                """,
                record.user_id,
                record.leader_id,
                record.order_id,
                record.condition_id,
                record.token_id,
                record.side,
                str(record.size_tokens),
                str(record.price),
                str(record.fee),
                # cubic P2: ``record.fill_ts or time.time()`` would
                # overwrite a legitimate ``fill_ts == 0`` epoch with the
                # current time. ``is None`` preserves a deliberately-set
                # zero while still falling back when the field is unset.
                int(record.fill_ts if record.fill_ts is not None else time.time()),
                json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
            )
            return int(new_id) if new_id is not None else 0

    async def list_fills_for_user(self, user_id: str) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, user_id, leader_id, order_id, condition_id, token_id, side,
                       size_tokens, price, fee, fill_ts, created_at
                FROM fills
                WHERE user_id = $1
                ORDER BY fill_ts ASC, id ASC
                """,
                user_id,
            )
        return [dict(row) for row in rows]

    async def list_fills_for_user_condition(
        self, user_id: str, condition_id: str
    ) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, user_id, leader_id, order_id, condition_id, token_id, side,
                       size_tokens, price, fee, fill_ts, created_at
                FROM fills
                WHERE user_id = $1 AND condition_id = $2
                ORDER BY fill_ts ASC, id ASC
                """,
                user_id,
                condition_id,
            )
        return [dict(row) for row in rows]
