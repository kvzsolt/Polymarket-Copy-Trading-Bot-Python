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
from typing import Any

import asyncpg

from ports.repositories import AbstractFillRepository
from services.execution.fill_accounting import AccountedOrderFill, FillAccountingConflict
from storage.postgres.task_fill_projection import PostgresTaskFillProjection


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
    fee: Decimal | None = None
    fill_ts: int | None = None
    # P0-1: stable reconciler dedup discriminator (the cumulative-matched total
    # at settlement). When set, the fill is unique on
    # ``idx_fills_dedup_v2 (user_id, order_id, condition_id, side, dedup_key)``
    # so re-applying the same settlement is skipped instead of double-applied.
    # ``None`` for non-reconciler fills (partial index excludes them).
    dedup_key: str | None = None
    accounted_shares: Decimal | None = None


class FillsRepo(AbstractFillRepository):
    def __init__(
        self, pool: asyncpg.Pool, *, task_projection: PostgresTaskFillProjection | None = None
    ):
        self._pool = pool
        self._task_projection = task_projection

    async def accounted_order_fill(self, user_id: str, order_id: str) -> AccountedOrderFill:
        """Read confirmed ledger totals scoped to one follower and venue order."""
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                "SELECT COALESCE(SUM(size_tokens::numeric), 0) AS shares, "
                "COALESCE(SUM(size_tokens::numeric * price::numeric), 0) AS collateral, "
                "CASE WHEN BOOL_AND(fee_evidence_known) IS FALSE THEN NULL "
                "ELSE COALESCE(SUM(fee::numeric), 0) END AS fees "
                "FROM fills WHERE user_id = $1 AND order_id = $2",
                user_id,
                order_id,
            )
        if row is None:
            raise RuntimeError("Accounted order fill totals are unavailable")
        return AccountedOrderFill(row["shares"], row["collateral"], row["fees"])

    async def append_fill(self, record: FillRecord, *, raw_payload: dict | None = None) -> int:
        payload = raw_payload or {
            "user_id": record.user_id,
            "leader_id": record.leader_id,
            "order_id": record.order_id,
            "condition_id": record.condition_id,
            "token_id": record.token_id,
            "side": record.side,
            "size_tokens": str(record.size_tokens),
            "price": str(record.price),
            "fee": None if record.fee is None else str(record.fee),
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
                    size_tokens, price, fee, fill_ts, fill_json, dedup_key, fee_evidence_known
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
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
                None if record.fee is None else str(record.fee),
                # cubic P2: ``record.fill_ts or time.time()`` would
                # overwrite a legitimate ``fill_ts == 0`` epoch with the
                # current time. ``is None`` preserves a deliberately-set
                # zero while still falling back when the field is unset.
                int(record.fill_ts if record.fill_ts is not None else time.time()),
                json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                record.dedup_key,
                record.fee is not None,
            )
            return int(new_id) if new_id is not None else 0

    async def append_fill_and_apply(
        self,
        record: FillRecord,
        positions_repo,
        *,
        raw_payload: dict | None = None,
    ) -> tuple[int, Any]:
        """Append a fill row and apply it to ``positions`` in a single transaction.

        Returns ``(fill_id, updated_position)``. ``fill_id == 0`` means the
        fill was a duplicate (existing ``(user_id, order_id, condition_id,
        side, fill_ts)`` row), and the position is **not** updated — this
        closes the double-count window where ``append_fill`` auto-commits
        a no-op duplicate but ``apply_fill`` runs anyway on the next call.

        This is the atomic alternative to calling ``append_fill`` and
        ``positions_repo.apply_fill`` sequentially on separate pool
        connections — required on the risk-monitor / trade-pipeline
        execution success path so a process crash between the two writes
        cannot leave the fill committed while the position is unchanged.
        """
        payload = raw_payload or {
            "user_id": record.user_id,
            "leader_id": record.leader_id,
            "order_id": record.order_id,
            "condition_id": record.condition_id,
            "token_id": record.token_id,
            "side": record.side,
            "size_tokens": str(record.size_tokens),
            "price": str(record.price),
            "fee": None if record.fee is None else str(record.fee),
            "fill_ts": int(record.fill_ts or time.time()),
        }
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if record.order_id is not None:
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"fill-accounting:{record.user_id}:{record.order_id}",
                    )
                if record.accounted_shares is not None:
                    accounted = await conn.fetchval(
                        "SELECT COALESCE(SUM(size_tokens::numeric), 0) FROM fills "
                        "WHERE user_id = $1 AND order_id = $2",
                        record.user_id,
                        record.order_id,
                    )
                    if accounted != record.accounted_shares:
                        raise FillAccountingConflict(
                            "Accounted order fills changed during settlement"
                        )
                new_id = await conn.fetchval(
                    """
                    INSERT INTO fills (
                        user_id, leader_id, order_id, condition_id, token_id, side,
                        size_tokens, price, fee, fill_ts, fill_json, dedup_key, fee_evidence_known
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
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
                    None if record.fee is None else str(record.fee),
                    int(record.fill_ts if record.fill_ts is not None else time.time()),
                    json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str),
                    record.dedup_key,
                    record.fee is not None,
                )
                fill_id = int(new_id) if new_id is not None else 0
                if fill_id == 0:
                    # Duplicate fill — do NOT re-apply to the position.
                    return 0, None
                updated = await positions_repo._apply_fill_in_conn(
                    conn,
                    user_id=record.user_id,
                    leader_id=record.leader_id,
                    condition_id=record.condition_id,
                    token_id=record.token_id,
                    side=record.side,
                    fill_size=record.size_tokens,
                    fill_price=record.price,
                )
                if self._task_projection is not None:
                    await self._task_projection.apply(conn, fill_id, record)
                return fill_id, updated

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

    async def list_fills_for_user_condition(self, user_id: str, condition_id: str) -> list[dict]:
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
