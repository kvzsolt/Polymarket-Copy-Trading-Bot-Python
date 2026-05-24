"""Async asyncpg port of storage/sqlite/cursors_repo.py.

Same class names, same method names — bodies become ``async def`` and
take a connection pool instead of a db_path. Translation notes:
  * SQLite ``?`` → asyncpg ``$1, $2 …`` positional.
  * BEGIN IMMEDIATE → ``async with conn.transaction()`` (default isolation
    READ COMMITTED) + explicit ``SELECT … FOR UPDATE`` on the watermark
    row so two concurrent ingestion tasks cannot race past the
    monotonicity guard.
  * ``ON CONFLICT(k) DO UPDATE SET col = excluded.col`` is identical.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import asyncpg

from domain.ports.repositories import AbstractCursorRepository


@dataclass(frozen=True)
class TradeCursor:
    timestamp: int
    tx_hash: str
    trade_id: str | None = None


class CursorsRepo(AbstractCursorRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def get_cursor(self, leader_id: str) -> TradeCursor:
        leader_key = str(leader_id or "").lower()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT last_timestamp, cursor_tx_hash, cursor_trade_id
                FROM leader_cursors
                WHERE leader_wallet = $1
                LIMIT 1
                """,
                leader_key,
            )

        if not row:
            return TradeCursor(timestamp=0, tx_hash="", trade_id=None)

        return TradeCursor(
            timestamp=int(float(row["last_timestamp"] or 0)),
            tx_hash=str(row["cursor_tx_hash"] or ""),
            trade_id=str(row["cursor_trade_id"] or "") or None,
        )

    async def set_cursor(self, leader_id: str, cursor: TradeCursor) -> None:
        leader_key = str(leader_id or "").lower()
        async with self._pool.acquire() as conn:
            # Single transaction with FOR UPDATE on the contended row so
            # the SELECT → compare → UPSERT chain is atomic. The asyncpg
            # transaction() context-manager handles BEGIN / COMMIT /
            # ROLLBACK on exception automatically.
            async with conn.transaction():
                existing_row = await conn.fetchrow(
                    """
                    SELECT last_timestamp, cursor_tx_hash, cursor_trade_id
                    FROM leader_cursors
                    WHERE leader_wallet = $1
                    LIMIT 1
                    FOR UPDATE
                    """,
                    leader_key,
                )

                if existing_row:
                    existing = TradeCursor(
                        timestamp=int(float(existing_row["last_timestamp"] or 0)),
                        tx_hash=str(existing_row["cursor_tx_hash"] or ""),
                        trade_id=str(existing_row["cursor_trade_id"] or "") or None,
                    )
                    if _cursor_key(cursor) <= _cursor_key(existing):
                        # Watermark would regress — silently drop the
                        # write. The FOR UPDATE lock releases on commit.
                        return

                # cubic P1: ``WHERE`` guard makes the UPSERT monotonic
                # even when two concurrent first-writers both observe
                # "no row" under their FOR UPDATE (a non-existent row
                # cannot be locked, so the lock above doesn't serialize
                # them). With this WHERE clause, an older watermark
                # arriving second is silently dropped at the row level.
                await conn.execute(
                    """
                    INSERT INTO leader_cursors
                        (leader_wallet, last_timestamp, cursor_tx_hash,
                         cursor_trade_id, updated_at)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT(leader_wallet) DO UPDATE SET
                        last_timestamp  = excluded.last_timestamp,
                        cursor_tx_hash  = excluded.cursor_tx_hash,
                        cursor_trade_id = excluded.cursor_trade_id,
                        updated_at      = excluded.updated_at
                    WHERE (excluded.last_timestamp,
                           excluded.cursor_tx_hash,
                           COALESCE(excluded.cursor_trade_id, ''))
                          > (leader_cursors.last_timestamp,
                             leader_cursors.cursor_tx_hash,
                             COALESCE(leader_cursors.cursor_trade_id, ''))
                    """,
                    leader_key,
                    float(cursor.timestamp),
                    cursor.tx_hash,
                    cursor.trade_id,
                    time.time(),
                )


def _cursor_key(cursor: TradeCursor) -> tuple[int, str, str]:
    return (
        int(cursor.timestamp or 0),
        str(cursor.tx_hash or ""),
        str(cursor.trade_id or ""),
    )
