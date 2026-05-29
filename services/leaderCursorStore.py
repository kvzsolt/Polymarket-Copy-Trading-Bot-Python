"""Async wrapper around the asyncpg-backed CursorsRepo.

PG-A5 migration: the wrapper API is async — every public method is
``async def`` — because the underlying ``storage.postgres.cursors_repo``
is async. The single in-tree caller (``services.tradeMonitor.trade_monitor``)
is already an ``async def`` coroutine so the cascade is bounded.

Construction is async too — the wrapper acquires the singleton asyncpg
pool from ``storage.postgres.connection.get_pool()`` which is only
ready after ``config.db.connect_db()`` has run. Callers that own a
``RuntimeUser`` lifecycle should use the ``create()`` classmethod
factory; legacy ``LeaderCursorStore()`` constructor is preserved
for tests that wire a pool explicitly.
"""

from __future__ import annotations

import asyncpg

from storage.postgres.connection import get_pool
from storage.postgres.cursors_repo import CursorsRepo, TradeCursor
from utils.logger import get_logger

logger = get_logger("LeaderCursorStore")


class LeaderCursorStore:
    def __init__(self, pool: asyncpg.Pool):
        self._repo = CursorsRepo(pool)

    @classmethod
    async def create(cls) -> "LeaderCursorStore":
        """Factory that resolves the singleton pool lazily.

        Avoids forcing callers to import ``get_pool`` themselves and
        keeps the construction async-explicit so a caller that forgot
        to ``await connect_db()`` fails fast with a clear
        ``RuntimeError`` rather than silently using a stale handle.
        """
        pool = await get_pool()
        return cls(pool)

    async def get_last_timestamp(self, leader_wallet: str) -> float:
        try:
            cursor = await self._repo.get_cursor(leader_wallet)
            return float(cursor.timestamp)
        except Exception as exc:
            logger.warning(f"Failed reading cursor for {leader_wallet[:10]}…: {exc}")
            return 0.0

    async def set_last_timestamp(self, leader_wallet: str, ts: float) -> None:
        try:
            current = await self._repo.get_cursor(leader_wallet)
            await self._repo.set_cursor(
                leader_wallet,
                TradeCursor(
                    timestamp=int(ts),
                    tx_hash=current.tx_hash,
                    trade_id=current.trade_id,
                ),
            )
        except Exception as exc:
            logger.warning(f"Failed writing cursor for {leader_wallet[:10]}…: {exc}")

    async def get_cursor(self, leader_wallet: str) -> TradeCursor:
        return await self._repo.get_cursor(leader_wallet)

    async def set_cursor(
        self, leader_wallet: str, cursor: TradeCursor
    ) -> None:
        await self._repo.set_cursor(leader_wallet, cursor)
