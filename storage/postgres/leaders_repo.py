"""Async asyncpg port of storage/sqlite/leaders_repo.py.

Translation:
  * ``active`` column is native Postgres ``boolean`` (was integer
    0/1 in SQLite). Pass Python ``bool`` directly.
  * Parameter style ``?`` → ``$N``.
  * Single transaction wraps the bulk UPSERT loop so a mid-batch
    crash does not leave a partial set of leaders.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

import asyncpg

from domain.ports.repositories import AbstractLeaderRepository


@dataclass
class LeaderCandidateRecord:
    leader_id: str
    score: float
    recent_trades: int
    active: bool
    rejection_reason: str | None = None
    source: str | None = None


@dataclass
class LeaderTierRecord:
    wallet: str
    follow_tier: str
    tier_changed_at: float | None


class LeadersRepo(AbstractLeaderRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def upsert_candidates(
        self, records: list[LeaderCandidateRecord]
    ) -> None:
        if not records:
            return
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for rec in records:
                    await conn.execute(
                        """
                        INSERT INTO discovered_leaders (
                            wallet, composite_score, total_positions, active,
                            rejection_reason, source, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT(wallet) DO UPDATE SET
                            composite_score  = excluded.composite_score,
                            total_positions  = excluded.total_positions,
                            active           = excluded.active,
                            rejection_reason = excluded.rejection_reason,
                            source           = excluded.source,
                            updated_at       = excluded.updated_at
                        """,
                        rec.leader_id.lower(),
                        float(rec.score),
                        int(rec.recent_trades),
                        bool(rec.active),
                        rec.rejection_reason,
                        rec.source,
                        time.time(),
                    )

    async def list_active(self) -> list[str]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT wallet
                FROM discovered_leaders
                WHERE active = TRUE
                ORDER BY composite_score DESC, wallet ASC
                """
            )
        return [str(r["wallet"]) for r in rows]

    async def get_tier(self, wallet: str) -> LeaderTierRecord:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT wallet, follow_tier, tier_changed_at FROM discovered_leaders WHERE wallet = $1",
                wallet.lower(),
            )
        if row is None:
            return LeaderTierRecord(
                wallet=wallet.lower(),
                follow_tier="watchlist",
                tier_changed_at=None,
            )
        return LeaderTierRecord(
            wallet=str(row["wallet"]),
            follow_tier=str(row["follow_tier"]) if row["follow_tier"] else "watchlist",
            tier_changed_at=(
                float(row["tier_changed_at"]) if row["tier_changed_at"] else None
            ),
        )

    async def update_tier(
        self, wallet: str, follow_tier: str, tier_changed_at: float
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE discovered_leaders
                SET follow_tier = $1, tier_changed_at = $2, updated_at = $3
                WHERE wallet = $4
                """,
                follow_tier,
                tier_changed_at,
                time.time(),
                wallet.lower(),
            )

    async def get_tier_for_leader(self, wallet: str) -> str:
        rec = await self.get_tier(wallet)
        return rec.follow_tier

    async def get_leaders_by_tier(self, tier: str) -> list[str]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT wallet FROM discovered_leaders WHERE follow_tier = $1",
                tier,
            )
        return [row["wallet"] for row in rows]
