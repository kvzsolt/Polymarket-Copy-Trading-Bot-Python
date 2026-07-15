"""Leader roster repository backed by the single discovery schema."""

from __future__ import annotations

import time
from dataclasses import dataclass

import asyncpg

from domain.ports.repositories import AbstractLeaderRepository


@dataclass(frozen=True, slots=True)
class LeaderTierRecord:
    wallet: str
    follow_tier: str
    tier_changed_at: float | None


@dataclass(frozen=True, slots=True)
class LeaderRosterRecord:
    wallet: str
    tier: str
    score: float
    base_score: float
    longitudinal_score: float | None
    cycles_in_tier: int
    promote_streak: int
    demote_streak: int
    tier_changed_at: float
    first_promoted_at: float | None
    last_seen_at: float | None
    config_hash: str


class LeadersRepo(AbstractLeaderRepository):
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def list_roster(self) -> list[LeaderRosterRecord]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT wallet, tier, score, base_score, longitudinal_score,
                       cycles_in_tier, promote_streak, demote_streak,
                       tier_changed_at, first_promoted_at, last_seen_at,
                       config_hash
                FROM leader_roster
                ORDER BY score DESC, wallet ASC
                """
            )
        return [_roster_record(row) for row in rows]

    async def get_roster(self, wallet: str) -> LeaderRosterRecord | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT wallet, tier, score, base_score, longitudinal_score,
                       cycles_in_tier, promote_streak, demote_streak,
                       tier_changed_at, first_promoted_at, last_seen_at,
                       config_hash
                FROM leader_roster
                WHERE wallet = $1
                """,
                wallet.lower(),
            )
        return _roster_record(row) if row is not None else None

    async def save_roster(self, records: list[LeaderRosterRecord]) -> None:
        if not records:
            return
        now = time.time()
        values = [
            (
                record.wallet.lower(),
                record.tier,
                float(record.score),
                float(record.base_score),
                record.longitudinal_score,
                int(record.cycles_in_tier),
                int(record.promote_streak),
                int(record.demote_streak),
                float(record.tier_changed_at),
                record.first_promoted_at,
                record.last_seen_at,
                record.config_hash,
                now,
            )
            for record in records
        ]
        async with self._pool.acquire() as connection:
            await connection.executemany(
                """
                INSERT INTO leader_roster (
                    wallet, tier, score, base_score, longitudinal_score,
                    cycles_in_tier, promote_streak, demote_streak,
                    tier_changed_at, first_promoted_at, last_seen_at,
                    config_hash, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                )
                ON CONFLICT (wallet) DO UPDATE SET
                    tier = excluded.tier,
                    score = excluded.score,
                    base_score = excluded.base_score,
                    longitudinal_score = excluded.longitudinal_score,
                    cycles_in_tier = excluded.cycles_in_tier,
                    promote_streak = excluded.promote_streak,
                    demote_streak = excluded.demote_streak,
                    tier_changed_at = excluded.tier_changed_at,
                    first_promoted_at = excluded.first_promoted_at,
                    last_seen_at = excluded.last_seen_at,
                    config_hash = excluded.config_hash,
                    updated_at = excluded.updated_at
                """,
                values,
            )

    async def list_active(
        self,
        limit: int | None = None,
        config_hash: str | None = None,
    ) -> list[str]:
        query = """
            SELECT wallet
            FROM leader_roster
            WHERE tier = 'active'
        """
        values: list[object] = []
        if config_hash is not None:
            values.append(config_hash)
            query += f" AND config_hash = ${len(values)}"
        query += " ORDER BY score DESC, wallet ASC"
        if limit is not None:
            values.append(max(0, int(limit)))
            query += f" LIMIT ${len(values)}"
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, *values)
        return [str(row["wallet"]) for row in rows]

    async def get_tier(self, wallet: str) -> LeaderTierRecord:
        record = await self.get_roster(wallet)
        if record is None:
            return LeaderTierRecord(
                wallet=wallet.lower(),
                follow_tier="watchlist",
                tier_changed_at=None,
            )
        return LeaderTierRecord(
            wallet=record.wallet,
            follow_tier="live_follow" if record.tier == "active" else "watchlist",
            tier_changed_at=record.tier_changed_at,
        )

def _roster_record(row: asyncpg.Record) -> LeaderRosterRecord:
    return LeaderRosterRecord(
        wallet=str(row["wallet"]),
        tier=str(row["tier"]),
        score=float(row["score"] or 0.0),
        base_score=float(row["base_score"] or 0.0),
        longitudinal_score=(
            float(row["longitudinal_score"])
            if row["longitudinal_score"] is not None
            else None
        ),
        cycles_in_tier=int(row["cycles_in_tier"] or 0),
        promote_streak=int(row["promote_streak"] or 0),
        demote_streak=int(row["demote_streak"] or 0),
        tier_changed_at=float(row["tier_changed_at"]),
        first_promoted_at=(
            float(row["first_promoted_at"])
            if row["first_promoted_at"] is not None
            else None
        ),
        last_seen_at=(
            float(row["last_seen_at"]) if row["last_seen_at"] is not None else None
        ),
        config_hash=str(row["config_hash"] or ""),
    )


__all__ = [
    "LeaderRosterRecord",
    "LeaderTierRecord",
    "LeadersRepo",
]
