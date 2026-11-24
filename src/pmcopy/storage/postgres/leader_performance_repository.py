"""Read attributed episode evidence from the existing durable fill ledger."""

import asyncpg

from pmcopy.domain.detection import TradeSide
from pmcopy.services.trading.leader_performance import (
    ClosedEpisodeAccumulator,
    ClosedEpisodeStats,
    FollowerLeaderKey,
    PerformanceFill,
)


class PostgresLeaderPerformanceReader:
    """Rebuild scoped episodes in bounded pages without venue or analytics I/O."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Use the application's canonical PostgreSQL pool."""
        self._pool = pool

    async def get_closed_episode_stats(self, key: FollowerLeaderKey) -> ClosedEpisodeStats:
        """Read confirmed fills up to a stable ledger watermark for this scope.

        Missing task attribution remains its own scope. Orders without fills
        never count as performance. Database failures propagate to the caller.
        """
        accumulator = ClosedEpisodeAccumulator()
        async with self._pool.acquire() as connection:
            upper = await connection.fetchval("SELECT COALESCE(MAX(id), 0) FROM fills")
        cursor = 0
        while cursor < upper:
            async with self._pool.acquire() as connection:
                rows = await connection.fetch(
                    """
                    SELECT f.id, f.token_id, f.side, f.size_tokens, f.price,
                           CASE WHEN f.fee_evidence_known THEN f.fee ELSE NULL END AS fee,
                           o.request_json #>> '{recovery_metadata,task_id}' AS task_id
                    FROM fills f
                    LEFT JOIN orders o ON o.order_id = f.order_id AND o.user_id = f.user_id
                    WHERE f.user_id = $1 AND LOWER(f.leader_id) = $2
                      AND ($3::text IS NULL OR
                           o.request_json #>> '{recovery_metadata,task_id}' = $3)
                      AND f.id > $4 AND f.id <= $5
                    ORDER BY f.id
                    LIMIT 1000
                    """,
                    key.follower_account,
                    key.leader_wallet,
                    key.task_id,
                    cursor,
                    upper,
                )
            if not rows:
                break
            for row in rows:
                accumulator.apply(
                    PerformanceFill(
                        fill_id=str(row["id"]),
                        token_id=row["token_id"],
                        task_id=row["task_id"],
                        side=TradeSide(row["side"]),
                        shares=row["size_tokens"],
                        price=row["price"],
                        fee=row["fee"],
                    )
                )
            cursor = rows[-1]["id"]
        return accumulator.stats
