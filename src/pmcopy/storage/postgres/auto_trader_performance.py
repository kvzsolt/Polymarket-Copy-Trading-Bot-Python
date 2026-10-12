"""Source-isolated recent live-copy results from canonical attributed fills."""

from datetime import UTC, datetime
from decimal import Decimal

import asyncpg

from pmcopy.domain.auto_trader import AutoTraderScope, CopyPerformance
from pmcopy.domain.detection import TradeSide
from pmcopy.services.auto_trader_performance import RecentEpisodeAccumulator
from pmcopy.services.leader_performance import PerformanceFill


class PostgresAutoTraderPerformance:
    """Read bounded stable fill pages without exposing paper profits as live results."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Inject the canonical fill database."""
        self._pool = pool

    async def read(self, scope: AutoTraderScope, wallet: str, now: datetime) -> CopyPerformance:
        """Rebuild exact flat-to-flat episodes within one follower and leader scope."""
        if scope.trading_mode != "live":
            raise ValueError("live performance requires a live account scope")
        async with self._pool.acquire() as connection:
            upper = await connection.fetchval(
                "SELECT COALESCE(MAX(id),0) FROM fills WHERE user_id=$1 AND LOWER(leader_id)=$2",
                scope.follower_account,
                wallet,
            )
        accumulator = RecentEpisodeAccumulator()
        cursor = 0
        started = now
        complete = True
        for _ in range(100):
            async with self._pool.acquire() as connection:
                rows = await connection.fetch(
                    """
                    SELECT f.id,f.token_id,f.side,f.size_tokens,f.price,f.fill_ts,
                           CASE WHEN f.fee_evidence_known THEN f.fee ELSE NULL END AS fee,
                           o.request_json #>> '{recovery_metadata,task_id}' AS task_id
                    FROM fills f LEFT JOIN orders o ON o.order_id=f.order_id AND o.user_id=f.user_id
                    WHERE f.user_id=$1 AND LOWER(f.leader_id)=$2 AND f.id>$3 AND f.id<=$4
                      AND COALESCE(f.order_id,'') NOT LIKE 'paper-%'
                      AND COALESCE(f.fill_json->>'paper','false') <> 'true'
                      AND COALESCE(f.fill_json #>> '{raw,paper}','false') <> 'true'
                    ORDER BY f.id LIMIT 1000
                    """,
                    scope.follower_account,
                    wallet,
                    cursor,
                    upper,
                )
            if not rows:
                break
            for row in rows:
                accumulator.apply(
                    PerformanceFill(
                        str(row["id"]),
                        row["token_id"],
                        row["task_id"],
                        TradeSide(row["side"]),
                        Decimal(str(row["size_tokens"])),
                        Decimal(str(row["price"])),
                        None if row["fee"] is None else Decimal(str(row["fee"])),
                    )
                )
                if row["fill_ts"] is not None:
                    started = min(started, datetime.fromtimestamp(row["fill_ts"], UTC))
            cursor = rows[-1]["id"]
            if len(rows) < 1000:
                break
        else:
            complete = False
        stats = accumulator.stats
        return CopyPerformance(
            f"live:{upper}:{now.isoformat()}",
            now,
            started,
            stats.closed_episodes,
            stats.realized_pnl,
            sum(accumulator.recent_pnls, Decimal(0)),
            None,
            "live:" + accumulator.closed_watermark,
            complete and stats.excluded_scopes == 0,
        )
