"""Read exit ownership before retiring an automatic trader's signal monitor."""

import asyncpg

from domain.auto_trader import AutoTraderScope


class PostgresAutoTraderOwnership:
    """Retain every wallet with inventory or potentially unfinished execution."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        """Inject the existing database pool."""
        self._pool = pool

    async def retained_wallets(self, scope: AutoTraderScope) -> set[str]:
        """Return scoped owners in one snapshot; query failures must propagate.

        Unknown order states retain monitoring. Only explicitly terminal rows
        allow retirement, so a pending POST or task reservation cannot lose its
        subsequent leader exit signal.
        """
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT leader_wallet AS wallet FROM auto_trader_paper_portfolios
                WHERE follower_account=$1 AND trading_mode=$2
                  AND jsonb_array_length(snapshot->'positions') > 0
                UNION
                SELECT leader_id FROM positions
                WHERE $2='live' AND user_id=$1 AND size_tokens::numeric > 0
                UNION
                SELECT leader_id FROM orders
                WHERE $2='live' AND user_id=$1 AND status NOT IN (
                    'filled','cancelled','canceled','rejected','terminal_failed',
                    'submission_failed','duplicate_ignored','cross_source_claimed')
                UNION
                SELECT leader_wallet FROM copy_task_risk_lots
                WHERE $2='live' AND follower_account=$1 AND status='open'
                UNION
                SELECT task.leader_wallet FROM copy_task_budget_reservations AS reservation
                JOIN copy_tasks AS task USING (task_id)
                WHERE $2='live' AND reservation.follower_account=$1
                  AND reservation.status='reserved'
                """,
                scope.follower_account,
                scope.trading_mode,
            )
        return {row["wallet"].lower() for row in rows if row["wallet"]}
