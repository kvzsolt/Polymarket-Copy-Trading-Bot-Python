"""Account-wide task market capacity evaluated under the existing BUY lock."""

from decimal import Decimal

import asyncpg

from domain.order_lifecycle import OrderIntent


class PostgresTaskBuyCapacity:
    """Include held inventory and all pending task allocations in configured caps."""

    @staticmethod
    async def permits(
        connection: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
        intent: OrderIntent,
        collateral: Decimal,
    ) -> bool:
        """Read capacity within the caller's short account reservation transaction."""
        limit = intent.max_market_notional
        if limit is None:
            return True
        if not limit.is_finite() or limit <= 0:
            return False
        rows = await connection.fetch(
            "SELECT condition_id, size_tokens::numeric AS size_tokens, "
            "avg_entry_price::numeric AS avg_entry_price FROM positions "
            "WHERE user_id = $1 AND size_tokens::numeric > 0",
            intent.key.user_id,
        )
        pending = await connection.fetch(
            "SELECT condition_id, reserved_usdc::numeric AS reserved_usdc FROM order_reservations "
            "WHERE user_id = $1 AND side = 'BUY' AND status = 'active'",
            intent.key.user_id,
        )
        markets = {row["condition_id"] for row in (*rows, *pending)}
        if (
            intent.max_open_markets > 0
            and intent.key.condition_id not in markets
            and len(markets) >= intent.max_open_markets
        ):
            return False
        held = Decimal("0")
        for row in rows:
            if row["condition_id"] != intent.key.condition_id:
                continue
            basis = row["avg_entry_price"]
            if basis is None or not basis.is_finite() or basis <= 0:
                return False
            held += row["size_tokens"] * basis
        reserved = sum(
            (
                row["reserved_usdc"]
                for row in pending
                if row["condition_id"] == intent.key.condition_id
            ),
            Decimal("0"),
        )
        return held + reserved + collateral <= limit
