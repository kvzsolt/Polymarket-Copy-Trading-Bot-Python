"""Real database performance excludes paper sources and advances only on closures."""

from datetime import UTC, datetime
from decimal import Decimal as D

import asyncpg
from tests.integration_postgres import PostgresContainer

from pmcopy.domain.auto_trader import AutoTraderScope
from pmcopy.storage.postgres.auto_trader_performance import PostgresAutoTraderPerformance
from pmcopy.storage.postgres.fills_repo import FillRecord, FillsRepo
from pmcopy.storage.postgres.migrations import apply_migrations


async def test_live_performance_is_scoped_and_excludes_paper_fills():
    with PostgresContainer("postgres:16-alpine") as container:
        pool = await asyncpg.create_pool(
            container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        )
        try:
            async with pool.acquire() as connection:
                await apply_migrations(connection)
            wallet = "0x" + "a" * 40
            fills = FillsRepo(pool)
            for account, prefix, price in [
                ("follower", "live", ".4"),
                ("other", "live", ".9"),
                ("follower", "paper", ".9"),
            ]:
                for side, value in [("BUY", ".5"), ("SELL", price)]:
                    await fills.append_fill(
                        FillRecord(
                            account,
                            wallet,
                            prefix + "-" + side,
                            "c",
                            prefix,
                            side,
                            D("10"),
                            D(value),
                            fee=D(".01"),
                            fill_ts=100,
                        )
                    )
            reader = PostgresAutoTraderPerformance(pool)
            result = await reader.read(
                AutoTraderScope("follower", "live"), wallet, datetime(2026, 9, 16, tzinfo=UTC)
            )
            assert result.closed_episodes == 1
            assert result.realized_pnl == D("-1.02")
            assert result.recent_pnl == D("-1.02")
            assert result.complete
            assert result.drawdown is None
            watermark = result.watermark
            await fills.append_fill(
                FillRecord(
                    "follower",
                    wallet,
                    "next-buy",
                    "c",
                    "next",
                    "BUY",
                    D("1"),
                    D(".5"),
                    fee=D("0"),
                    fill_ts=101,
                )
            )
            result = await reader.read(
                AutoTraderScope("follower", "live"), wallet, datetime(2026, 9, 17, tzinfo=UTC)
            )
            assert result.watermark == watermark
        finally:
            await pool.close()
