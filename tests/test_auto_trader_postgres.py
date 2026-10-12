"""PostgreSQL roster concurrency, exact evidence, and durable job contracts."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import asyncpg
import pytest
from testcontainers.community.postgres import PostgresContainer

from pmcopy.domain.auto_trader import AutoRoster, AutoTraderScope, Trader
from pmcopy.storage.postgres.auto_trader_repository import PostgresAutoTraderRepository
from pmcopy.storage.postgres.migrations import apply_migrations


@pytest.mark.asyncio
async def test_roster_compare_and_swap_restart_scope_audit_and_discovery_lease():
    with PostgresContainer("postgres:16-alpine") as container:
        dsn = container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        try:
            async with pool.acquire() as connection:
                await apply_migrations(connection)
                await apply_migrations(connection)
            repo = PostgresAutoTraderRepository(pool)
            scope = AutoTraderScope("account-a", "live")
            wallet = "0x" + "a" * 40
            roster = AutoRoster(
                initialized=True, traders=(Trader(wallet, score=Decimal("0.123456789123456789")),)
            )
            results = await asyncio.gather(
                *(
                    repo.save(scope, 0, roster, event="created", receipt_key="command")
                    for _ in range(2)
                )
            )
            assert sorted(results) == [False, True]
            restored = await PostgresAutoTraderRepository(pool).load(scope)
            assert restored == replace(roster, revision=1)
            assert await repo.receipt(scope, "command") == restored
            assert await repo.load(AutoTraderScope("account-a", "paper")) == AutoRoster()
            assert await repo.load(AutoTraderScope("account-b", "live")) == AutoRoster()
            now = datetime(2026, 9, 16, tzinfo=UTC)
            assert await repo.claim_discovery(scope, "initialization", now)
            assert not await repo.claim_discovery(scope, "initialization", now)
            assert await repo.claim_discovery(scope, "initialization", now + timedelta(hours=2))
            await repo.finish_discovery(scope, "initialization", succeeded=True)
            assert not await repo.claim_discovery(scope, "initialization", now + timedelta(days=1))
            async with pool.acquire() as connection:
                assert await connection.fetchval("SELECT count(*) FROM auto_trader_events") == 1
        finally:
            await pool.close()


async def test_candidate_score_lookup_preserves_wallets_across_targeted_cycles():
    from pmcopy.storage.postgres.discovery_repo import DiscoveryRepo
    from pmcopy.services.discovery.models import Candidate

    with PostgresContainer("postgres:16-alpine") as container:
        pool = await asyncpg.create_pool(
            container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        )
        try:
            async with pool.acquire() as connection:
                await apply_migrations(connection)
            repo = DiscoveryRepo(pool)
            for wallet, score in [("wallet-a", "80"), ("wallet-b", "90")]:
                cycle = await repo.begin_cycle("config")
                await repo.persist_cycle(
                    cycle,
                    [Candidate(wallet=wallet, hscore={"h_score": score}, score=0.8)],
                    "config",
                )
                await repo.complete_cycle(
                    cycle, status="completed", candidate_count=1, kept_count=1
                )
            assert await repo.candidate_hscores(("wallet-a", "wallet-b"), config_hash="config") == {
                "wallet-a": Decimal("80"),
                "wallet-b": Decimal("90"),
            }
            assert await repo.candidate_hscores(("wallet-a",), config_hash="other") == {}
        finally:
            await pool.close()


async def test_exit_ownership_retains_inventory_pending_orders_and_paper_scope():
    from pmcopy.storage.postgres.auto_trader_ownership import PostgresAutoTraderOwnership
    from pmcopy.storage.postgres.paper_evaluation_repository import PostgresPaperLedgerRepository
    from pmcopy.domain.paper_evaluation import PaperLedger, PaperFill

    with PostgresContainer("postgres:16-alpine") as container:
        pool = await asyncpg.create_pool(
            container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        )
        try:
            async with pool.acquire() as connection:
                await apply_migrations(connection)
                await connection.execute(
                    "INSERT INTO orders (idempotency_key,user_id,leader_id,status) VALUES ('pending','follower','pending-leader','submitting'),('closed','follower','closed-leader','filled'),('other','other','other-leader','live')"
                )
                await connection.execute(
                    "INSERT INTO positions (user_id,condition_id,token_id,leader_id,size_tokens,avg_entry_price) VALUES ('follower','c','t','position-leader','2','.5')"
                )
            scope = AutoTraderScope("follower", "live")
            now = datetime(2026, 9, 16, tzinfo=UTC)
            ledger = PaperLedger.start(Decimal("100"), now).apply(
                token="t",
                condition="c",
                side="BUY",
                fill=PaperFill(Decimal("2"), Decimal("1")),
                event="buy",
            )
            await PostgresPaperLedgerRepository(pool).save(
                scope, "paper-leader", ledger, event="buy", now=now
            )
            reader = PostgresAutoTraderOwnership(pool)
            assert await reader.retained_wallets(scope) == {
                "pending-leader",
                "position-leader",
                "paper-leader",
            }
            assert await reader.retained_wallets(AutoTraderScope("follower", "paper")) == set()
        finally:
            await pool.close()


async def test_doctor_allows_initialization_before_new_roster_migration(monkeypatch):
    from unittest.mock import AsyncMock
    from pmcopy.bot_cli.commands.doctor import _automatic_rosters_available
    from tests.config_helpers import load_test_config
    import pmcopy.storage.postgres.connection as connection_module

    with PostgresContainer("postgres:16-alpine") as container:
        pool = await asyncpg.create_pool(
            container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        )
        try:
            async with pool.acquire() as connection:
                await apply_migrations(connection)
                await connection.execute("DROP TABLE auto_trader_rosters")
            monkeypatch.setattr(connection_module, "init_pool", AsyncMock(return_value=pool))
            monkeypatch.setattr(connection_module, "shutdown_pool", AsyncMock())
            assert not await _automatic_rosters_available(load_test_config())
            async with pool.acquire() as connection:
                assert (
                    await connection.fetchval("SELECT to_regclass('public.auto_trader_rosters')")
                    is None
                )
        finally:
            await pool.close()
