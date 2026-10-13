"""Durable paper ledger recovery and event deduplication."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal as D

import asyncpg
from tests.integration_postgres import PostgresContainer

from pmcopy.domain.auto_trader import AutoTraderScope
from pmcopy.domain.paper_evaluation import PaperFill, PaperLedger
from pmcopy.storage.postgres.migrations import apply_migrations
from pmcopy.storage.postgres.paper_evaluation_repository import (
    PaperLedgerCodec,
    PostgresPaperLedgerRepository,
)


def test_exact_ledger_codec_preserves_open_episode_and_costs():
    ledger = PaperLedger.start(D("100.123456789"), datetime(2026, 9, 16, tzinfo=UTC))
    ledger = ledger.apply(
        token="t",
        condition="c",
        side="BUY",
        fill=PaperFill(D("2"), D(".99"), D(".012345")),
        event="entry",
    )
    assert PaperLedgerCodec.decode(PaperLedgerCodec.encode(ledger)) == ledger


async def test_atomic_paper_event_dedup_restart_and_scope_isolation():
    with PostgresContainer("postgres:16-alpine") as container:
        pool = await asyncpg.create_pool(
            container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        )
        try:
            async with pool.acquire() as connection:
                await apply_migrations(connection)
            scope = AutoTraderScope("follower", "live")
            repository = PostgresPaperLedgerRepository(pool)
            now = datetime(2026, 9, 16, tzinfo=UTC)
            initial = PaperLedger.start(D("100"), now)
            assert await repository.load(scope, "leader") is None
            assert await repository.save(scope, "leader", initial, event="seed", now=now)
            restored = await repository.load(scope, "leader")
            assert restored == replace(initial, revision=1)
            updated = restored.apply(
                token="t",
                condition="c",
                side="BUY",
                fill=PaperFill(D("2"), D("1"), D(".01")),
                event="buy",
            )
            results = await asyncio.gather(
                *(repository.save(scope, "leader", updated, event="buy", now=now) for _ in range(2))
            )
            assert sorted(results) == [False, True]
            assert await repository.seen(scope, "leader", "buy")
            recovered = await PostgresPaperLedgerRepository(pool).load(scope, "leader")
            assert recovered.cash == D("98.99")
            assert recovered.positions[0].shares == D("2")
            assert await repository.load(AutoTraderScope("follower", "paper"), "leader") is None
        finally:
            await pool.close()


async def test_paper_source_claim_is_durable_atomic_and_scoped():
    with PostgresContainer("postgres:16-alpine") as container:
        pool = await asyncpg.create_pool(
            container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        )
        try:
            async with pool.acquire() as connection:
                await apply_migrations(connection)
            repo = PostgresPaperLedgerRepository(pool)
            scope = AutoTraderScope("follower", "live")
            now = datetime(2026, 9, 16, tzinfo=UTC)
            results = await asyncio.gather(
                *(
                    repo.claim_source(scope, "leader", "tx", source=source, now=now)
                    for source in ("on_chain", "polling")
                )
            )
            assert sorted(results) == [False, True]
            winner = ("on_chain", "polling")[results.index(True)]
            restored = PostgresPaperLedgerRepository(pool)
            assert await restored.claim_source(scope, "leader", "tx", source=winner, now=now)
            assert await restored.claim_source(
                AutoTraderScope("follower", "paper"), "leader", "tx", source="polling", now=now
            )
        finally:
            await pool.close()
