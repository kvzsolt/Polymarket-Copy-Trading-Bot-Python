from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from domain.discovery_models import MarketMeta, ObservedTrade, PositionSnapshotRecord
from services.discovery.scoring_input_service import ScoringInputAssemblerService
from storage.postgres.discovery_repo import DiscoveryRepo

pytestmark = pytest.mark.asyncio


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply

        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


def _trade(
    *,
    trader_wallet: str = "0xabc",
    condition_id: str = "cond-1",
    token_id: str = "token-yes",
    side: str,
    size: str,
    price: str,
    event_ts: int,
    tx_hash: str,
    trade_id: str = "",
) -> ObservedTrade:
    return ObservedTrade(
        trader_wallet=trader_wallet,
        condition_id=condition_id,
        token_id=token_id,
        side=side,
        size=Decimal(size),
        price=Decimal(price),
        usdc_size=Decimal(size) * Decimal(price),
        tx_hash=tx_hash,
        event_ts=event_ts,
        trade_id=trade_id,
    )


def _snapshot(
    *,
    trader_wallet: str = "0xabc",
    condition_id: str = "cond-1",
    token_id: str = "token-yes",
    size: str,
    avg_price: str,
    snapshot_ts: int,
) -> PositionSnapshotRecord:
    return PositionSnapshotRecord(
        trader_wallet=trader_wallet,
        condition_id=condition_id,
        token_id=token_id,
        size=Decimal(size),
        avg_price=Decimal(avg_price),
        snapshot_ts=snapshot_ts,
    )


async def test_scoring_input_assembles_stable_record_from_reconstructed_position():
    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        await repo.insert_trade(
            _trade(side="BUY", size="10", price="0.40", event_ts=100, tx_hash="0x1")
        )
        await repo.insert_trade(
            _trade(side="SELL", size="4", price="0.70", event_ts=200, tx_hash="0x2")
        )

        service = ScoringInputAssemblerService(repo)
        records = await service.assemble_for_trader("0xabc")

    key = ("0xabc", "cond-1", "token-yes")
    assert key in records

    record = records[key]
    assert record.key == key
    assert record.current_size == Decimal("6")
    assert record.avg_cost == Decimal("0.40")
    assert record.current_cost_basis == Decimal("2.40")
    assert record.realized_pnl == Decimal("1.20")
    assert record.observed_trade_count == 2
    assert record.position_status.value == "open"
    assert record.reconstruction_confidence.value == "high"
    assert record.mismatch_types == ()
    assert record.evidence.last_observed_trade_ts == 200
    assert record.evidence.latest_snapshot_ts is None
    assert record.market_context.has_market_meta is False
    assert record.market_context.buckets == ()


async def test_scoring_input_attaches_market_context_and_buckets():
    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        await repo.insert_trade(
            _trade(side="BUY", size="5", price="0.35", event_ts=100, tx_hash="0x1")
        )
        await repo.upsert_market(
            MarketMeta(
                condition_id="cond-1",
                question="Will it rain tomorrow?",
                slug="rain-tomorrow",
                event_slug="weather",
                category="Weather",
                end_date_ts=100 + (3 * 86400),
                active=True,
                resolved=False,
                neg_risk=True,
                yes_token_id="token-yes",
                no_token_id="token-no",
            )
        )

        service = ScoringInputAssemblerService(repo)
        records = await service.assemble_for_trader("0xabc")
    record = records[("0xabc", "cond-1", "token-yes")]

    assert record.market_context.has_market_meta is True
    assert record.market_context.question == "Will it rain tomorrow?"
    assert record.market_context.market_slug == "rain-tomorrow"
    assert record.market_context.event_slug == "weather"
    assert record.market_context.category == "Weather"
    assert record.market_context.active is True
    assert record.market_context.resolved is False
    assert record.market_context.neg_risk is True
    assert record.market_context.reference_ts == 100

    buckets = {bucket.dimension: bucket.bucket for bucket in record.market_context.buckets}
    assert buckets == {
        "category": "weather",
        "days_to_resolution": "short",
    }


async def test_scoring_input_keeps_low_confidence_state_honest_when_history_is_missing():
    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        await repo.insert_position_snapshot(
            _snapshot(size="3", avg_price="0.44", snapshot_ts=300)
        )

        service = ScoringInputAssemblerService(repo)
        records = await service.assemble_for_trader("0xabc")
    record = records[("0xabc", "cond-1", "token-yes")]

    assert record.current_size == Decimal("0")
    assert record.avg_cost == Decimal("0")
    assert record.position_status.value == "unknown"
    assert record.reconstruction_confidence.value == "low"
    assert record.mismatch_types == ("missing_history",)
    assert record.evidence.has_snapshot is True
    assert record.evidence.snapshot_size == Decimal("3")
    assert record.evidence.snapshot_avg_price == Decimal("0.44")
    assert record.evidence.last_observed_trade_ts is None


async def test_scoring_input_uses_newer_snapshot_ts_as_reference_ts():
    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        await repo.insert_trade(
            _trade(side="BUY", size="5", price="0.35", event_ts=100, tx_hash="0x1")
        )
        await repo.insert_position_snapshot(
            _snapshot(size="5", avg_price="0.35", snapshot_ts=250)
        )
        await repo.upsert_market(
            MarketMeta(
                condition_id="cond-1",
                question="Will it rain tomorrow?",
                slug="rain-tomorrow",
                event_slug="weather",
                category="Weather",
                end_date_ts=250 + (3 * 86400),
                active=True,
                resolved=False,
                neg_risk=False,
                yes_token_id="token-yes",
                no_token_id="token-no",
            )
        )

        service = ScoringInputAssemblerService(repo)
        records = await service.assemble_for_trader("0xabc")
    record = records[("0xabc", "cond-1", "token-yes")]

    assert record.evidence.last_observed_trade_ts == 100
    assert record.evidence.latest_snapshot_ts == 250
    assert record.market_context.reference_ts == 250
    buckets = {bucket.dimension: bucket.bucket for bucket in record.market_context.buckets}
    assert buckets["days_to_resolution"] == "short"
