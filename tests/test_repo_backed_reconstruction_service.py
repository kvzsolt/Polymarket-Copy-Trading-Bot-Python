from __future__ import annotations

import contextlib
from decimal import Decimal

import pytest

from domain.discovery_models import (
    ObservedTrade,
    PositionSnapshotRecord,
    ReconstructionConfidence,
    ReconstructedPositionStatus,
)
from services.discovery.reconstructed_positions_service import (
    ReconstructedPositionsService,
)
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


async def test_repo_backed_reconstruction_rebuilds_position_from_stored_trades():
    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        await repo.insert_trade(
            _trade(side="BUY", size="10", price="0.40", event_ts=100, tx_hash="0x1")
        )
        await repo.insert_trade(
            _trade(side="SELL", size="4", price="0.70", event_ts=200, tx_hash="0x2")
        )

        service = ReconstructedPositionsService(repo)
        results = await service.reconstruct_for_trader("0xabc")

    key = ("0xabc", "cond-1", "token-yes")
    assert key in results
    assert results[key].current_size == Decimal("6")
    assert results[key].avg_cost == Decimal("0.40")
    assert results[key].realized_pnl == Decimal("1.20")
    assert results[key].status == ReconstructedPositionStatus.OPEN
    assert results[key].confidence == ReconstructionConfidence.HIGH


async def test_repo_backed_reconstruction_uses_latest_snapshot_from_storage():
    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        await repo.insert_trade(
            _trade(side="BUY", size="10", price="0.40", event_ts=100, tx_hash="0x1")
        )
        await repo.insert_position_snapshot(
            _snapshot(size="10", avg_price="0.40", snapshot_ts=150)
        )
        await repo.insert_position_snapshot(
            _snapshot(size="10", avg_price="0.46", snapshot_ts=300)
        )

        service = ReconstructedPositionsService(repo)
        results = await service.reconstruct_for_trader("0xabc")

    key = ("0xabc", "cond-1", "token-yes")
    assert results[key].confidence == ReconstructionConfidence.MEDIUM
    assert len(results[key].mismatches) == 1
    assert results[key].mismatches[0].reported_avg_price == Decimal("0.46")


async def test_repo_backed_reconstruction_includes_snapshot_only_keys():
    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        await repo.insert_trade(
            _trade(side="BUY", size="2", price="0.30", event_ts=100, tx_hash="0x1")
        )
        await repo.insert_position_snapshot(
            _snapshot(
                condition_id="cond-2",
                token_id="token-no",
                size="4",
                avg_price="0.55",
                snapshot_ts=300,
            )
        )

        service = ReconstructedPositionsService(repo)
        results = await service.reconstruct_for_trader("0xabc")

    assert ("0xabc", "cond-1", "token-yes") in results
    assert ("0xabc", "cond-2", "token-no") in results
    assert results[("0xabc", "cond-2", "token-no")].status == ReconstructedPositionStatus.UNKNOWN
    assert results[("0xabc", "cond-2", "token-no")].confidence == ReconstructionConfidence.LOW


async def test_repo_backed_reconstruction_respects_token_and_since_filters():
    async with _pg_pool() as pool:
        repo = DiscoveryRepo(pool)
        await repo.insert_trade(
            _trade(
                token_id="token-yes",
                side="BUY",
                size="5",
                price="0.40",
                event_ts=100,
                tx_hash="0x1",
            )
        )
        await repo.insert_trade(
            _trade(
                token_id="token-no",
                side="BUY",
                size="7",
                price="0.25",
                event_ts=200,
                tx_hash="0x2",
            )
        )
        await repo.insert_position_snapshot(
            _snapshot(
                token_id="token-no",
                size="7",
                avg_price="0.25",
                snapshot_ts=250,
            )
        )

        service = ReconstructedPositionsService(repo)
        results = await service.reconstruct_for_trader(
            "0xabc", token_id="token-no", since_ts=150
        )

    assert ("0xabc", "cond-1", "token-no") in results
    assert ("0xabc", "cond-1", "token-yes") not in results
    assert results[("0xabc", "cond-1", "token-no")].current_size == Decimal("7")
    assert results[("0xabc", "cond-1", "token-no")].confidence == ReconstructionConfidence.HIGH
