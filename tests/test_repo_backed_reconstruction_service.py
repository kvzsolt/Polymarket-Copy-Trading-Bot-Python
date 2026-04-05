from __future__ import annotations

from decimal import Decimal

from domain.discovery_models import (
    ObservedTrade,
    PositionSnapshotRecord,
    ReconstructionConfidence,
    ReconstructedPositionStatus,
)
from services.discovery.reconstructed_positions_service import (
    ReconstructedPositionsService,
)
from storage.sqlite.connection import create_connection
from storage.sqlite.discovery_repo import DiscoveryRepo
from storage.sqlite.migrations import apply_migrations


def _init_db(path) -> None:
    with create_connection(str(path)) as conn:
        apply_migrations(conn)
        conn.commit()


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


def test_repo_backed_reconstruction_rebuilds_position_from_stored_trades(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    repo = DiscoveryRepo(str(db_path))
    repo.insert_trade(
        _trade(side="BUY", size="10", price="0.40", event_ts=100, tx_hash="0x1")
    )
    repo.insert_trade(
        _trade(side="SELL", size="4", price="0.70", event_ts=200, tx_hash="0x2")
    )

    service = ReconstructedPositionsService(repo)
    results = service.reconstruct_for_trader("0xabc")

    key = ("0xabc", "cond-1", "token-yes")
    assert key in results
    assert results[key].current_size == Decimal("6")
    assert results[key].avg_cost == Decimal("0.40")
    assert results[key].realized_pnl == Decimal("1.20")
    assert results[key].status == ReconstructedPositionStatus.OPEN
    assert results[key].confidence == ReconstructionConfidence.HIGH


def test_repo_backed_reconstruction_uses_latest_snapshot_from_storage(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    repo = DiscoveryRepo(str(db_path))
    repo.insert_trade(
        _trade(side="BUY", size="10", price="0.40", event_ts=100, tx_hash="0x1")
    )
    repo.insert_position_snapshot(
        _snapshot(size="10", avg_price="0.40", snapshot_ts=150)
    )
    repo.insert_position_snapshot(
        _snapshot(size="10", avg_price="0.46", snapshot_ts=300)
    )

    service = ReconstructedPositionsService(repo)
    results = service.reconstruct_for_trader("0xabc")

    key = ("0xabc", "cond-1", "token-yes")
    assert results[key].confidence == ReconstructionConfidence.MEDIUM
    assert len(results[key].mismatches) == 1
    assert results[key].mismatches[0].reported_avg_price == Decimal("0.46")


def test_repo_backed_reconstruction_includes_snapshot_only_keys(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    repo = DiscoveryRepo(str(db_path))
    repo.insert_trade(
        _trade(side="BUY", size="2", price="0.30", event_ts=100, tx_hash="0x1")
    )
    repo.insert_position_snapshot(
        _snapshot(
            condition_id="cond-2",
            token_id="token-no",
            size="4",
            avg_price="0.55",
            snapshot_ts=300,
        )
    )

    service = ReconstructedPositionsService(repo)
    results = service.reconstruct_for_trader("0xabc")

    assert ("0xabc", "cond-1", "token-yes") in results
    assert ("0xabc", "cond-2", "token-no") in results
    assert results[("0xabc", "cond-2", "token-no")].status == ReconstructedPositionStatus.UNKNOWN
    assert results[("0xabc", "cond-2", "token-no")].confidence == ReconstructionConfidence.LOW


def test_repo_backed_reconstruction_respects_token_and_since_filters(tmp_path):
    db_path = tmp_path / "bot_state.db"
    _init_db(db_path)

    repo = DiscoveryRepo(str(db_path))
    repo.insert_trade(
        _trade(
            token_id="token-yes",
            side="BUY",
            size="5",
            price="0.40",
            event_ts=100,
            tx_hash="0x1",
        )
    )
    repo.insert_trade(
        _trade(
            token_id="token-no",
            side="BUY",
            size="7",
            price="0.25",
            event_ts=200,
            tx_hash="0x2",
        )
    )
    repo.insert_position_snapshot(
        _snapshot(
            token_id="token-no",
            size="7",
            avg_price="0.25",
            snapshot_ts=250,
        )
    )

    service = ReconstructedPositionsService(repo)
    results = service.reconstruct_for_trader("0xabc", token_id="token-no", since_ts=150)

    assert ("0xabc", "cond-1", "token-no") in results
    assert ("0xabc", "cond-1", "token-yes") not in results
    assert results[("0xabc", "cond-1", "token-no")].current_size == Decimal("7")
    assert results[("0xabc", "cond-1", "token-no")].confidence == ReconstructionConfidence.HIGH
