from __future__ import annotations

from decimal import Decimal

from domain.discovery_models import (
    ObservedTrade,
    PositionSnapshotRecord,
    ReconstructionConfidence,
    ReconstructionMismatchType,
    ReconstructedPositionStatus,
)
from services.discovery.reconstruction_service import ReconstructionService


def _trade(
    *,
    side: str,
    size: str,
    price: str,
    event_ts: int,
    tx_hash: str,
    trade_id: str = "",
) -> ObservedTrade:
    return ObservedTrade(
        trader_wallet="0xabc",
        condition_id="cond-1",
        token_id="token-yes",
        side=side,
        size=Decimal(size),
        price=Decimal(price),
        usdc_size=Decimal(size) * Decimal(price),
        tx_hash=tx_hash,
        event_ts=event_ts,
        trade_id=trade_id,
    )


def _snapshot(*, size: str, avg_price: str) -> PositionSnapshotRecord:
    return PositionSnapshotRecord(
        trader_wallet="0xabc",
        condition_id="cond-1",
        token_id="token-yes",
        size=Decimal(size),
        avg_price=Decimal(avg_price),
        snapshot_ts=1712301000,
    )


def test_reconstruct_buy_then_partial_sell_keeps_open_inventory_with_high_confidence():
    service = ReconstructionService()

    position = service.reconstruct(
        [
            _trade(side="BUY", size="10", price="0.40", event_ts=100, tx_hash="0x1"),
            _trade(side="SELL", size="4", price="0.70", event_ts=200, tx_hash="0x2"),
        ]
    )

    assert position.current_size == Decimal("6")
    assert position.avg_cost == Decimal("0.40")
    assert position.realized_pnl == Decimal("1.20")
    assert position.status == ReconstructedPositionStatus.OPEN
    assert position.confidence == ReconstructionConfidence.HIGH
    assert position.mismatches == []


def test_reconstruct_full_close_marks_position_closed():
    service = ReconstructionService()

    position = service.reconstruct(
        [
            _trade(side="BUY", size="10", price="0.40", event_ts=100, tx_hash="0x1"),
            _trade(side="SELL", size="10", price="0.55", event_ts=200, tx_hash="0x2"),
        ]
    )

    assert position.current_size == Decimal("0")
    assert position.avg_cost == Decimal("0")
    assert position.realized_pnl == Decimal("1.50")
    assert position.status == ReconstructedPositionStatus.CLOSED
    assert position.confidence == ReconstructionConfidence.HIGH
    assert position.mismatches == []


def test_reconstruct_oversell_downgrades_confidence_without_fabricating_inventory():
    service = ReconstructionService()

    position = service.reconstruct(
        [
            _trade(side="BUY", size="5", price="0.40", event_ts=100, tx_hash="0x1"),
            _trade(side="SELL", size="8", price="0.60", event_ts=200, tx_hash="0x2"),
        ]
    )

    assert position.current_size == Decimal("0")
    assert position.realized_pnl == Decimal("1.00")
    assert position.status == ReconstructedPositionStatus.UNKNOWN
    assert position.confidence == ReconstructionConfidence.LOW
    assert len(position.mismatches) == 1
    assert position.mismatches[0].mismatch_type == ReconstructionMismatchType.OVERSOLD
    assert position.mismatches[0].quantity == Decimal("3")


def test_reconstruct_snapshot_gap_marks_missing_history():
    service = ReconstructionService()

    position = service.reconstruct(
        [_trade(side="BUY", size="5", price="0.40", event_ts=100, tx_hash="0x1")],
        snapshot=_snapshot(size="9", avg_price="0.40"),
    )

    assert position.current_size == Decimal("5")
    assert position.status == ReconstructedPositionStatus.UNKNOWN
    assert position.confidence == ReconstructionConfidence.LOW
    assert len(position.mismatches) == 1
    assert position.mismatches[0].mismatch_type == ReconstructionMismatchType.MISSING_HISTORY
    assert position.mismatches[0].reported_size == Decimal("9")
    assert position.mismatches[0].reconstructed_size == Decimal("5")


def test_reconstruct_avg_cost_mismatch_downgrades_confidence_but_keeps_position_open():
    service = ReconstructionService(price_tolerance=Decimal("0.01"))

    position = service.reconstruct(
        [_trade(side="BUY", size="10", price="0.40", event_ts=100, tx_hash="0x1")],
        snapshot=_snapshot(size="10", avg_price="0.46"),
    )

    assert position.current_size == Decimal("10")
    assert position.status == ReconstructedPositionStatus.OPEN
    assert position.confidence == ReconstructionConfidence.MEDIUM
    assert len(position.mismatches) == 1
    assert position.mismatches[0].mismatch_type == ReconstructionMismatchType.AVG_COST_MISMATCH
    assert position.mismatches[0].reconstructed_avg_cost == Decimal("0.40")
    assert position.mismatches[0].reported_avg_price == Decimal("0.46")


def test_reconstruct_snapshot_only_position_marks_unknown_without_inventing_lots():
    service = ReconstructionService()

    position = service.reconstruct([], snapshot=_snapshot(size="3", avg_price="0.44"))

    assert position.current_size == Decimal("0")
    assert position.status == ReconstructedPositionStatus.UNKNOWN
    assert position.confidence == ReconstructionConfidence.LOW
    assert len(position.mismatches) == 1
    assert position.mismatches[0].mismatch_type == ReconstructionMismatchType.MISSING_HISTORY
    assert position.mismatches[0].reported_size == Decimal("3")
    assert position.mismatches[0].reconstructed_size == Decimal("0")



def test_reconstruct_many_includes_snapshot_only_keys():
    service = ReconstructionService()

    results = service.reconstruct_many(
        [
            _trade(side="BUY", size="2", price="0.30", event_ts=100, tx_hash="0x1"),
        ],
        snapshots_by_key={
            ("0xabc", "cond-2", "token-no"): PositionSnapshotRecord(
                trader_wallet="0xabc",
                condition_id="cond-2",
                token_id="token-no",
                size=Decimal("4"),
                avg_price=Decimal("0.55"),
                snapshot_ts=1712301000,
            )
        },
    )

    assert ("0xabc", "cond-1", "token-yes") in results
    assert ("0xabc", "cond-2", "token-no") in results
    assert results[("0xabc", "cond-2", "token-no")].status == ReconstructedPositionStatus.UNKNOWN
    assert results[("0xabc", "cond-2", "token-no")].confidence == ReconstructionConfidence.LOW
