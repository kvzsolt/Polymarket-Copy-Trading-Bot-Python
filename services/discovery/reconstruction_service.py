"""Phase 2 — Honest inventory reconstruction.

Rebuilds inventory only from observed leader trades and optional reported
position snapshots. The service never fabricates bankroll, backfills missing
lots, or treats mismatches as resolved state.
"""
from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Mapping, Sequence

from domain.discovery_models import (
    InventoryLot,
    ObservedTrade,
    PositionSnapshotRecord,
    ReconstructionConfidence,
    ReconstructionMismatch,
    ReconstructionMismatchType,
    ReconstructedPosition,
    ReconstructedPositionStatus,
)

PositionKey = tuple[str, str, str]


class ReconstructionService:
    """Rebuild per-position inventory from observed trades."""

    _CONFIDENCE_ORDER = {
        ReconstructionConfidence.HIGH: 2,
        ReconstructionConfidence.MEDIUM: 1,
        ReconstructionConfidence.LOW: 0,
    }

    def __init__(
        self,
        *,
        size_tolerance: Decimal = Decimal("0.01"),
        price_tolerance: Decimal = Decimal("0.01"),
    ) -> None:
        self._size_tolerance = size_tolerance
        self._price_tolerance = price_tolerance

    def reconstruct(
        self,
        trades: Sequence[ObservedTrade],
        *,
        snapshot: PositionSnapshotRecord | None = None,
    ) -> ReconstructedPosition:
        """Reconstruct one ``(trader, condition_id, token_id)`` position."""
        ordered_trades = self._order_and_validate_trades(trades)
        seed_trade = ordered_trades[0] if ordered_trades else None

        if snapshot is not None and seed_trade is not None:
            trade_key = self._trade_key(seed_trade)
            snapshot_key = self._snapshot_key(snapshot)
            if trade_key != snapshot_key:
                raise ValueError(
                    "snapshot key does not match trade key: "
                    f"{snapshot_key!r} != {trade_key!r}"
                )

        pos = ReconstructedPosition(
            trader_wallet=(
                seed_trade.trader_wallet
                if seed_trade is not None
                else (snapshot.trader_wallet if snapshot is not None else "")
            ),
            condition_id=(
                seed_trade.condition_id
                if seed_trade is not None
                else (snapshot.condition_id if snapshot is not None else "")
            ),
            token_id=(
                seed_trade.token_id
                if seed_trade is not None
                else (snapshot.token_id if snapshot is not None else "")
            ),
        )

        for trade in ordered_trades:
            pos.observed_trade_count += 1
            self._apply_trade(pos, trade)

        if snapshot is not None:
            self._reconcile_snapshot(pos, snapshot)

        self._finalize_status(pos)
        return pos

    def reconstruct_many(
        self,
        trades: Sequence[ObservedTrade],
        *,
        snapshots_by_key: Mapping[PositionKey, PositionSnapshotRecord] | None = None,
    ) -> dict[PositionKey, ReconstructedPosition]:
        """Group trades by position key and reconstruct each position."""
        grouped: dict[PositionKey, list[ObservedTrade]] = defaultdict(list)
        for trade in trades:
            grouped[self._trade_key(trade)].append(trade)

        results: dict[PositionKey, ReconstructedPosition] = {}
        for key, grouped_trades in grouped.items():
            results[key] = self.reconstruct(
                grouped_trades,
                snapshot=(snapshots_by_key or {}).get(key),
            )

        if snapshots_by_key:
            for key, snapshot in snapshots_by_key.items():
                if key in results:
                    continue
                results[key] = self.reconstruct([], snapshot=snapshot)

        return results

    # ------------------------------------------------------------------

    def _order_and_validate_trades(
        self, trades: Sequence[ObservedTrade]
    ) -> list[ObservedTrade]:
        ordered = sorted(
            trades,
            key=lambda trade: (
                trade.event_ts,
                trade.trade_id or "",
                trade.tx_hash or "",
                trade.side,
            ),
        )
        if not ordered:
            return []

        expected_key = self._trade_key(ordered[0])
        for trade in ordered[1:]:
            if self._trade_key(trade) != expected_key:
                raise ValueError("reconstruct() requires trades for exactly one position key")
        return ordered

    def _apply_trade(self, pos: ReconstructedPosition, trade: ObservedTrade) -> None:
        if trade.side == "BUY":
            self._apply_buy(pos, trade)
            return
        if trade.side == "SELL":
            self._apply_sell(pos, trade)

    def _apply_buy(self, pos: ReconstructedPosition, trade: ObservedTrade) -> None:
        pos.lots.append(
            InventoryLot(
                size=trade.size,
                price=trade.price,
                ts=trade.event_ts,
                tx_hash=trade.tx_hash,
            )
        )
        pos.total_bought += trade.size
        pos.total_buy_cost += trade.size * trade.price

    def _apply_sell(self, pos: ReconstructedPosition, trade: ObservedTrade) -> None:
        remaining = trade.size
        pos.total_sold += trade.size

        while remaining > 0 and pos.lots:
            lot = pos.lots[0]
            matched_size = min(lot.size, remaining)
            pos.realized_pnl += matched_size * (trade.price - lot.price)
            remaining -= matched_size

            if matched_size == lot.size:
                pos.lots.pop(0)
            else:
                lot.size -= matched_size

        if remaining > 0:
            self._record_mismatch(
                pos,
                ReconstructionMismatch(
                    mismatch_type=ReconstructionMismatchType.OVERSOLD,
                    detail=(
                        "Observed SELL exceeded reconstructed inventory; "
                        "unmatched quantity was left unresolved."
                    ),
                    quantity=remaining,
                    event_ts=trade.event_ts,
                    tx_hash=trade.tx_hash,
                    trade_id=trade.trade_id,
                    reconstructed_size=Decimal("0"),
                ),
                ReconstructionConfidence.LOW,
            )

    # ------------------------------------------------------------------

    def _reconcile_snapshot(
        self,
        pos: ReconstructedPosition,
        snapshot: PositionSnapshotRecord,
    ) -> None:
        reconstructed_size = pos.current_size
        reported_size = snapshot.size

        if not self._is_close(reconstructed_size, reported_size, self._size_tolerance):
            mismatch_type = (
                ReconstructionMismatchType.MISSING_HISTORY
                if reported_size > reconstructed_size
                else ReconstructionMismatchType.SNAPSHOT_SIZE_MISMATCH
            )
            detail = (
                "Reported snapshot size exceeds reconstructed inventory; "
                "history is incomplete."
                if mismatch_type == ReconstructionMismatchType.MISSING_HISTORY
                else "Reported snapshot size is below reconstructed inventory; "
                "snapshot or sell history does not reconcile."
            )
            self._record_mismatch(
                pos,
                ReconstructionMismatch(
                    mismatch_type=mismatch_type,
                    detail=detail,
                    reconstructed_size=reconstructed_size,
                    reported_size=reported_size,
                    reported_avg_price=snapshot.avg_price,
                    reconstructed_avg_cost=pos.avg_cost,
                ),
                ReconstructionConfidence.LOW,
            )
            return

        if reported_size > 0 and not self._is_close(pos.avg_cost, snapshot.avg_price, self._price_tolerance):
            self._record_mismatch(
                pos,
                ReconstructionMismatch(
                    mismatch_type=ReconstructionMismatchType.AVG_COST_MISMATCH,
                    detail=(
                        "Reported snapshot average price differs from reconstructed "
                        "cost basis."
                    ),
                    reconstructed_size=reconstructed_size,
                    reported_size=reported_size,
                    reconstructed_avg_cost=pos.avg_cost,
                    reported_avg_price=snapshot.avg_price,
                ),
                ReconstructionConfidence.MEDIUM,
            )

    def _record_mismatch(
        self,
        pos: ReconstructedPosition,
        mismatch: ReconstructionMismatch,
        confidence: ReconstructionConfidence,
    ) -> None:
        pos.mismatches.append(mismatch)
        self._degrade_confidence(pos, confidence)

    def _degrade_confidence(
        self,
        pos: ReconstructedPosition,
        target: ReconstructionConfidence,
    ) -> None:
        if self._CONFIDENCE_ORDER[target] < self._CONFIDENCE_ORDER[pos.confidence]:
            pos.confidence = target

    def _finalize_status(self, pos: ReconstructedPosition) -> None:
        blocking_mismatches = {
            ReconstructionMismatchType.OVERSOLD,
            ReconstructionMismatchType.MISSING_HISTORY,
            ReconstructionMismatchType.SNAPSHOT_SIZE_MISMATCH,
        }
        if any(m.mismatch_type in blocking_mismatches for m in pos.mismatches):
            pos.status = ReconstructedPositionStatus.UNKNOWN
            return
        if pos.current_size > 0:
            pos.status = ReconstructedPositionStatus.OPEN
            return
        pos.status = ReconstructedPositionStatus.CLOSED

    def _trade_key(self, trade: ObservedTrade) -> PositionKey:
        return trade.trader_wallet, trade.condition_id, trade.token_id

    def _snapshot_key(self, snapshot: PositionSnapshotRecord) -> PositionKey:
        return snapshot.trader_wallet, snapshot.condition_id, snapshot.token_id

    def _is_close(self, left: Decimal, right: Decimal, tolerance: Decimal) -> bool:
        return abs(left - right) <= tolerance
