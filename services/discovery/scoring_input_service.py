"""Read-only bridge from reconstructed positions into scoring-input records."""
from __future__ import annotations

from domain.discovery_models import (
    ContextBucket,
    MarketMeta,
    PositionSnapshotRecord,
    ReconstructedPosition,
)
from domain.scoring_models import (
    PositionKey,
    ScoringInputEvidence,
    ScoringInputMarketContext,
    ScoringInputRecord,
)
from services.discovery.context_service import ContextService
from services.discovery.reconstructed_positions_service import (
    ReconstructedPositionsService,
)
from storage.sqlite.discovery_repo import DiscoveryRepo


class ScoringInputAssemblerService:
    """Assemble stable scoring inputs without computing any final scores."""

    def __init__(
        self,
        repo: DiscoveryRepo,
        *,
        reconstructed_positions_service: ReconstructedPositionsService | None = None,
        context_service: ContextService | None = None,
    ) -> None:
        self._repo = repo
        self._reconstructed_positions = (
            reconstructed_positions_service or ReconstructedPositionsService(repo)
        )
        self._context_service = context_service or ContextService()

    def assemble_for_trader(
        self,
        trader_wallet: str,
        *,
        condition_id: str | None = None,
        token_id: str | None = None,
        since_ts: int | None = None,
        trade_limit: int = 5000,
    ) -> dict[PositionKey, ScoringInputRecord]:
        positions = self._reconstructed_positions.reconstruct_for_trader(
            trader_wallet,
            condition_id=condition_id,
            token_id=token_id,
            since_ts=since_ts,
            trade_limit=trade_limit,
        )
        snapshots_by_key = {
            self._snapshot_key(snapshot): snapshot
            for snapshot in self._repo.list_latest_position_snapshots(
                trader_wallet,
                condition_id=condition_id,
                token_id=token_id,
            )
        }
        last_trade_ts_by_key = self._build_last_trade_ts_index(
            trader_wallet,
            condition_id=condition_id,
            token_id=token_id,
            since_ts=since_ts,
            trade_limit=trade_limit,
        )

        market_cache: dict[str, MarketMeta | None] = {}
        records: dict[PositionKey, ScoringInputRecord] = {}
        for key in sorted(positions):
            position = positions[key]
            snapshot = snapshots_by_key.get(key)
            market = market_cache.get(key[1])
            if key[1] not in market_cache:
                market = self._repo.get_market(key[1])
                market_cache[key[1]] = market
            records[key] = self._build_record(
                position,
                snapshot=snapshot,
                last_trade_ts=last_trade_ts_by_key.get(key),
                market=market,
            )
        return records

    def _build_last_trade_ts_index(
        self,
        trader_wallet: str,
        *,
        condition_id: str | None = None,
        token_id: str | None = None,
        since_ts: int | None = None,
        trade_limit: int = 5000,
    ) -> dict[PositionKey, int]:
        last_trade_ts_by_key: dict[PositionKey, int] = {}
        for trade in self._repo.list_trades(
            trader_wallet,
            condition_id=condition_id,
            token_id=token_id,
            since_ts=since_ts,
            limit=trade_limit,
        ):
            key = (trade.trader_wallet, trade.condition_id, trade.token_id)
            current = last_trade_ts_by_key.get(key)
            if current is None or trade.event_ts > current:
                last_trade_ts_by_key[key] = trade.event_ts
        return last_trade_ts_by_key

    def _build_record(
        self,
        position: ReconstructedPosition,
        *,
        snapshot: PositionSnapshotRecord | None,
        last_trade_ts: int | None,
        market: MarketMeta | None,
    ) -> ScoringInputRecord:
        reference_ts = self._resolve_reference_ts(last_trade_ts, snapshot)
        return ScoringInputRecord(
            trader_wallet=position.trader_wallet,
            condition_id=position.condition_id,
            token_id=position.token_id,
            current_size=position.current_size,
            avg_cost=position.avg_cost,
            current_cost_basis=position.current_cost_basis,
            realized_pnl=position.realized_pnl,
            observed_trade_count=position.observed_trade_count,
            position_status=position.status,
            reconstruction_confidence=position.confidence,
            mismatch_types=tuple(
                mismatch.mismatch_type.value for mismatch in position.mismatches
            ),
            mismatch_details=tuple(mismatch.detail for mismatch in position.mismatches),
            evidence=ScoringInputEvidence(
                last_observed_trade_ts=last_trade_ts,
                latest_snapshot_ts=(snapshot.snapshot_ts if snapshot is not None else None),
                snapshot_size=(snapshot.size if snapshot is not None else None),
                snapshot_avg_price=(
                    snapshot.avg_price if snapshot is not None else None
                ),
                has_snapshot=snapshot is not None,
            ),
            market_context=self._build_market_context(
                market,
                reference_ts=reference_ts,
            ),
        )

    def _build_market_context(
        self,
        market: MarketMeta | None,
        *,
        reference_ts: int | None,
    ) -> ScoringInputMarketContext:
        if market is None:
            return ScoringInputMarketContext(reference_ts=reference_ts)

        buckets: list[ContextBucket] = []
        category_bucket = self._context_service.compute_category_bucket(market.category)
        if category_bucket is not None:
            buckets.append(category_bucket)
        if market.end_date_ts is not None and reference_ts is not None:
            days_bucket = self._context_service.compute_days_to_resolution_bucket(
                market.end_date_ts,
                reference_ts=reference_ts,
            )
            if days_bucket is not None:
                buckets.append(days_bucket)

        return ScoringInputMarketContext(
            question=market.question,
            market_slug=market.slug,
            event_slug=market.event_slug,
            category=market.category,
            end_date_ts=market.end_date_ts,
            active=market.active,
            resolved=market.resolved,
            neg_risk=market.neg_risk,
            buckets=tuple(sorted(buckets, key=lambda bucket: bucket.dimension)),
            reference_ts=reference_ts,
            has_market_meta=True,
        )

    def _resolve_reference_ts(
        self,
        last_trade_ts: int | None,
        snapshot: PositionSnapshotRecord | None,
    ) -> int | None:
        snapshot_ts = snapshot.snapshot_ts if snapshot is not None else None
        if last_trade_ts is None:
            return snapshot_ts
        if snapshot_ts is None:
            return last_trade_ts
        return max(last_trade_ts, snapshot_ts)

    def _snapshot_key(self, snapshot: PositionSnapshotRecord) -> PositionKey:
        return (snapshot.trader_wallet, snapshot.condition_id, snapshot.token_id)
