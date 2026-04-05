"""Thin repo-backed reconstruction orchestration for research consumers."""
from __future__ import annotations

from domain.discovery_models import PositionSnapshotRecord, ReconstructedPosition
from services.discovery.reconstruction_service import ReconstructionService
from storage.sqlite.discovery_repo import DiscoveryRepo

PositionKey = tuple[str, str, str]


class ReconstructedPositionsService:
    """Load stored discovery inputs and reconstruct positions by natural key."""

    def __init__(
        self,
        repo: DiscoveryRepo,
        reconstruction_service: ReconstructionService | None = None,
    ) -> None:
        self._repo = repo
        self._reconstruction_service = reconstruction_service or ReconstructionService()

    def reconstruct_for_trader(
        self,
        trader_wallet: str,
        *,
        condition_id: str | None = None,
        token_id: str | None = None,
        since_ts: int | None = None,
        trade_limit: int = 5000,
    ) -> dict[PositionKey, ReconstructedPosition]:
        trades = self._repo.list_trades(
            trader_wallet,
            condition_id=condition_id,
            token_id=token_id,
            since_ts=since_ts,
            limit=trade_limit,
        )
        snapshots_by_key = {
            self._snapshot_key(snapshot): snapshot
            for snapshot in self._repo.list_latest_position_snapshots(
                trader_wallet,
                condition_id=condition_id,
                token_id=token_id,
            )
        }
        return self._reconstruction_service.reconstruct_many(
            trades,
            snapshots_by_key=snapshots_by_key,
        )

    def _snapshot_key(self, snapshot: PositionSnapshotRecord) -> PositionKey:
        return (
            snapshot.trader_wallet,
            snapshot.condition_id,
            snapshot.token_id,
        )
