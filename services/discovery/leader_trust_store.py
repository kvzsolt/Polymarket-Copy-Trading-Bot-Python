from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class LeaderTrustRow:
    wallet: str
    status: str
    final_score: float
    falcon_quality_score: float
    copyability_score: float
    copyable_signal_ratio: float
    copyable_notional_ratio: float


class LeaderTrustStore:
    """Synchronous in-memory cache for live scoring.

    Live trade decisions must not perform async DB calls. Refresh this cache
    during startup or from a background task, then read it synchronously from
    LeaderScoringService.score_leader().
    """

    def __init__(self):
        self._rows: dict[str, LeaderTrustRow] = {}

    def get_latest(self, wallet: str) -> LeaderTrustRow | None:
        return self._rows.get(str(wallet).lower())

    def update_from_evaluations(self, evaluations) -> None:
        rows: dict[str, LeaderTrustRow] = {}
        for e in evaluations:
            rows[e.wallet.lower()] = LeaderTrustRow(
                wallet=e.wallet.lower(),
                status=e.status,
                final_score=float(e.final_score),
                falcon_quality_score=float(e.falcon_quality_score),
                copyability_score=float(e.copyability_score),
                copyable_signal_ratio=float(e.copyable_signal_ratio),
                copyable_notional_ratio=float(e.copyable_notional_ratio),
            )
        self._rows = rows

    def upsert_evaluation(self, e) -> None:
        self._rows[e.wallet.lower()] = LeaderTrustRow(
            wallet=e.wallet.lower(),
            status=e.status,
            final_score=float(e.final_score),
            falcon_quality_score=float(e.falcon_quality_score),
            copyability_score=float(e.copyability_score),
            copyable_signal_ratio=float(e.copyable_signal_ratio),
            copyable_notional_ratio=float(e.copyable_notional_ratio),
        )

    async def refresh_from_repo(self, repo, *, limit: int = 500) -> None:
        evaluations = await repo.list_evaluations(limit=limit)
        self.update_from_evaluations(evaluations)
