"""Aggregate per-position CompositeScoreRecords into a trader-level score.

This bridges the gap between position-level scoring (Phase 5) and
trader-level promotion decisions (leader registry / permission ladder).

Aggregation rules:
- Only positions with AVAILABLE or PARTIAL composite availability count.
- Trader score = weighted average of position scores, weighted by
  the number of available families (more evidence → more influence).
- A trader with zero scoreable positions gets score=None (not zero).
- Reports how many positions contributed and total family coverage.

Does NOT fabricate scores for positions with UNAVAILABLE composites.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from domain.composite_score_models import (
    CompositeAvailability,
    CompositeScoreRecord,
)


@dataclass(frozen=True)
class TraderAggregateScore:
    """Trader-level aggregated score from per-position composites."""

    trader_wallet: str
    aggregate_score: float | None
    positions_scored: int
    positions_total: int
    avg_families_available: float
    min_composite: float | None
    max_composite: float | None
    has_sufficient_coverage: bool

    @property
    def scoreable(self) -> bool:
        return self.aggregate_score is not None


# Minimum scored positions needed for sufficient coverage
_MIN_SCORED_POSITIONS = 2


def aggregate_trader_score(
    trader_wallet: str,
    position_scores: Sequence[CompositeScoreRecord],
    min_scored_positions: int = _MIN_SCORED_POSITIONS,
) -> TraderAggregateScore:
    """Aggregate per-position composites into a single trader score.

    Parameters
    ----------
    trader_wallet:
        Address of the leader.
    position_scores:
        All CompositeScoreRecords for this trader's positions.
    min_scored_positions:
        Minimum positions with a composite score needed for
        ``has_sufficient_coverage`` to be True.

    Returns
    -------
    TraderAggregateScore
        Aggregated score; ``aggregate_score`` is None when no positions
        have a usable composite.
    """
    scoreable = [
        csr for csr in position_scores
        if csr.composite_score is not None
        and csr.availability in (
            CompositeAvailability.AVAILABLE,
            CompositeAvailability.PARTIAL,
        )
    ]

    total = len(position_scores)

    if not scoreable:
        return TraderAggregateScore(
            trader_wallet=trader_wallet,
            aggregate_score=None,
            positions_scored=0,
            positions_total=total,
            avg_families_available=0.0,
            min_composite=None,
            max_composite=None,
            has_sufficient_coverage=False,
        )

    # Weight each position by number of families available (1-3).
    # More evidence = more influence on the aggregate.
    total_weight = sum(csr.families_available for csr in scoreable)
    if total_weight <= 0:
        total_weight = len(scoreable)
        weighted_sum = sum(csr.composite_score for csr in scoreable)
    else:
        weighted_sum = sum(
            csr.composite_score * csr.families_available
            for csr in scoreable
        )

    agg = weighted_sum / total_weight

    composites = [csr.composite_score for csr in scoreable]
    families = [csr.families_available for csr in scoreable]

    return TraderAggregateScore(
        trader_wallet=trader_wallet,
        aggregate_score=round(agg, 6),
        positions_scored=len(scoreable),
        positions_total=total,
        avg_families_available=round(sum(families) / len(families), 2),
        min_composite=min(composites),
        max_composite=max(composites),
        has_sufficient_coverage=len(scoreable) >= min_scored_positions,
    )
