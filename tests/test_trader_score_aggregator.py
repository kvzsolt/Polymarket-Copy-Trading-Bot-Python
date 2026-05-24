"""Tests for trader-level score aggregation."""
from __future__ import annotations


from domain.composite_score_models import (
    CompositeAvailability,
    CompositeScoreRecord,
)
from services.discovery.trader_score_aggregator import (
    aggregate_trader_score,
)

WALLET = "0xaaaa"
COND = "cond_1"


def _csr(
    score: float | None,
    avail: CompositeAvailability = CompositeAvailability.AVAILABLE,
    families: int = 3,
    token_id: str = "tok_1",
) -> CompositeScoreRecord:
    return CompositeScoreRecord(
        trader_wallet=WALLET,
        condition_id=COND,
        token_id=token_id,
        composite_score=score,
        availability=avail,
        families_available=families,
        families_total=3,
    )


class TestAggregateTraderScore:
    def test_empty_positions_returns_none(self):
        result = aggregate_trader_score(WALLET, [])
        assert result.aggregate_score is None
        assert result.positions_scored == 0
        assert result.positions_total == 0
        assert not result.has_sufficient_coverage
        assert not result.scoreable

    def test_single_fully_available_position(self):
        result = aggregate_trader_score(WALLET, [_csr(0.75)])
        assert result.aggregate_score == 0.75
        assert result.positions_scored == 1
        assert result.positions_total == 1
        assert not result.has_sufficient_coverage  # < 2

    def test_multiple_positions_weighted_by_families(self):
        # pos1: score=0.9, 3 families -> weight 3
        # pos2: score=0.3, 1 family -> weight 1
        # weighted avg = (0.9*3 + 0.3*1) / 4 = 3.0/4 = 0.75
        scores = [_csr(0.9, families=3), _csr(0.3, families=1)]
        result = aggregate_trader_score(WALLET, scores)
        assert result.aggregate_score == 0.75
        assert result.positions_scored == 2
        assert result.has_sufficient_coverage

    def test_unavailable_positions_excluded(self):
        scores = [
            _csr(0.8),
            _csr(None, avail=CompositeAvailability.UNAVAILABLE, families=0),
        ]
        result = aggregate_trader_score(WALLET, scores)
        assert result.aggregate_score == 0.8
        assert result.positions_scored == 1
        assert result.positions_total == 2

    def test_partial_availability_included(self):
        scores = [
            _csr(0.6, avail=CompositeAvailability.PARTIAL, families=2),
            _csr(0.8, avail=CompositeAvailability.AVAILABLE, families=3),
        ]
        result = aggregate_trader_score(WALLET, scores)
        # weighted avg = (0.6*2 + 0.8*3) / 5 = 3.6/5 = 0.72
        assert result.aggregate_score == 0.72
        assert result.positions_scored == 2

    def test_min_max_composites_tracked(self):
        scores = [_csr(0.3), _csr(0.9), _csr(0.6)]
        result = aggregate_trader_score(WALLET, scores)
        assert result.min_composite == 0.3
        assert result.max_composite == 0.9

    def test_all_unavailable_returns_none(self):
        scores = [
            _csr(None, avail=CompositeAvailability.UNAVAILABLE, families=0),
            _csr(None, avail=CompositeAvailability.UNAVAILABLE, families=0),
        ]
        result = aggregate_trader_score(WALLET, scores)
        assert result.aggregate_score is None
        assert result.positions_scored == 0
        assert not result.scoreable

    def test_custom_min_scored_positions(self):
        scores = [_csr(0.5)]
        result = aggregate_trader_score(WALLET, scores, min_scored_positions=1)
        assert result.has_sufficient_coverage

    def test_avg_families_reported(self):
        scores = [_csr(0.5, families=3), _csr(0.7, families=1)]
        result = aggregate_trader_score(WALLET, scores)
        assert result.avg_families_available == 2.0
