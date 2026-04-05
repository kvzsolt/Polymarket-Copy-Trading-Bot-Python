"""Tests for the Phase 5 composite-scoring bridge.

Coverage:
- Full-availability metric record → composite score with decomposed contributions.
- Partially-available families degrade honestly (PARTIAL, lower families_available).
- Fully-unavailable metric record → composite is None (UNAVAILABLE).
- Decomposed contributions remain individually visible/auditable.
- Custom weights are respected.
- Batch scoring preserves keys.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from domain.composite_score_models import (
    CompositeAvailability,
    CompositeScoreRecord,
    FamilyContribution,
    FamilyWeights,
)
from domain.discovery_models import ReconstructionConfidence
from domain.metric_models import (
    DelaySurvivalMetrics,
    EntryAlphaMetrics,
    EntryAlphaWindowMetric,
    MetricAvailability,
    MetricFamilyRecord,
    SlippageCopyabilityMetrics,
)
from services.discovery.composite_scoring_bridge import (
    CompositeScoringBridge,
    _normalise_delay_survival,
    _normalise_entry_alpha,
    _normalise_slippage_copyability,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _available_window(label: str, price: str = "0.55", count: int = 3) -> EntryAlphaWindowMetric:
    return EntryAlphaWindowMetric(
        window_label=label,
        availability=MetricAvailability.AVAILABLE,
        vwap_entry_price=Decimal(price),
        trade_count=count,
    )


def _full_entry_alpha() -> EntryAlphaMetrics:
    return EntryAlphaMetrics(
        window_5m=_available_window("5m"),
        window_30m=_available_window("30m"),
        window_4h=_available_window("4h"),
    )


def _full_delay_survival() -> DelaySurvivalMetrics:
    return DelaySurvivalMetrics(
        availability=MetricAvailability.AVAILABLE,
        unavailable_reason=None,
        delay_seconds=45,
        position_still_open=True,
        reconstruction_confidence=ReconstructionConfidence.HIGH,
        has_mismatch=False,
        confidence_sufficient=True,
    )


def _full_slippage() -> SlippageCopyabilityMetrics:
    return SlippageCopyabilityMetrics(
        availability=MetricAvailability.AVAILABLE,
        unavailable_reason=None,
        spread_decimal=Decimal("0.02"),
        spread_bps=200,
        mid_price=Decimal("0.50"),
        book_depth_available=True,
        snapshot_age_seconds=30,
        snapshot_stale=False,
    )


def _full_mfr(wallet: str = "0xAAA") -> MetricFamilyRecord:
    return MetricFamilyRecord(
        trader_wallet=wallet,
        condition_id="C1",
        token_id="T1",
        entry_alpha=_full_entry_alpha(),
        delay_survival=_full_delay_survival(),
        slippage_copyability=_full_slippage(),
    )


def _empty_mfr(wallet: str = "0xBBB") -> MetricFamilyRecord:
    """All families unavailable (defaults)."""
    return MetricFamilyRecord(
        trader_wallet=wallet,
        condition_id="C2",
        token_id="T2",
    )


@pytest.fixture
def bridge() -> CompositeScoringBridge:
    return CompositeScoringBridge()


# ---------------------------------------------------------------------------
# Normaliser unit tests
# ---------------------------------------------------------------------------

class TestNormalisers:
    def test_entry_alpha_all_windows(self):
        v = _normalise_entry_alpha(_full_entry_alpha())
        assert v is not None
        assert v == pytest.approx(1.0)

    def test_entry_alpha_partial_windows(self):
        ea = EntryAlphaMetrics(
            window_5m=_available_window("5m"),
            # 30m and 4h default to unavailable
        )
        v = _normalise_entry_alpha(ea)
        assert v is not None
        assert 0.0 < v < 1.0
        assert v == pytest.approx(0.50)

    def test_entry_alpha_no_windows(self):
        ea = EntryAlphaMetrics()  # all defaults → unavailable
        assert _normalise_entry_alpha(ea) is None

    def test_delay_survival_full(self):
        v = _normalise_delay_survival(_full_delay_survival())
        assert v is not None
        assert v == pytest.approx(1.0)

    def test_delay_survival_position_closed(self):
        ds = DelaySurvivalMetrics(
            availability=MetricAvailability.AVAILABLE,
            unavailable_reason=None,
            delay_seconds=120,
            position_still_open=False,
            reconstruction_confidence=ReconstructionConfidence.HIGH,
            has_mismatch=False,
            confidence_sufficient=True,
        )
        v = _normalise_delay_survival(ds)
        assert v is not None
        assert v == pytest.approx(0.75)  # 0.5 base + 0.25 confidence

    def test_delay_survival_unavailable(self):
        assert _normalise_delay_survival(DelaySurvivalMetrics()) is None

    def test_slippage_tight_spread(self):
        v = _normalise_slippage_copyability(_full_slippage())
        assert v is not None
        assert v == pytest.approx(0.60)  # 1 - 200/500

    def test_slippage_wide_spread(self):
        sc = SlippageCopyabilityMetrics(
            availability=MetricAvailability.AVAILABLE,
            unavailable_reason=None,
            spread_bps=500,
            book_depth_available=True,
            snapshot_stale=False,
        )
        v = _normalise_slippage_copyability(sc)
        assert v is not None
        assert v == pytest.approx(0.0)

    def test_slippage_stale_snapshot_penalty(self):
        sc = SlippageCopyabilityMetrics(
            availability=MetricAvailability.AVAILABLE,
            unavailable_reason=None,
            spread_bps=100,
            book_depth_available=True,
            snapshot_stale=True,
        )
        v = _normalise_slippage_copyability(sc)
        assert v is not None
        # (1 - 100/500) * 0.5 = 0.8 * 0.5 = 0.4
        assert v == pytest.approx(0.40)

    def test_slippage_no_depth_penalty(self):
        sc = SlippageCopyabilityMetrics(
            availability=MetricAvailability.AVAILABLE,
            unavailable_reason=None,
            spread_bps=0,
            book_depth_available=False,
            snapshot_stale=False,
        )
        v = _normalise_slippage_copyability(sc)
        assert v is not None
        # 1.0 * 0.75 = 0.75
        assert v == pytest.approx(0.75)

    def test_slippage_unavailable(self):
        assert _normalise_slippage_copyability(SlippageCopyabilityMetrics()) is None


# ---------------------------------------------------------------------------
# Bridge: full-availability path
# ---------------------------------------------------------------------------

class TestFullAvailability:
    def test_composite_is_available(self, bridge: CompositeScoringBridge):
        result = bridge.score(_full_mfr())
        assert result.availability == CompositeAvailability.AVAILABLE
        assert result.families_available == 3
        assert result.composite_score is not None
        assert 0.0 <= result.composite_score <= 1.0

    def test_key_preserved(self, bridge: CompositeScoringBridge):
        mfr = _full_mfr("0xWALLET")
        result = bridge.score(mfr)
        assert result.key == ("0xWALLET", "C1", "T1")
        assert result.trader_wallet == "0xWALLET"

    def test_all_contributions_available(self, bridge: CompositeScoringBridge):
        result = bridge.score(_full_mfr())
        for c in result.contributions:
            assert c.available is True
            assert c.normalised_value is not None
            assert c.weighted_value is not None
            assert c.unavailable_reason is None

    def test_contributions_are_individually_visible(self, bridge: CompositeScoringBridge):
        result = bridge.score(_full_mfr())
        names = {c.family_name for c in result.contributions}
        assert names == {"entry_alpha", "delay_survival", "slippage_copyability"}


# ---------------------------------------------------------------------------
# Bridge: unavailable / degradation path
# ---------------------------------------------------------------------------

class TestUnavailableFamilies:
    def test_all_unavailable_gives_none_composite(self, bridge: CompositeScoringBridge):
        result = bridge.score(_empty_mfr())
        assert result.composite_score is None
        assert result.availability == CompositeAvailability.UNAVAILABLE
        assert result.families_available == 0

    def test_all_unavailable_contributions_explain_why(self, bridge: CompositeScoringBridge):
        result = bridge.score(_empty_mfr())
        for c in result.contributions:
            assert c.available is False
            assert c.normalised_value is None
            assert c.unavailable_reason is not None

    def test_partial_availability(self, bridge: CompositeScoringBridge):
        mfr = MetricFamilyRecord(
            trader_wallet="0xPARTIAL",
            condition_id="C3",
            token_id="T3",
            entry_alpha=_full_entry_alpha(),
            # delay_survival and slippage default to unavailable
        )
        result = bridge.score(mfr)
        assert result.availability == CompositeAvailability.PARTIAL
        assert result.families_available == 1
        assert result.composite_score is not None
        # Only entry_alpha contributes → score equals entry_alpha normalised
        assert result.entry_alpha_contribution.available is True
        assert result.delay_survival_contribution.available is False
        assert result.slippage_copyability_contribution.available is False

    def test_partial_composite_equals_single_family_normalised(
        self, bridge: CompositeScoringBridge,
    ):
        """When only one family is available, composite = that family's normalised value."""
        mfr = MetricFamilyRecord(
            trader_wallet="0xSINGLE",
            condition_id="C4",
            token_id="T4",
            delay_survival=_full_delay_survival(),
        )
        result = bridge.score(mfr)
        assert result.composite_score is not None
        assert result.composite_score == pytest.approx(
            result.delay_survival_contribution.normalised_value,
        )


# ---------------------------------------------------------------------------
# Custom weights
# ---------------------------------------------------------------------------

class TestCustomWeights:
    def test_custom_weights_applied(self):
        weights = FamilyWeights(
            entry_alpha=1.0,
            delay_survival=0.0,
            slippage_copyability=0.0,
        )
        bridge = CompositeScoringBridge(weights=weights)
        result = bridge.score(_full_mfr())
        assert result.weights_used == weights
        # Only entry_alpha matters
        assert result.composite_score == pytest.approx(
            result.entry_alpha_contribution.normalised_value,
        )

    def test_equal_weights(self):
        weights = FamilyWeights(
            entry_alpha=1.0,
            delay_survival=1.0,
            slippage_copyability=1.0,
        )
        bridge = CompositeScoringBridge(weights=weights)
        result = bridge.score(_full_mfr())
        # Simple average of three normalised values
        expected = (
            (result.entry_alpha_contribution.normalised_value or 0)
            + (result.delay_survival_contribution.normalised_value or 0)
            + (result.slippage_copyability_contribution.normalised_value or 0)
        ) / 3.0
        assert result.composite_score == pytest.approx(expected)


# ---------------------------------------------------------------------------
# Batch scoring
# ---------------------------------------------------------------------------

class TestBatchScoring:
    def test_batch_preserves_keys(self, bridge: CompositeScoringBridge):
        mfr1 = _full_mfr("0xA")
        mfr2 = _empty_mfr("0xB")
        batch = {mfr1.key: mfr1, mfr2.key: mfr2}
        results = bridge.score_batch(batch)
        assert set(results.keys()) == set(batch.keys())
        assert results[mfr1.key].composite_score is not None
        assert results[mfr2.key].composite_score is None
