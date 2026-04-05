"""Tests for metric-family calculators.

These construct ``RawFeatureRecord`` instances directly (no DB needed)
and verify that the calculators produce honest, typed metric outputs.

Coverage targets:
- raw feature → entry-alpha metric outputs (per window)
- missing evidence degrades availability instead of fabricating values
- low-confidence / missing-history states remain visible downstream
- slippage metrics only where orderbook evidence exists
- stale orderbook snapshots are flagged
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from domain.discovery_models import (
    ReconstructionConfidence,
    ReconstructedPositionStatus,
)
from domain.metric_models import (
    MetricAvailability,
    MetricFamilyRecord,
)
from domain.raw_feature_models import (
    DelaySurvivalInputs,
    EntryAlphaInputs,
    EvidencePresenceFlags,
    RawFeatureRecord,
    SlippageCopyabilityInputs,
)
from services.discovery.metric_calculators import MetricFamilyCalculator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raw(
    *,
    entry_alpha: EntryAlphaInputs | None = None,
    delay_survival: DelaySurvivalInputs | None = None,
    slippage: SlippageCopyabilityInputs | None = None,
    wallet: str = "0xLEADER",
    condition_id: str = "cond1",
    token_id: str = "tok1",
) -> RawFeatureRecord:
    """Build a minimal ``RawFeatureRecord`` with optional sub-record overrides."""
    return RawFeatureRecord(
        trader_wallet=wallet,
        condition_id=condition_id,
        token_id=token_id,
        current_size=Decimal("100"),
        avg_cost=Decimal("0.55"),
        realized_pnl=Decimal("0"),
        position_status=ReconstructedPositionStatus.OPEN,
        reconstruction_confidence=ReconstructionConfidence.HIGH,
        entry_alpha=entry_alpha or EntryAlphaInputs(),
        delay_survival=delay_survival or DelaySurvivalInputs(),
        slippage_copyability=slippage or SlippageCopyabilityInputs(),
        evidence_flags=EvidencePresenceFlags(),
    )


@pytest.fixture
def calc() -> MetricFamilyCalculator:
    return MetricFamilyCalculator()


# ===================================================================
# Entry-alpha family
# ===================================================================

class TestEntryAlphaMetrics:
    """Transform raw entry-alpha inputs into metric outputs."""

    def test_all_windows_available(self, calc: MetricFamilyCalculator):
        rec = _raw(entry_alpha=EntryAlphaInputs(
            last_trade_ts=1000,
            reference_ts=1100,
            window_5m_available=True,
            window_5m_entry_price=Decimal("0.60"),
            window_5m_trade_count=3,
            window_30m_available=True,
            window_30m_entry_price=Decimal("0.58"),
            window_30m_trade_count=7,
            window_4h_available=True,
            window_4h_entry_price=Decimal("0.55"),
            window_4h_trade_count=12,
        ))
        m = calc.compute(rec)
        ea = m.entry_alpha

        assert ea.any_window_available is True

        assert ea.window_5m.availability == MetricAvailability.AVAILABLE
        assert ea.window_5m.vwap_entry_price == Decimal("0.60")
        assert ea.window_5m.trade_count == 3

        assert ea.window_30m.availability == MetricAvailability.AVAILABLE
        assert ea.window_30m.vwap_entry_price == Decimal("0.58")
        assert ea.window_30m.trade_count == 7

        assert ea.window_4h.availability == MetricAvailability.AVAILABLE
        assert ea.window_4h.vwap_entry_price == Decimal("0.55")
        assert ea.window_4h.trade_count == 12

    def test_partial_windows(self, calc: MetricFamilyCalculator):
        """Only 5m window has evidence — 30m and 4h should be unavailable."""
        rec = _raw(entry_alpha=EntryAlphaInputs(
            last_trade_ts=1000,
            reference_ts=1100,
            window_5m_available=True,
            window_5m_entry_price=Decimal("0.60"),
            window_5m_trade_count=2,
        ))
        m = calc.compute(rec)
        ea = m.entry_alpha

        assert ea.any_window_available is True
        assert ea.window_5m.availability == MetricAvailability.AVAILABLE
        assert ea.window_30m.availability == MetricAvailability.UNAVAILABLE_NO_EVIDENCE
        assert ea.window_4h.availability == MetricAvailability.UNAVAILABLE_NO_EVIDENCE

    def test_no_windows_available(self, calc: MetricFamilyCalculator):
        """All windows are empty — all should be unavailable."""
        rec = _raw()
        m = calc.compute(rec)
        ea = m.entry_alpha

        assert ea.any_window_available is False
        assert ea.window_5m.availability == MetricAvailability.UNAVAILABLE_NO_EVIDENCE
        assert ea.window_5m.vwap_entry_price is None
        assert ea.window_5m.unavailable_reason is not None
        assert ea.window_30m.availability == MetricAvailability.UNAVAILABLE_NO_EVIDENCE
        assert ea.window_4h.availability == MetricAvailability.UNAVAILABLE_NO_EVIDENCE

    def test_window_available_but_price_none(self, calc: MetricFamilyCalculator):
        """Flag says available but price is None — treat as unavailable."""
        rec = _raw(entry_alpha=EntryAlphaInputs(
            window_5m_available=True,
            window_5m_entry_price=None,
            window_5m_trade_count=0,
        ))
        m = calc.compute(rec)
        assert m.entry_alpha.window_5m.availability == MetricAvailability.UNAVAILABLE_NO_EVIDENCE
        assert m.entry_alpha.window_5m.unavailable_reason == "entry price missing"


# ===================================================================
# Delay-survival family
# ===================================================================

class TestDelaySurvivalMetrics:
    """Delay-survival evidence transforms."""

    def test_available_high_confidence(self, calc: MetricFamilyCalculator):
        rec = _raw(delay_survival=DelaySurvivalInputs(
            delay_seconds=45,
            position_still_open=True,
            reconstruction_confidence=ReconstructionConfidence.HIGH,
            has_mismatch=False,
        ))
        m = calc.compute(rec)
        ds = m.delay_survival

        assert ds.availability == MetricAvailability.AVAILABLE
        assert ds.delay_seconds == 45
        assert ds.position_still_open is True
        assert ds.confidence_sufficient is True
        assert ds.unavailable_reason is None

    def test_available_medium_confidence(self, calc: MetricFamilyCalculator):
        rec = _raw(delay_survival=DelaySurvivalInputs(
            delay_seconds=120,
            position_still_open=False,
            reconstruction_confidence=ReconstructionConfidence.MEDIUM,
            has_mismatch=True,
        ))
        m = calc.compute(rec)
        ds = m.delay_survival

        assert ds.availability == MetricAvailability.AVAILABLE
        assert ds.confidence_sufficient is True
        assert ds.has_mismatch is True

    def test_low_confidence_degrades_availability(self, calc: MetricFamilyCalculator):
        """LOW confidence → metric is present but flagged as low-confidence."""
        rec = _raw(delay_survival=DelaySurvivalInputs(
            delay_seconds=90,
            position_still_open=True,
            reconstruction_confidence=ReconstructionConfidence.LOW,
            has_mismatch=True,
        ))
        m = calc.compute(rec)
        ds = m.delay_survival

        assert ds.availability == MetricAvailability.UNAVAILABLE_LOW_CONFIDENCE
        assert ds.confidence_sufficient is False
        # Values are still passed through for transparency
        assert ds.delay_seconds == 90
        assert ds.position_still_open is True
        assert "LOW" in (ds.unavailable_reason or "")

    def test_no_timestamps_unavailable(self, calc: MetricFamilyCalculator):
        """No trade timestamps → completely unavailable."""
        rec = _raw(delay_survival=DelaySurvivalInputs(
            delay_seconds=None,
        ))
        m = calc.compute(rec)
        ds = m.delay_survival

        assert ds.availability == MetricAvailability.UNAVAILABLE_NO_EVIDENCE
        assert ds.delay_seconds is None
        assert ds.confidence_sufficient is False


# ===================================================================
# Slippage / copyability family
# ===================================================================

class TestSlippageCopyabilityMetrics:
    """Slippage metrics only where orderbook evidence exists."""

    def test_no_orderbook_unavailable(self, calc: MetricFamilyCalculator):
        rec = _raw()
        m = calc.compute(rec)
        sc = m.slippage_copyability

        assert sc.availability == MetricAvailability.UNAVAILABLE_NO_EVIDENCE
        assert sc.spread_decimal is None
        assert sc.spread_bps is None
        assert sc.mid_price is None
        assert "no orderbook" in (sc.unavailable_reason or "").lower()

    def test_fresh_orderbook_available(self, calc: MetricFamilyCalculator):
        rec = _raw(slippage=SlippageCopyabilityInputs(
            has_orderbook_snapshot=True,
            snapshot_ts=1000,
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.52"),
            mid_price=Decimal("0.51"),
            spread=Decimal("0.02"),
            book_depth_available=True,
            snapshot_age_seconds=60,
        ))
        m = calc.compute(rec)
        sc = m.slippage_copyability

        assert sc.availability == MetricAvailability.AVAILABLE
        assert sc.spread_decimal == Decimal("0.02")
        assert sc.mid_price == Decimal("0.51")
        assert sc.book_depth_available is True
        assert sc.snapshot_stale is False
        # spread_bps = (0.02 / 0.51) * 10000 ≈ 392
        assert sc.spread_bps == 392

    def test_stale_orderbook_flagged(self, calc: MetricFamilyCalculator):
        """Orderbook older than 5 minutes → flagged as stale."""
        rec = _raw(slippage=SlippageCopyabilityInputs(
            has_orderbook_snapshot=True,
            snapshot_ts=1000,
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.52"),
            mid_price=Decimal("0.51"),
            spread=Decimal("0.02"),
            book_depth_available=True,
            snapshot_age_seconds=600,
        ))
        m = calc.compute(rec)
        sc = m.slippage_copyability

        assert sc.availability == MetricAvailability.UNAVAILABLE_STALE_DATA
        assert sc.snapshot_stale is True
        # Values are still passed through for transparency
        assert sc.spread_decimal == Decimal("0.02")
        assert sc.spread_bps is not None

    def test_orderbook_exactly_at_staleness_threshold_is_still_usable(self, calc: MetricFamilyCalculator):
        rec = _raw(slippage=SlippageCopyabilityInputs(
            has_orderbook_snapshot=True,
            snapshot_ts=1000,
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.52"),
            mid_price=Decimal("0.51"),
            spread=Decimal("0.02"),
            book_depth_available=True,
            snapshot_age_seconds=300,
        ))
        m = calc.compute(rec)
        sc = m.slippage_copyability

        assert sc.availability == MetricAvailability.AVAILABLE
        assert sc.snapshot_stale is False
        assert sc.spread_bps == 392

    def test_orderbook_with_no_spread(self, calc: MetricFamilyCalculator):
        """Orderbook exists but spread/mid_price are None."""
        rec = _raw(slippage=SlippageCopyabilityInputs(
            has_orderbook_snapshot=True,
            snapshot_ts=1000,
            best_bid=None,
            best_ask=None,
            mid_price=None,
            spread=None,
            book_depth_available=False,
            snapshot_age_seconds=30,
        ))
        m = calc.compute(rec)
        sc = m.slippage_copyability

        assert sc.availability == MetricAvailability.AVAILABLE
        assert sc.spread_bps is None
        assert sc.book_depth_available is False

    def test_zero_mid_price_no_bps_crash(self, calc: MetricFamilyCalculator):
        """Mid-price of zero should not crash, spread_bps stays None."""
        rec = _raw(slippage=SlippageCopyabilityInputs(
            has_orderbook_snapshot=True,
            snapshot_ts=1000,
            mid_price=Decimal("0"),
            spread=Decimal("0.01"),
            snapshot_age_seconds=10,
        ))
        m = calc.compute(rec)
        assert m.slippage_copyability.spread_bps is None


# ===================================================================
# compute_all batch interface
# ===================================================================

class TestComputeAll:
    """Batch computation over a dict of raw features."""

    def test_empty_input(self, calc: MetricFamilyCalculator):
        result = calc.compute_all({})
        assert result == {}

    def test_multiple_records(self, calc: MetricFamilyCalculator):
        r1 = _raw(wallet="0xA", condition_id="c1", token_id="t1")
        r2 = _raw(
            wallet="0xB",
            condition_id="c2",
            token_id="t2",
            entry_alpha=EntryAlphaInputs(
                window_5m_available=True,
                window_5m_entry_price=Decimal("0.70"),
                window_5m_trade_count=5,
            ),
        )
        features = {r1.key: r1, r2.key: r2}
        result = calc.compute_all(features)

        assert len(result) == 2
        assert ("0xA", "c1", "t1") in result
        assert ("0xB", "c2", "t2") in result

        # r1 has no windows
        assert result[("0xA", "c1", "t1")].entry_alpha.any_window_available is False
        # r2 has 5m
        assert result[("0xB", "c2", "t2")].entry_alpha.window_5m.availability == MetricAvailability.AVAILABLE

    def test_key_consistency(self, calc: MetricFamilyCalculator):
        """Output MetricFamilyRecord.key matches the dict key."""
        rec = _raw(wallet="0xW", condition_id="C", token_id="T")
        result = calc.compute_all({rec.key: rec})
        mfr = result[("0xW", "C", "T")]
        assert mfr.key == ("0xW", "C", "T")
        assert mfr.trader_wallet == "0xW"
        assert mfr.condition_id == "C"
        assert mfr.token_id == "T"
