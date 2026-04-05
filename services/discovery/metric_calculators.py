"""Metric-family calculators: transform raw feature records into typed metrics.

Each calculator is stateless and read-only.  Missing evidence degrades
metric availability honestly — no values are fabricated.

Usage::

    calculator = MetricFamilyCalculator()
    metrics = calculator.compute_all(raw_features)  # dict[PositionKey, MetricFamilyRecord]
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from domain.discovery_models import ReconstructionConfidence
from domain.metric_models import (
    DelaySurvivalMetrics,
    EntryAlphaMetrics,
    EntryAlphaWindowMetric,
    MetricAvailability,
    MetricFamilyRecord,
    SlippageCopyabilityMetrics,
)
from domain.raw_feature_models import (
    DelaySurvivalInputs,
    EntryAlphaInputs,
    RawFeatureRecord,
    SlippageCopyabilityInputs,
)
from domain.scoring_models import PositionKey


# ---------------------------------------------------------------------------
# Staleness threshold (seconds) for orderbook snapshots
# ---------------------------------------------------------------------------
_ORDERBOOK_STALE_SECONDS = 300  # 5 minutes


class MetricFamilyCalculator:
    """Stateless calculator: ``RawFeatureRecord`` → ``MetricFamilyRecord``."""

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def compute_all(
        self,
        raw_features: dict[PositionKey, RawFeatureRecord],
    ) -> dict[PositionKey, MetricFamilyRecord]:
        """Compute metric families for every raw-feature record."""
        return {key: self.compute(rec) for key, rec in raw_features.items()}

    def compute(self, rec: RawFeatureRecord) -> MetricFamilyRecord:
        """Compute a single ``MetricFamilyRecord`` from a raw feature record."""
        return MetricFamilyRecord(
            trader_wallet=rec.trader_wallet,
            condition_id=rec.condition_id,
            token_id=rec.token_id,
            entry_alpha=self._compute_entry_alpha(rec.entry_alpha),
            delay_survival=self._compute_delay_survival(rec.delay_survival),
            slippage_copyability=self._compute_slippage_copyability(
                rec.slippage_copyability,
            ),
        )

    # -------------------------------------------------------------------
    # Entry-alpha family
    # -------------------------------------------------------------------

    def _compute_entry_alpha(self, inputs: EntryAlphaInputs) -> EntryAlphaMetrics:
        return EntryAlphaMetrics(
            window_5m=self._compute_entry_alpha_window("5m", inputs.window_5m_available, inputs.window_5m_entry_price, inputs.window_5m_trade_count),
            window_30m=self._compute_entry_alpha_window("30m", inputs.window_30m_available, inputs.window_30m_entry_price, inputs.window_30m_trade_count),
            window_4h=self._compute_entry_alpha_window("4h", inputs.window_4h_available, inputs.window_4h_entry_price, inputs.window_4h_trade_count),
        )

    @staticmethod
    def _compute_entry_alpha_window(
        label: str,
        available: bool,
        entry_price: Decimal | None,
        trade_count: int,
    ) -> EntryAlphaWindowMetric:
        if not available or entry_price is None:
            reason = "no BUY trades in window" if not available else "entry price missing"
            return EntryAlphaWindowMetric(
                window_label=label,
                availability=MetricAvailability.UNAVAILABLE_NO_EVIDENCE,
                unavailable_reason=reason,
            )

        return EntryAlphaWindowMetric(
            window_label=label,
            availability=MetricAvailability.AVAILABLE,
            vwap_entry_price=entry_price,
            trade_count=trade_count,
        )

    # -------------------------------------------------------------------
    # Delay-survival family
    # -------------------------------------------------------------------

    def _compute_delay_survival(
        self, inputs: DelaySurvivalInputs,
    ) -> DelaySurvivalMetrics:
        if inputs.delay_seconds is None:
            return DelaySurvivalMetrics(
                availability=MetricAvailability.UNAVAILABLE_NO_EVIDENCE,
                unavailable_reason="no trade timestamps available",
            )

        confidence_sufficient = (
            inputs.reconstruction_confidence != ReconstructionConfidence.LOW
        )

        if not confidence_sufficient:
            return DelaySurvivalMetrics(
                availability=MetricAvailability.UNAVAILABLE_LOW_CONFIDENCE,
                unavailable_reason="reconstruction confidence is LOW",
                delay_seconds=inputs.delay_seconds,
                position_still_open=inputs.position_still_open,
                reconstruction_confidence=inputs.reconstruction_confidence,
                has_mismatch=inputs.has_mismatch,
                confidence_sufficient=False,
            )

        return DelaySurvivalMetrics(
            availability=MetricAvailability.AVAILABLE,
            unavailable_reason=None,
            delay_seconds=inputs.delay_seconds,
            position_still_open=inputs.position_still_open,
            reconstruction_confidence=inputs.reconstruction_confidence,
            has_mismatch=inputs.has_mismatch,
            confidence_sufficient=True,
        )

    # -------------------------------------------------------------------
    # Slippage / copyability family
    # -------------------------------------------------------------------

    def _compute_slippage_copyability(
        self, inputs: SlippageCopyabilityInputs,
    ) -> SlippageCopyabilityMetrics:
        if not inputs.has_orderbook_snapshot:
            return SlippageCopyabilityMetrics(
                availability=MetricAvailability.UNAVAILABLE_NO_EVIDENCE,
                unavailable_reason="no orderbook snapshot",
            )

        # Compute spread in basis points when mid_price is available
        spread_bps: int | None = None
        if inputs.spread is not None and inputs.mid_price is not None:
            try:
                if inputs.mid_price > 0:
                    bps_raw = (inputs.spread / inputs.mid_price) * 10_000
                    spread_bps = int(bps_raw)
            except (InvalidOperation, ZeroDivisionError):
                spread_bps = None

        # Check staleness
        snapshot_stale = False
        if inputs.snapshot_age_seconds is not None:
            snapshot_stale = inputs.snapshot_age_seconds > _ORDERBOOK_STALE_SECONDS

        availability = MetricAvailability.AVAILABLE
        unavailable_reason = None
        if snapshot_stale:
            availability = MetricAvailability.UNAVAILABLE_STALE_DATA
            unavailable_reason = (
                f"orderbook snapshot is {inputs.snapshot_age_seconds}s old "
                f"(threshold: {_ORDERBOOK_STALE_SECONDS}s)"
            )

        return SlippageCopyabilityMetrics(
            availability=availability,
            unavailable_reason=unavailable_reason,
            spread_decimal=inputs.spread,
            spread_bps=spread_bps,
            mid_price=inputs.mid_price,
            book_depth_available=inputs.book_depth_available,
            snapshot_age_seconds=inputs.snapshot_age_seconds,
            snapshot_stale=snapshot_stale,
        )
