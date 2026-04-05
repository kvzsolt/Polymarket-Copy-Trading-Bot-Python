"""Typed metric-family output models for the scoring pipeline.

These are produced by metric calculators that consume ``RawFeatureRecord``
instances.  Each family carries its own availability / confidence metadata
so that downstream composite scoring can gate on data quality honestly.

Hard rules:
- Never fabricate a metric when evidence is absent.
- ``available`` is False when the underlying data is missing — the metric
  value is None, and ``unavailable_reason`` explains why.
- No composite / cross-family scores live here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from domain.discovery_models import ReconstructionConfidence
from domain.scoring_models import PositionKey


# ---------------------------------------------------------------------------
# Shared confidence / availability enum
# ---------------------------------------------------------------------------

class MetricAvailability(Enum):
    """Whether a metric could be computed from existing evidence."""
    AVAILABLE = "available"
    UNAVAILABLE_NO_EVIDENCE = "unavailable_no_evidence"
    UNAVAILABLE_LOW_CONFIDENCE = "unavailable_low_confidence"
    UNAVAILABLE_STALE_DATA = "unavailable_stale_data"


# ---------------------------------------------------------------------------
# Entry-alpha metric family
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EntryAlphaWindowMetric:
    """Metric for a single entry-alpha time window (e.g. 5m, 30m, 4h).

    ``vwap_entry_price`` is the volume-weighted average entry price for
    BUY trades in that window.  ``trade_count`` is how many BUY trades
    fell inside the window.

    When the window is unavailable (no qualifying trades), both
    ``vwap_entry_price`` and ``trade_count`` are None/0 and
    ``availability`` explains why.
    """
    window_label: str
    availability: MetricAvailability
    unavailable_reason: str | None = None

    vwap_entry_price: Decimal | None = None
    trade_count: int = 0


@dataclass(frozen=True)
class EntryAlphaMetrics:
    """Entry-alpha family: per-window metrics derived from trade evidence."""
    window_5m: EntryAlphaWindowMetric = field(
        default_factory=lambda: EntryAlphaWindowMetric(
            window_label="5m",
            availability=MetricAvailability.UNAVAILABLE_NO_EVIDENCE,
            unavailable_reason="no raw feature data",
        )
    )
    window_30m: EntryAlphaWindowMetric = field(
        default_factory=lambda: EntryAlphaWindowMetric(
            window_label="30m",
            availability=MetricAvailability.UNAVAILABLE_NO_EVIDENCE,
            unavailable_reason="no raw feature data",
        )
    )
    window_4h: EntryAlphaWindowMetric = field(
        default_factory=lambda: EntryAlphaWindowMetric(
            window_label="4h",
            availability=MetricAvailability.UNAVAILABLE_NO_EVIDENCE,
            unavailable_reason="no raw feature data",
        )
    )

    @property
    def any_window_available(self) -> bool:
        return any(
            w.availability == MetricAvailability.AVAILABLE
            for w in (self.window_5m, self.window_30m, self.window_4h)
        )


# ---------------------------------------------------------------------------
# Delay-survival metric family
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DelaySurvivalMetrics:
    """Delay-survival family: scaffolding derived from available raw evidence.

    ``delay_seconds`` is None when no trade timestamps are available.
    ``position_still_open`` is meaningful only when the underlying
    reconstruction has sufficient confidence.
    """
    availability: MetricAvailability = MetricAvailability.UNAVAILABLE_NO_EVIDENCE
    unavailable_reason: str | None = "no raw feature data"

    delay_seconds: int | None = None
    position_still_open: bool | None = None
    reconstruction_confidence: ReconstructionConfidence = ReconstructionConfidence.LOW
    has_mismatch: bool = False
    confidence_sufficient: bool = False


# ---------------------------------------------------------------------------
# Slippage / copyability metric family
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SlippageCopyabilityMetrics:
    """Slippage / copyability evidence-derived metrics.

    Only populated when an orderbook snapshot actually exists.
    ``spread_bps`` is the observed spread in basis points.
    ``book_depth_available`` indicates whether the book had non-empty levels.

    We do NOT simulate executable fills from midpoint — that would be fantasy.
    """
    availability: MetricAvailability = MetricAvailability.UNAVAILABLE_NO_EVIDENCE
    unavailable_reason: str | None = "no orderbook snapshot"

    spread_decimal: Decimal | None = None
    spread_bps: int | None = None
    mid_price: Decimal | None = None
    book_depth_available: bool = False
    snapshot_age_seconds: int | None = None
    snapshot_stale: bool = False

    # Staleness threshold: snapshots older than 5 minutes are flagged
    STALENESS_THRESHOLD_SECONDS: int = field(default=300, repr=False)


# ---------------------------------------------------------------------------
# Top-level metric family record
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MetricFamilyRecord:
    """Aggregated metric-family outputs for one position (keyed by PositionKey).

    Produced by ``MetricFamilyCalculator`` from a ``RawFeatureRecord``.
    Each family is independently available/unavailable.  No composite
    score is computed here — that is a future slice.
    """
    trader_wallet: str
    condition_id: str
    token_id: str

    entry_alpha: EntryAlphaMetrics = field(default_factory=EntryAlphaMetrics)
    delay_survival: DelaySurvivalMetrics = field(default_factory=DelaySurvivalMetrics)
    slippage_copyability: SlippageCopyabilityMetrics = field(
        default_factory=SlippageCopyabilityMetrics
    )

    @property
    def key(self) -> PositionKey:
        return (self.trader_wallet, self.condition_id, self.token_id)
