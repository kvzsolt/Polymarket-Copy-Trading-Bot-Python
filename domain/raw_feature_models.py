"""Typed raw-feature records for the scoring pipeline.

These models are consumed by future scoring / ranking layers.  They carry
only raw inputs and honest availability flags — no composite scores.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from domain.discovery_models import (
    ReconstructionConfidence,
    ReconstructedPositionStatus,
)
from domain.scoring_models import PositionKey


# ---------------------------------------------------------------------------
# Sub-record: Entry-alpha input windows
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EntryAlphaInputs:
    """Raw inputs for entry-alpha evaluation across time windows.

    Each ``*_available`` flag is True only when the corresponding trade
    timestamp and reference price are genuinely present in storage.
    Values are left as None when the underlying data is absent — the
    joiner never fabricates them.
    """

    last_trade_ts: int | None = None
    reference_ts: int | None = None

    # 5-minute window
    window_5m_available: bool = False
    window_5m_entry_price: Decimal | None = None
    window_5m_trade_count: int = 0

    # 30-minute window
    window_30m_available: bool = False
    window_30m_entry_price: Decimal | None = None
    window_30m_trade_count: int = 0

    # 4-hour window
    window_4h_available: bool = False
    window_4h_entry_price: Decimal | None = None
    window_4h_trade_count: int = 0


# ---------------------------------------------------------------------------
# Sub-record: Delay-survival inputs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DelaySurvivalInputs:
    """Raw inputs for copy-delay survival estimation.

    ``delay_seconds`` is the gap between the leader's last observed trade
    and the reference timestamp (now).  It is None when either timestamp
    is missing.
    """

    delay_seconds: int | None = None
    position_still_open: bool = False
    reconstruction_confidence: ReconstructionConfidence = ReconstructionConfidence.HIGH
    has_mismatch: bool = False


# ---------------------------------------------------------------------------
# Sub-record: Slippage / copyability evidence
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SlippageCopyabilityInputs:
    """Raw orderbook evidence that future slippage / copyability scoring
    will need.  All fields are None / False when no orderbook snapshot
    is available — the joiner never invents a book.
    """

    has_orderbook_snapshot: bool = False
    snapshot_ts: int | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    mid_price: Decimal | None = None
    spread: Decimal | None = None
    book_depth_available: bool = False
    snapshot_age_seconds: int | None = None


# ---------------------------------------------------------------------------
# Sub-record: Evidence presence flags
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EvidencePresenceFlags:
    """Boolean presence flags summarising what data the pipeline has."""

    has_position_snapshot: bool = False
    has_market_meta: bool = False
    has_orderbook_snapshot: bool = False
    has_trade_history: bool = False
    has_price_history: bool = False       # always False for now — no price table
    trade_count: int = 0
    orderbook_snapshot_count: int = 0


# ---------------------------------------------------------------------------
# Top-level raw feature record
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RawFeatureRecord:
    """Typed raw feature record keyed by PositionKey.

    Carries only raw inputs and availability flags.  Composite scores
    are computed by a downstream scoring layer — NOT here.
    """

    trader_wallet: str
    condition_id: str
    token_id: str

    # Position fundamentals (passed through from ScoringInputRecord)
    current_size: Decimal
    avg_cost: Decimal
    realized_pnl: Decimal
    position_status: ReconstructedPositionStatus
    reconstruction_confidence: ReconstructionConfidence

    # Feature sub-records
    entry_alpha: EntryAlphaInputs = field(default_factory=EntryAlphaInputs)
    delay_survival: DelaySurvivalInputs = field(default_factory=DelaySurvivalInputs)
    slippage_copyability: SlippageCopyabilityInputs = field(
        default_factory=SlippageCopyabilityInputs
    )
    evidence_flags: EvidencePresenceFlags = field(
        default_factory=EvidencePresenceFlags
    )

    @property
    def key(self) -> PositionKey:
        return (self.trader_wallet, self.condition_id, self.token_id)
