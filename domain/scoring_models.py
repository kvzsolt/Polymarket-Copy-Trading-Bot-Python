"""Typed scoring-input models derived from discovery reconstruction."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from domain.discovery_models import (
    ContextBucket,
    ReconstructionConfidence,
    ReconstructedPositionStatus,
)

PositionKey = tuple[str, str, str]


@dataclass(frozen=True)
class ScoringInputEvidence:
    """Direct evidence attached to a scoring input record."""

    last_observed_trade_ts: int | None = None
    latest_snapshot_ts: int | None = None
    snapshot_size: Decimal | None = None
    snapshot_avg_price: Decimal | None = None
    has_snapshot: bool = False


@dataclass(frozen=True)
class ScoringInputMarketContext:
    """Minimal market metadata and context buckets for future scoring families."""

    question: str = ""
    market_slug: str = ""
    event_slug: str = ""
    category: str = ""
    end_date_ts: int | None = None
    active: bool | None = None
    resolved: bool | None = None
    neg_risk: bool | None = None
    buckets: tuple[ContextBucket, ...] = ()
    reference_ts: int | None = None
    has_market_meta: bool = False


@dataclass(frozen=True)
class ScoringInputRecord:
    """Stable, read-only record keyed by (trader, condition_id, token_id)."""

    trader_wallet: str
    condition_id: str
    token_id: str
    current_size: Decimal
    avg_cost: Decimal
    current_cost_basis: Decimal
    realized_pnl: Decimal
    observed_trade_count: int
    position_status: ReconstructedPositionStatus
    reconstruction_confidence: ReconstructionConfidence
    mismatch_types: tuple[str, ...] = ()
    mismatch_details: tuple[str, ...] = ()
    evidence: ScoringInputEvidence = field(default_factory=ScoringInputEvidence)
    market_context: ScoringInputMarketContext = field(
        default_factory=ScoringInputMarketContext
    )

    @property
    def key(self) -> PositionKey:
        return (self.trader_wallet, self.condition_id, self.token_id)
