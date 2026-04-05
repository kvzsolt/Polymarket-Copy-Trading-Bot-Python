"""Domain models for the leader discovery subsystem (Phases 1-4).

All models use Decimal for monetary values and frozen dataclasses for
immutable value objects.  Mutable state carriers use regular dataclasses.
"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


# ---------------------------------------------------------------------------
# Phase 1 — Data Foundation
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TraderRecord:
    """A trader wallet observed in the wild."""
    wallet: str
    first_seen_ts: int
    last_seen_ts: int
    trade_count: int = 0
    source: str = "activity_api"
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketMeta:
    """Minimal market metadata keyed by condition_id."""
    condition_id: str
    question: str = ""
    slug: str = ""
    event_slug: str = ""
    category: str = ""
    end_date_ts: int | None = None      # resolution deadline (epoch seconds)
    active: bool = True
    resolved: bool = False
    neg_risk: bool = False
    yes_token_id: str = ""
    no_token_id: str = ""
    tokens_json: str = "[]"              # serialised token list from API
    meta_json: str = "{}"                # raw API payload for extensibility


@dataclass(frozen=True)
class ObservedTrade:
    """A single trade observed via the Polymarket data API.

    This is the immutable ledger record — we never mutate it once stored.
    """
    trader_wallet: str
    condition_id: str
    token_id: str
    side: str                            # "BUY" | "SELL"
    size: Decimal                        # token quantity
    price: Decimal                       # fill price (0–1)
    usdc_size: Decimal                   # notional USDC
    tx_hash: str
    event_ts: int                        # epoch seconds
    trade_id: str = ""                   # API-assigned ID if available
    fee: Decimal = Decimal("0")
    outcome: str = ""
    title: str = ""
    market_slug: str = ""
    event_slug: str = ""
    raw_json: str = "{}"
    source: str = "activity_api"


@dataclass(frozen=True)
class PositionSnapshotRecord:
    """Point-in-time snapshot of a trader's position from the positions API."""
    trader_wallet: str
    condition_id: str
    token_id: str
    size: Decimal
    avg_price: Decimal
    snapshot_ts: int
    current_value: Decimal = Decimal("0")
    current_price: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    raw_json: str = "{}"


@dataclass(frozen=True)
class ClosedPositionSnapshot:
    """A position that has been fully exited or resolved."""
    trader_wallet: str
    condition_id: str
    token_id: str
    entry_size: Decimal
    entry_avg_price: Decimal
    exit_avg_price: Decimal
    realized_pnl: Decimal
    closed_ts: int
    close_reason: str = "trade"          # "trade" | "resolution"
    raw_json: str = "{}"


@dataclass(frozen=True)
class OrderbookSnapshot:
    """A point-in-time L2 orderbook snapshot captured near a leader trade."""
    token_id: str
    condition_id: str
    snapshot_ts: int                     # capture timestamp in ms
    trader_wallet: str = ""
    reference_tx_hash: str = ""
    reference_trade_id: str = ""
    reference_event_ts: int = 0
    bids_json: str = "[]"               # [[price, size], ...]
    asks_json: str = "[]"
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    mid_price: Decimal | None = None
    spread: Decimal | None = None


# ---------------------------------------------------------------------------
# Phase 2 — Reconstruction Engine
# ---------------------------------------------------------------------------

@dataclass
class InventoryLot:
    """A single FIFO lot in the inventory ledger."""
    size: Decimal
    price: Decimal
    ts: int
    tx_hash: str

    @property
    def cost_basis(self) -> Decimal:
        return self.size * self.price


class ReconstructionConfidence(enum.Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class ReconstructionMismatchType(enum.Enum):
    OVERSOLD = "oversold"
    MISSING_HISTORY = "missing_history"
    SNAPSHOT_SIZE_MISMATCH = "snapshot_size_mismatch"
    AVG_COST_MISMATCH = "avg_cost_mismatch"


class ReconstructedPositionStatus(enum.Enum):
    OPEN = "open"
    CLOSED = "closed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class ReconstructionMismatch:
    """Explicit mismatch emitted during reconstruction."""
    mismatch_type: ReconstructionMismatchType
    detail: str
    quantity: Decimal = Decimal("0")
    event_ts: int | None = None
    tx_hash: str = ""
    trade_id: str = ""
    reconstructed_size: Decimal | None = None
    reported_size: Decimal | None = None
    reconstructed_avg_cost: Decimal | None = None
    reported_avg_price: Decimal | None = None


@dataclass
class ReconstructedPosition:
    """Running inventory state for (trader, condition_id, token_id)."""
    trader_wallet: str
    condition_id: str
    token_id: str

    # Current lots (FIFO queue)
    lots: list[InventoryLot] = field(default_factory=list)

    # Accumulators
    total_bought: Decimal = Decimal("0")
    total_sold: Decimal = Decimal("0")
    total_buy_cost: Decimal = Decimal("0")  # sum of (size * price) for buys
    realized_pnl: Decimal = Decimal("0")
    observed_trade_count: int = 0

    status: ReconstructedPositionStatus = ReconstructedPositionStatus.CLOSED
    confidence: ReconstructionConfidence = ReconstructionConfidence.HIGH
    mismatches: list[ReconstructionMismatch] = field(default_factory=list)

    @property
    def current_size(self) -> Decimal:
        return sum((lot.size for lot in self.lots), Decimal("0"))

    @property
    def current_cost_basis(self) -> Decimal:
        return sum((lot.cost_basis for lot in self.lots), Decimal("0"))

    @property
    def avg_cost(self) -> Decimal:
        total_size = self.current_size
        if total_size == 0:
            return Decimal("0")
        return self.current_cost_basis / total_size

    @property
    def mismatch_details(self) -> str:
        return "; ".join(m.detail for m in self.mismatches)

    def unrealized_pnl(self, mark_price: Decimal) -> Decimal:
        return self.current_size * (mark_price - self.avg_cost)


# ---------------------------------------------------------------------------
# Phase 3 — Episode Model
# ---------------------------------------------------------------------------

class EpisodeState(enum.Enum):
    OPEN = "open"             # initial entry placed
    ADDING = "adding"         # subsequent buys on same side
    TRIMMING = "trimming"     # partial exits
    CLOSED = "closed"         # fully exited or resolved


@dataclass
class EpisodeEvent:
    """A single event within an episode (maps to an ObservedTrade)."""
    trade_id: str             # FK to ObservedTrade (tx_hash or API id)
    side: str
    size: Decimal
    price: Decimal
    usdc_size: Decimal
    ts: int
    event_type: str = ""      # "entry" | "add" | "trim" | "close"


@dataclass
class Episode:
    """A coherent group of trades on one side of a market."""
    episode_id: str           # deterministic: f"{trader}:{condition_id}:{token_id}:{start_ts}"
    trader_wallet: str
    condition_id: str
    token_id: str

    state: EpisodeState = EpisodeState.OPEN
    direction: str = ""       # "LONG" | "SHORT" (derived from initial entry)

    events: list[EpisodeEvent] = field(default_factory=list)

    entry_size: Decimal = Decimal("0")
    entry_avg_price: Decimal = Decimal("0")
    exit_size: Decimal = Decimal("0")
    exit_avg_price: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")

    opened_ts: int = 0
    closed_ts: int | None = None

    # Phase 4 context
    context_buckets: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Phase 4 — Context Bucketing
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ContextBucket:
    """A single bucket assignment."""
    dimension: str            # e.g. "category", "liquidity", "days_to_resolution", "trade_size"
    bucket: str               # e.g. "politics", "low_liquidity", "7d", "whale"
    raw_value: str = ""       # the underlying value before bucketing


# Bucket definition helpers (thresholds)

TRADE_SIZE_BUCKETS: list[tuple[str, Decimal, Decimal]] = [
    ("micro",  Decimal("0"),   Decimal("10")),
    ("small",  Decimal("10"),  Decimal("100")),
    ("medium", Decimal("100"), Decimal("1000")),
    ("large",  Decimal("1000"), Decimal("10000")),
    ("whale",  Decimal("10000"), Decimal("999999999")),
]

DAYS_TO_RESOLUTION_BUCKETS: list[tuple[str, int, int]] = [
    ("imminent", 0, 1),
    ("short",    1, 7),
    ("medium",   7, 30),
    ("long",     30, 90),
    ("distant",  90, 9999),
]

LIQUIDITY_BUCKETS: list[tuple[str, Decimal, Decimal]] = [
    ("very_low",  Decimal("0"),     Decimal("1000")),
    ("low",       Decimal("1000"),  Decimal("10000")),
    ("medium",    Decimal("10000"), Decimal("100000")),
    ("high",      Decimal("100000"), Decimal("1000000")),
    ("very_high", Decimal("1000000"), Decimal("999999999")),
]
