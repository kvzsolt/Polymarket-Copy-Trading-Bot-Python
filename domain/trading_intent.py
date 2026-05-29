"""Trading intent types — normalized signals and resolved trading intents."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class NormalizedSignal:
    """A leader trade signal normalized into a canonical form."""

    leader_id: str
    condition_id: str
    token_id: str
    side: str  # "BUY" or "SELL"
    size: Decimal
    price: Decimal
    usdc_size: Decimal
    tx_hash: str
    event_ts: int
    title: str = ""
    event_slug: str = ""
    outcome: str = ""
    submarket: str = ""


@dataclass(frozen=True)
class TradingIntent:
    """A resolved intent to trade, after copy-decision and sizing."""

    user_id: str
    leader_id: str
    condition_id: str
    token_id: str
    side: str
    size: Decimal
    price: Decimal
    notional_usdc: Decimal
    reason: str
    signal_id: str | None = None
    created_at: datetime | None = None
