from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class ComplianceDecision:
    allowed: bool
    reason: str
    source: str
    checked_at: datetime
    expires_at: datetime | None
    raw: dict[str, Any] | None = None


@dataclass(frozen=True)
class TradeSignal:
    leader_id: str
    condition_id: str
    token_id: str
    side: str
    size: Decimal
    price: Decimal
    usdc_size: Decimal
    tx_hash: str
    event_ts: int | None
    title: str = ""
    event_slug: str = ""
    outcome: str = ""
    submarket: str = ""


@dataclass
class PositionSnapshot:
    user_id: str
    condition_id: str
    token_id: str
    leader_id: str | None
    size_tokens: Decimal
    avg_entry_price: Decimal
