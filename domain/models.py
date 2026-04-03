from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


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
    event_ts: int
    title: str = ""
    event_slug: str = ""


@dataclass
class PositionSnapshot:
    user_id: str
    condition_id: str
    token_id: str
    leader_id: str | None
    size_tokens: Decimal
    avg_entry_price: Decimal
