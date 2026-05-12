from dataclasses import dataclass
from decimal import Decimal


@dataclass
class TokenMarketState:
    token_id: str
    condition_id: str | None
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    best_bid: Decimal | None
    best_ask: Decimal | None
    tick_size: Decimal | None
    min_order_size: Decimal | None
    active: bool
    resolved: bool
    dirty: bool

@dataclass(frozen=True)
class MarketSnapshot:
    token_id: str
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]
    best_bid: Decimal | None
    best_ask: Decimal | None
    tick_size: Decimal | None
    min_order_size: Decimal | None