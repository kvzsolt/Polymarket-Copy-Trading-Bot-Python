"""Position domain types — a single open position held by the user."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Position:
    """A single open position held by the user."""

    user_id: str
    condition_id: str
    token_id: str
    leader_id: str | None
    size_tokens: Decimal
    avg_entry_price: Decimal
    current_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None

    @property
    def notional_value(self) -> Decimal:
        price = self.current_price or self.avg_entry_price
        return self.size_tokens * price
