from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass
class TradingContext:
    user_id: str
    wallet_address: str
    client: Any
    positions_repo: Any
    fills_repo: Any
    orders_repo: Any
    pnl_tracker: Any
    risk_engine: Any
    accounting_service: Any
    position_manager: Any
    execution_service: Any | None = None
    allocation_pct: float = 0.05
    buy_min_amount: float = 1.0
    buy_max_amount: float = 100.0
    max_market_exposure_usdc: float = 300.0
    extra: dict[str, Any] = field(default_factory=dict)

    def copy_notional(self, leader_notional: Decimal) -> Decimal:
        raw = leader_notional * Decimal(str(self.allocation_pct))
        clamped = max(Decimal(str(self.buy_min_amount)), min(raw, Decimal(str(self.buy_max_amount))))
        return max(Decimal("1"), clamped)
