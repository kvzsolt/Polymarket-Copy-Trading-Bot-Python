from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from config.env import ENV
from services.copyStrategy import calculate_copy_notional


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
    leaders_repo: Any | None = None
    allocation_pct: float = 0.05
    buy_min_amount: float = 1.0
    buy_max_amount: float = 100.0
    max_market_exposure_usdc: float = 300.0
    extra: dict[str, Any] = field(default_factory=dict)

    def copy_notional(self, leader_notional: Decimal) -> Decimal:
        result = calculate_copy_notional(
            strategy=str(ENV.get("SIZING_MODE", "proportional") or "proportional"),
            leader_notional_usdc=leader_notional,
            copy_percentage=Decimal(str(self.allocation_pct)) * Decimal("100"),
            fixed_amount=ENV.get("FIXED_AMOUNT", self.buy_min_amount),
            min_order_usdc=self.buy_min_amount,
            max_order_usdc=self.buy_max_amount,
            trade_multiplier=ENV.get("TRADE_MULTIPLIER", 1.0),
            tiered_multipliers=ENV.get("TIERED_TRADE_MULTIPLIERS", ""),
            adaptive_basis=str(ENV.get("ADAPTIVE_SIZING_BASIS", "trade_size") or "trade_size"),
            adaptive_min_percent=ENV.get("ADAPTIVE_MIN_PERCENT", 0),
            adaptive_max_percent=ENV.get("ADAPTIVE_MAX_PERCENT", 0),
            adaptive_threshold_usdc=ENV.get("ADAPTIVE_THRESHOLD_USDC", 500),
        )
        return result.final_notional_usdc
