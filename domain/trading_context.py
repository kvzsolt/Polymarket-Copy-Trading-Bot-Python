from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from config.env import ENV
from services.copyDecisionService import CopyDecisionService
from services.copyStrategy import CopySizingResult, calculate_copy_notional
from services.healthServer import ReadinessTracker
from services.leaderScoringService import LeaderScoringService


def _default_leader_scorer() -> LeaderScoringService:
    return LeaderScoringService()


def _default_copy_decision() -> CopyDecisionService:
    return CopyDecisionService(leader_scorer=_default_leader_scorer())


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
    leader_scorer: LeaderScoringService = field(default_factory=_default_leader_scorer)
    copy_decision_service: CopyDecisionService = field(default_factory=_default_copy_decision)
    readiness: ReadinessTracker = field(default_factory=ReadinessTracker)
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.copy_decision_service is not None:
            self.copy_decision_service.leader_scorer = self.leader_scorer

    def copy_notional(self, leader_notional: Decimal) -> CopySizingResult:
        """Return the full sizing result so callers can inspect reasoning."""
        return calculate_copy_notional(
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
