from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from config.app_config import AppConfig
from services.copyDecisionService import CopyDecisionService
from services.copyStrategy import CopySizingResult, calculate_copy_notional
from services.entryPermissionService import EntryPermissionService
from services.healthServer import ReadinessTracker
from services.leaderScoringService import LeaderScoringService
from services.positionActionCoordinator import PositionActionCoordinator

if TYPE_CHECKING:
    from orchestrators.market_data_runtime import MarketDataRuntime
    from services.execution.coordinator import ExecutionCoordinator
    from services.market_data_service import MarketDataService


@dataclass
class TradingContext:
    user_id: str
    wallet_address: str
    client: Any
    positions_repo: Any
    fills_repo: Any
    orders_repo: Any
    risk_blocks_repo: Any
    pnl_tracker: Any
    risk_engine: Any
    accounting_service: Any
    position_manager: Any
    entry_permission_service: EntryPermissionService | None = None
    execution_service: Any | None = None
    leaders_repo: Any | None = None
    config: AppConfig | None = None
    allocation_pct: float = 0.05
    buy_min_amount: float = 1.0
    buy_max_amount: float = 100.0
    max_market_exposure_usdc: float = 300.0
    leader_scorer: LeaderScoringService | None = None
    copy_decision_service: CopyDecisionService | None = None
    readiness: ReadinessTracker = field(default_factory=ReadinessTracker)
    position_coordinator: PositionActionCoordinator = field(default_factory=PositionActionCoordinator)
    extra: dict[str, Any] = field(default_factory=dict)
    risk_triggers_repo: Any | None = None
    # Freshness-aware market data wiring. ``market_data_service`` is the
    # single execution-time authority for quote/book/tick state; the trade
    # pipeline reads sizing inputs from it, never from MarketStateStore or
    # the deprecated meta-service path. ``market_data_runtime`` owns WSS
    # subscription lifecycle for the user; the pipeline must subscribe new
    # signal tokens before placing orders.
    market_data_service: "MarketDataService | None" = None
    market_data_runtime: "MarketDataRuntime | None" = None
    # Production execution funnel — set by ``build_trading_context``. The
    # trade pipeline, risk monitor, and near-one exit route through this so
    # ReservationService and the PositionActionCoordinator apply universally.
    # See PR6 plan in `.claude/plans/replicated-leaping-breeze.md`.
    execution_coordinator: "ExecutionCoordinator | None" = None

    def __post_init__(self) -> None:
        # Fail fast: these dependencies are required in production contexts.
        # They default to None only to ease unit-test construction, but any
        # production code path that calls trade_pipeline or riskMonitor will
        # crash with a late AttributeError if they are absent.
        _required_for_runtime = {
            "risk_blocks_repo": self.risk_blocks_repo,
            "pnl_tracker": self.pnl_tracker,
            "risk_engine": self.risk_engine,
            "accounting_service": self.accounting_service,
            "position_manager": self.position_manager,
        }
        missing = [k for k, v in _required_for_runtime.items() if v is None]
        if missing and self.config is not None:
            # If a typed config was injected this is a production context —
            # all runtime deps must be present.
            raise TypeError(
                f"TradingContext created with config but missing required "
                f"dependencies: {', '.join(missing)}"
            )

        if isinstance(self.config, AppConfig):
            if self.leader_scorer is None:
                self.leader_scorer = LeaderScoringService(config=self.config)
            if getattr(self.copy_decision_service, "_config", None) is None:
                self.copy_decision_service = CopyDecisionService(
                    config=self.config,
                    leader_scorer=self.leader_scorer,
                )
        if self.copy_decision_service is not None:
            self.copy_decision_service.leader_scorer = self.leader_scorer
        if self.entry_permission_service is None and self.risk_blocks_repo is not None:
            self.entry_permission_service = EntryPermissionService(self.risk_blocks_repo)

    def copy_notional(self, leader_notional: Decimal) -> CopySizingResult:
        """Return the full sizing result so callers can inspect reasoning."""
        if self.config is None:
            raise RuntimeError("TradingContext.copy_notional requires injected AppConfig")

        sizing = self.config.sizing
        return calculate_copy_notional(
            strategy=str(sizing.mode or "proportional"),
            leader_notional_usdc=leader_notional,
            copy_percentage=Decimal(str(self.allocation_pct)) * Decimal("100"),
            fixed_amount=sizing.fixed_amount,
            min_order_usdc=self.buy_min_amount,
            max_order_usdc=self.buy_max_amount,
            trade_multiplier=sizing.trade_multiplier,
            tiered_multipliers=sizing.tiered_trade_multipliers,
            adaptive_basis=str(sizing.adaptive_basis or "trade_size"),
            adaptive_min_percent=sizing.adaptive_min_percent,
            adaptive_max_percent=sizing.adaptive_max_percent,
            adaptive_threshold_usdc=sizing.adaptive_threshold_usdc,
        )
