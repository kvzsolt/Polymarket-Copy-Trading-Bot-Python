"""Define explicit typed dependencies for a follower trading runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pmcopy.application.runtime_state import (
    AccountExecutionRuntimeState,
    AccountRiskRuntimeState,
    ContextExtraBridge,
)
from pmcopy.config.app_config import AppConfig
from pmcopy.interfaces.account_inventory import PositionSizeReader
from pmcopy.services.balance_regime import BalanceRegime
from pmcopy.services.copy_tasks.protective_runtime import TaskProtectivePositionManager
from pmcopy.services.copyDecisionService import CopyDecisionService
from pmcopy.services.copyStrategy import CopySizingResult, calculate_copy_notional
from pmcopy.services.entryPermissionService import EntryPermissionService
from pmcopy.services.healthServer import ReadinessTracker
from pmcopy.services.leader_performance import LeaderPerformanceReader
from pmcopy.services.leaderScoringService import LeaderScoringService
from pmcopy.services.portfolio_valuation import MarketQuoteSource
from pmcopy.services.positionCoordinator import PositionCoordinator
from pmcopy.services.telemetry import TelemetryEmitters

if TYPE_CHECKING:
    from pmcopy.interfaces.clob_gateway import ClobGateway, PriceQuoteGateway
    from pmcopy.orchestrators.market_data_runtime import MarketDataRuntime
    from pmcopy.ports.repositories import (
        AbstractFillRepository,
        AbstractLeaderRepository,
        AbstractOrderRepository,
        AbstractPositionRepository,
        AbstractReservationRepository,
    )
    from pmcopy.services.accountingService import AccountingService
    from pmcopy.services.execution.coordinator import ExecutionCoordinator
    from pmcopy.services.execution.reservation_service import ReservationService
    from pmcopy.services.executionService import ExecutionService
    from pmcopy.services.market_data_service import MarketDataService
    from pmcopy.services.onchain_detection.leader_sell_scope_guard import (
        LeaderSellBuyScopeGuard,
    )
    from pmcopy.services.pnlTracker import PnLTracker
    from pmcopy.services.position_drift_service import PositionDriftService
    from pmcopy.services.positionManager import PositionManager
    from pmcopy.services.risk.risk_engine import RiskEngine


@dataclass
class TradingContext:
    """Hold the injected services for one follower trading runtime."""

    user_id: str
    wallet_address: str
    client: ClobGateway | PriceQuoteGateway | Any
    positions_repo: AbstractPositionRepository | Any
    fills_repo: AbstractFillRepository | Any
    orders_repo: AbstractOrderRepository | Any
    risk_blocks_repo: Any
    pnl_tracker: PnLTracker | Any
    risk_engine: RiskEngine | Any
    accounting_service: AccountingService | Any
    position_manager: PositionManager | Any
    entry_permission_service: EntryPermissionService | None = None
    execution_service: ExecutionService | Any | None = None
    leaders_repo: AbstractLeaderRepository | Any | None = None
    config: AppConfig | None = None
    # A06 — exact-decimal money: multi-user follows inject ``Decimal`` values
    # from the strict config loader (see services/multiUserConfig); legacy
    # single-account and test paths still pass floats. Every downstream read
    # converts through ``Decimal(str(...))`` (copy_notional below) or
    # ``copyStrategy._to_decimal``, both of which are exact for either input,
    # so sizing math stays exact end-to-end.
    allocation_pct: float | Decimal = 0.05
    buy_min_amount: float = 1.0
    buy_max_amount: float | Decimal = 100.0
    max_market_exposure_usdc: float | Decimal = 300.0
    # Task 24 fix — True iff ``max_market_exposure_usdc`` came from an
    # explicitly configured copier-level open-exposure cap (schema v2
    # ``limits.max_open_exposure_quote`` / v1 explicit follow field). Only
    # then does the trade pipeline enforce the cap; the default value stays
    # informational exactly as before.
    market_exposure_cap_explicit: bool = False
    rank_size_multiplier: Decimal = Decimal("1")
    leader_scorer: LeaderScoringService | None = None
    copy_decision_service: CopyDecisionService | None = None
    readiness: ReadinessTracker = field(default_factory=ReadinessTracker)
    # A04 — account-owned coordination state (per-scope lock map + intent
    # registry). Production wiring ALWAYS passes the shared per-account
    # instance built once in ``orchestrators.runtime_bootstrap.build_user_state``
    # so every leader binding, the risk context, and the ExecutionCoordinator
    # for one follower account serialize on ONE lock map and ONE intent
    # registry.
    position_coordinator: PositionCoordinator = field(default_factory=PositionCoordinator)
    extra: dict[str, Any] = field(default_factory=dict)
    risk_triggers_repo: Any | None = None
    market_data_service: MarketDataService | None = None
    market_data_runtime: MarketDataRuntime | None = None
    execution_coordinator: ExecutionCoordinator | Any | None = None
    position_drift_service: PositionDriftService | Any | None = None
    balance_regime: BalanceRegime | None = None
    telemetry: TelemetryEmitters = field(default_factory=TelemetryEmitters.disabled)
    leader_performance: LeaderPerformanceReader | None = None
    task_position_manager: TaskProtectivePositionManager | None = None
    market_quotes: MarketQuoteSource | None = None
    holdings_reader: PositionSizeReader | None = None
    # WP-C — explicit typed runtime bindings & account-scoped state owners
    # replacing untyped ``context.extra`` keys in production code.
    leader_wallet: str = ""
    slippage_tolerance_bps: int = 200
    clob_gateway: ClobGateway | None = None
    accounted_fill_reader: Any | None = None
    reservation_service: ReservationService | Any | None = None
    reservations_repo: AbstractReservationRepository | Any | None = None
    leader_sell_target_service: Any | None = None
    leader_sell_scope_guard: LeaderSellBuyScopeGuard | None = None
    task_budget_settlement: Any | None = None
    audit_logger: Any | None = None
    account_execution_state: AccountExecutionRuntimeState = field(
        default_factory=AccountExecutionRuntimeState
    )
    risk_state: AccountRiskRuntimeState = field(default_factory=AccountRiskRuntimeState)

    def __post_init__(self) -> None:
        # Fail fast: these dependencies are required in production contexts.
        _required_for_runtime = {
            "risk_blocks_repo": self.risk_blocks_repo,
            "pnl_tracker": self.pnl_tracker,
            "risk_engine": self.risk_engine,
            "accounting_service": self.accounting_service,
            "position_manager": self.position_manager,
        }
        missing = [k for k, v in _required_for_runtime.items() if v is None]
        if missing and self.config is not None:
            raise TypeError(
                f"TradingContext created with config but missing required "
                f"dependencies: {', '.join(missing)}"
            )

        if not isinstance(self.account_execution_state, AccountExecutionRuntimeState):
            self.account_execution_state = AccountExecutionRuntimeState()
        if not isinstance(self.risk_state, AccountRiskRuntimeState):
            self.risk_state = AccountRiskRuntimeState()
        if not isinstance(self.extra, ContextExtraBridge):
            initial_extra = self.extra if isinstance(self.extra, dict) else {}
            self.extra = ContextExtraBridge(self, initial_extra)

        if isinstance(self.config, AppConfig):
            if self.leader_scorer is None:
                self.leader_scorer = LeaderScoringService(config=self.config)
            if getattr(self.copy_decision_service, "_config", None) is None:
                self.copy_decision_service = CopyDecisionService(
                    config=self.config,
                    leader_scorer=self.leader_scorer,
                )
        if self.copy_decision_service is not None and self.leader_scorer is not None:
            self.copy_decision_service.leader_scorer = self.leader_scorer
        if self.entry_permission_service is None and self.risk_blocks_repo is not None:
            self.entry_permission_service = EntryPermissionService(self.risk_blocks_repo)
        if self.balance_regime is None and isinstance(self.config, AppConfig):
            self.balance_regime = BalanceRegime.from_config(self.config.execution)

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
            rank_multiplier=self.rank_size_multiplier,
        )
