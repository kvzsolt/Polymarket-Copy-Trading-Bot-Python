"""Define immutable dependencies for a follower trading runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from config.app_config import AppConfig
from interfaces.account_inventory import PositionSizeReader
from services.balance_regime import BalanceRegime
from services.copyDecisionService import CopyDecisionService
from services.copyStrategy import CopySizingResult, calculate_copy_notional
from services.entryPermissionService import EntryPermissionService
from services.healthServer import ReadinessTracker
from services.leaderScoringService import LeaderScoringService
from services.leader_performance import LeaderPerformanceReader
from services.copy_tasks.protective_runtime import TaskProtectivePositionManager
from services.positionCoordinator import PositionCoordinator
from services.portfolio_valuation import MarketQuoteSource
from services.telemetry import TelemetryEmitters

if TYPE_CHECKING:
    from orchestrators.market_data_runtime import MarketDataRuntime
    from services.execution.coordinator import ExecutionCoordinator
    from services.market_data_service import MarketDataService
    from services.position_drift_service import PositionDriftService


@dataclass
class TradingContext:
    """Hold the injected services for one follower trading runtime."""

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
    rank_size_multiplier: Decimal = Decimal("1")
    leader_scorer: LeaderScoringService | None = None
    copy_decision_service: CopyDecisionService | None = None
    readiness: ReadinessTracker = field(default_factory=ReadinessTracker)
    # A04 — account-owned coordination state (per-scope lock map + intent
    # registry). Production wiring ALWAYS passes the shared per-account
    # instance built once in ``orchestrators.runtime_bootstrap.build_user_state``
    # so every leader binding, the risk context, and the ExecutionCoordinator
    # for one follower account serialize on ONE lock map and ONE intent
    # registry. The default factory exists ONLY for direct/test construction
    # of TradingContext outside the bootstrap; nothing in production may rely
    # on it, and ``build_trading_context`` never re-points it after
    # construction.
    position_coordinator: PositionCoordinator = field(default_factory=PositionCoordinator)
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
    # trade pipeline and risk monitor route through this so
    # ReservationService and the PositionActionCoordinator apply universally.
    # See PR6 plan in `.claude/plans/replicated-leaping-breeze.md`.
    execution_coordinator: "ExecutionCoordinator | None" = None
    # PR7 — proactive manual/off-bot position drift detection. Coordinator
    # checks ``is_scope_paused`` before every production submit.
    position_drift_service: "PositionDriftService | None" = None
    # Phase 4 — balance regime gate (per-position cap, daily spend ceiling,
    # min headroom floor). Built from ``config.execution`` when ``None``.
    balance_regime: BalanceRegime | None = None
    # Part C — operator telemetry emitters, injected by ``build_user_state``.
    # The default is the disabled null-object bundle so every existing
    # construction (tests, tools, paper harnesses) keeps working and no call
    # site has to branch on whether telemetry is running.
    telemetry: TelemetryEmitters = field(default_factory=TelemetryEmitters.disabled)
    leader_performance: LeaderPerformanceReader | None = None
    task_position_manager: TaskProtectivePositionManager | None = None
    market_quotes: MarketQuoteSource | None = None
    holdings_reader: PositionSizeReader | None = None

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
