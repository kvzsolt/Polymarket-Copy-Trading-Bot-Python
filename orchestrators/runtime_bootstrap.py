from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from domain.trading_context import TradingContext
from interfaces.clob_gateway import ClobGateway
from cache.market_state_store import MarketStateStore
from services.accountingService import AccountingService
from services.executionService import ExecutionService
from services.feeService import FeeService
from services.market_data_service import MarketDataService, MarketDataPolicy
from services.multiUserConfig import RuntimeUser, UserFollowConfig
from services.orderRepository import OrderRepository
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.riskEngine import RiskEngine, init_risk_engine
from services.healthServer import ReadinessTracker
from services.entryPermissionService import EntryPermissionService
from services.riskMonitor import rehydrate_position_manager, reconcile_unconfirmed_risk_orders
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo
from storage.sqlite.risk_triggers_repo import RiskTriggersRepo
from utils.fetchData import fetch_data
from utils.logger import get_logger

if TYPE_CHECKING:
    from config.app_config import AppConfig

logger = get_logger("RuntimeBootstrap")


async def startup_reconcile_unconfirmed(
    contexts: list[TradingContext],
) -> dict[str, int]:
    """Reconcile all submitted_unconfirmed orders before starting runtime loops.

    Must run AFTER build_context_maps() and BEFORE risk_monitor / trade_monitor
    tasks.  This closes the non-deterministic restart window where:

    * BUY fills that landed on-chain during shutdown are invisible to the
      risk monitor (position never created locally).
    * SELL fills that landed during shutdown leave stale positions that the
      risk monitor or next trade cycle could act on.

    After reconciliation, the in-memory PositionManager is re-rehydrated so
    all downstream consumers observe converged state.

    Returns a dict mapping user_id → number of orders reconciled.
    """
    from orchestrators.trade_pipeline import reconcile_submitted_unconfirmed

    reconciled_by_user: dict[str, int] = {}
    seen_users: set[str] = set()

    for context in contexts:
        if context.user_id in seen_users:
            continue
        seen_users.add(context.user_id)

        if context.execution_service is None:
            continue

        try:
            trade_n = await reconcile_submitted_unconfirmed(context)
            risk_n = await reconcile_unconfirmed_risk_orders(context)
            total_n = trade_n + risk_n
            if total_n > 0:
                # Position state changed — re-rehydrate so the risk monitor
                # and trade loops observe the converged snapshot.
                rehydrate_position_manager(context)
                reconciled_by_user[context.user_id] = total_n
                logger.info(
                    "startup_reconcile_complete | user=%s reconciled=%d (trade=%d risk=%d)",
                    context.user_id,
                    total_n,
                    trade_n,
                    risk_n,
                )
        except Exception as exc:
            logger.warning(
                "startup_reconcile_error | user=%s error=%s",
                context.user_id,
                repr(exc),
            )

    if reconciled_by_user:
        total = sum(reconciled_by_user.values())
        logger.info(
            "startup_reconcile_summary | users=%d total_reconciled=%d",
            len(reconciled_by_user),
            total,
        )
    else:
        logger.info("startup_reconcile_summary | no pending orders to reconcile")

    return reconciled_by_user


@dataclass
class UserRuntimeState:
    user: RuntimeUser
    client: object
    orders_repo: OrdersRepo
    fills_repo: FillsRepo
    positions_repo: PositionsRepo
    risk_blocks_repo: RiskBlocksRepo
    pnl_tracker: PnLTracker
    risk_engine: RiskEngine
    accounting_service: AccountingService
    position_manager: PositionManager
    execution_service: ExecutionService
    # CLOB V2 gateway — single instance per user runtime, reused by the
    # reconciler / execution coordinator when those wires land (see issue #66
    # / #71). Optional default keeps existing test fixtures that build state
    # without a venue boundary working.
    clob_gateway: ClobGateway | None = None
    risk_triggers_repo: RiskTriggersRepo | None = None
    # Freshness-aware market data — issue #hardening PR-1.
    market_data_service: MarketDataService | None = None
    market_store: MarketStateStore | None = None
    # PR6 — production execution funnel.
    execution_coordinator: "object | None" = None
    # PR7 — manual / off-bot position drift sweep.
    position_drift_service: "object | None" = None


def configured_leaders(config: AppConfig) -> list[str]:
    leaders = [str(wallet).strip() for wallet in config.leader.wallets if str(wallet).strip()]
    if not leaders:
        if config.leader.leader_auto:
            logger.info(
                "No leader wallets configured. LEADER_AUTO is enabled, so the bot "
                f"will auto-discover and follow up to {config.leader.leader_auto_count} leaders."
            )
        else:
            logger.warning(
                "No leader wallets configured. Both LEADER_WALLETS and USER_ADDRESS "
                "are empty. Set LEADER_AUTO=true to auto-discover leaders, or add "
                "manual leader wallets."
            )
    return leaders


def default_follow(user_id: str, leader_id: str, config: AppConfig) -> UserFollowConfig:
    return UserFollowConfig(
        user_id=user_id,
        leader_id=leader_id,
        enabled=True,
        allocation_pct=config.sizing.percentage / 100.0,
        max_trade_usdc=config.sizing.buy_max_amount,
        max_market_exposure_usdc=config.risk.max_market_exposure_usdc,
    )


def ensure_runtime_follows(runtime_users: list[RuntimeUser], leaders: list[str], config: AppConfig) -> None:
    for runtime_user in runtime_users:
        for leader in leaders:
            if leader not in runtime_user.follows:
                runtime_user.follows[leader] = default_follow(runtime_user.user_id, leader, config)


async def fetch_live_positions(wallet: str) -> list[dict]:
    try:
        positions = await fetch_data(
            f"https://data-api.polymarket.com/positions?user={wallet}",
            silent=True,
        )
    except Exception:
        return []

    if not isinstance(positions, list):
        return []

    live_positions: list[dict] = []
    for position in positions:
        try:
            size = float(position.get("size") or 0)
            value = float(position.get("currentValue") or 0)
        except (TypeError, ValueError):
            continue
        if abs(size) <= 1e-12 or value <= 0:
            continue
        live_positions.append(position)
    return live_positions


def build_user_state(
    runtime_user: RuntimeUser,
    client: object,
    db_path: str,
    config: AppConfig,
) -> UserRuntimeState:
    init_risk_engine(config)

    order_repo = OrdersRepo(db_path)
    fills_repo = FillsRepo(db_path)
    positions_repo = PositionsRepo(db_path)
    risk_blocks_repo = RiskBlocksRepo(db_path)
    risk_triggers_repo = RiskTriggersRepo(db_path)
    pnl_tracker = PnLTracker(log_path=f"log/pnl_{runtime_user.user_id}.jsonl")
    risk_engine = RiskEngine()
    accounting_service = AccountingService()
    position_manager = PositionManager(config=config)
    # Single CLOB venue boundary — same instance used by ExecutionService and,
    # when issue #71 lands, the reconciler / execution coordinator. SDK-shaped
    # types stay behind the gateway. See issue #66.
    from integrations.polymarket.clob_v2_gateway import ClobV2Gateway
    clob_gateway = ClobV2Gateway(client, config)

    # Freshness-aware market data layer (issue #hardening PR-1).
    # The store is fed by WSS deltas; the MDS falls back to REST via the
    # gateway when cache is stale/missing. ExecutionService uses MDS so that
    # live execution never runs on stale data.
    market_store = MarketStateStore()
    md_cfg = getattr(config, "market_data", None)
    primary_source = str(getattr(md_cfg, "primary_source", "rest_snapshot")).lower()
    market_data_svc = MarketDataService(
        market_store,
        clob_gateway,
        policy=MarketDataPolicy(
            # In REST primary mode, copy-trade execution gets its quote/book
            # from a fresh REST `/book` snapshot fetched right before sizing.
            # The legacy WSS-fresh gate is intentionally relaxed; the snapshot
            # path enforces its own millisecond budgets and book-quality gates.
            primary_source=primary_source,
            require_wss_fresh=primary_source == "wss",
            allow_rest_fallback=False,
            allow_rest_snapshot_for_execution=primary_source == "rest_snapshot",
            allow_rest_bootstrap_for_execution=bool(
                getattr(md_cfg, "enable_rest_bootstrap", False)
            ),
            rest_bootstrap_max_age_ms=int(
                getattr(md_cfg, "rest_bootstrap_max_age_ms", 500)
            ),
            rest_snapshot_max_age_ms=int(
                getattr(md_cfg, "rest_snapshot_max_age_ms", 500)
            ),
            rest_snapshot_request_timeout_ms=int(
                getattr(md_cfg, "rest_snapshot_request_timeout_ms", 750)
            ),
            rest_snapshot_total_budget_ms=int(
                getattr(md_cfg, "rest_snapshot_total_budget_ms", 1000)
            ),
            rest_snapshot_max_spread_bps=int(
                getattr(md_cfg, "rest_snapshot_max_spread_bps", 300)
            ),
            max_quote_age_seconds=float(
                getattr(md_cfg, "executable_quote_max_age_seconds", 2.0)
            ),
            max_book_age_seconds=float(
                getattr(md_cfg, "executable_book_max_age_seconds", 2.0)
            ),
            max_tick_age_seconds=float(
                getattr(md_cfg, "executable_tick_max_age_seconds", 300.0)
            ),
            wss_connection_max_silence_seconds=float(
                getattr(md_cfg, "wss_connection_max_silence_seconds", 20.0)
            ),
        ),
    )

    execution_service = ExecutionService(
        clob_client=client,
        fee_service=FeeService(client),
        order_repo=OrderRepository(db_path=db_path),
        config=config,
        clob_gateway=clob_gateway,
        market_data_service=market_data_svc,
    )

    # PR6 — production execution funnel. ReservationService + ExecutionCoordinator
    # are wired per user so trade pipeline, risk monitor, and near-one exits
    # all share reservation + collision scope.
    execution_coordinator = _build_execution_coordinator(
        db_path=db_path,
        positions_repo=positions_repo,
        order_repo=order_repo,
        fills_repo=fills_repo,
        execution_service=execution_service,
        funder_address=runtime_user.wallet.funder_address,
        config=config,
    )

    # PR7 — proactive position drift sweep.
    position_drift_service = _build_position_drift_service(
        db_path=db_path,
        positions_repo=positions_repo,
        config=config,
    )

    return UserRuntimeState(
        user=runtime_user,
        client=client,
        orders_repo=order_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
        risk_blocks_repo=risk_blocks_repo,
        pnl_tracker=pnl_tracker,
        risk_engine=risk_engine,
        accounting_service=accounting_service,
        position_manager=position_manager,
        execution_service=execution_service,
        clob_gateway=clob_gateway,
        risk_triggers_repo=risk_triggers_repo,
        market_data_service=market_data_svc,
        market_store=market_store,
        execution_coordinator=execution_coordinator,
        position_drift_service=position_drift_service,
    )


def _build_position_drift_service(
    *,
    db_path: str,
    positions_repo: PositionsRepo,
    config: AppConfig,
):
    """Build the per-user PositionDriftService (PR7).

    Returns None when the service cannot be constructed; runtime then runs
    without a drift sweep (paper / dry-run modes). In live mode the bootstrap
    must ensure this is wired.
    """
    try:
        from services.position_drift_service import PositionDriftService
        from storage.sqlite.reservations_repo import ReservationsRepo
    except Exception as exc:
        logger.warning("position_drift_service_unwired error=%r", exc)
        return None
    reservations_repo = ReservationsRepo(db_path)

    async def _fetcher(wallet: str):
        return await fetch_live_positions(wallet)

    return PositionDriftService(
        positions_repo=positions_repo,
        reservations_repo=reservations_repo,
        live_positions_fetcher=_fetcher,
        config=config.position_drift,
    )


def _build_execution_coordinator(
    *,
    db_path: str,
    positions_repo: PositionsRepo,
    order_repo,
    fills_repo: FillsRepo,
    execution_service: ExecutionService,
    funder_address: str,
    config: "AppConfig | None" = None,
):
    """Build the production ExecutionCoordinator for a user.

    Wraps ReservationService around ExecutionService so every production
    submit path applies reservation + collision semantics. Returns None when
    the funder address is missing so test contexts that build a partial state
    keep working.
    """
    if not funder_address:
        return None
    try:
        from services.execution.coordinator import ExecutionCoordinator
        from services.execution.reservation_service import ReservationService
        from storage.sqlite.reservations_repo import ReservationsRepo
        from utils.getMyBalance import get_my_balance
    except Exception as exc:
        logger.warning("execution_coordinator_unwired error=%r", exc)
        return None
    reservations_repo = ReservationsRepo(db_path)
    reservation_service = ReservationService(
        reservations_repo=reservations_repo,
        positions_repo=positions_repo,
        get_balance_func=get_my_balance,
        funder_address=funder_address,
        ttl_seconds=120,
    )
    coord = ExecutionCoordinator(
        reservation_service=reservation_service,
        orders_repo=order_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
        execution_service=execution_service,
    )
    return coord


def build_trading_context(
    user_state: UserRuntimeState,
    follow: UserFollowConfig,
    config: AppConfig,
    leaders_repo=None,
    readiness: ReadinessTracker | None = None,
) -> TradingContext:
    context = TradingContext(
        user_id=user_state.user.user_id,
        wallet_address=user_state.user.wallet.funder_address,
        client=user_state.client,
        positions_repo=user_state.positions_repo,
        fills_repo=user_state.fills_repo,
        orders_repo=user_state.orders_repo,
        risk_blocks_repo=user_state.risk_blocks_repo,
        pnl_tracker=user_state.pnl_tracker,
        risk_engine=user_state.risk_engine,
        accounting_service=user_state.accounting_service,
        position_manager=user_state.position_manager,
        config=config,
        entry_permission_service=EntryPermissionService(user_state.risk_blocks_repo),
        execution_service=user_state.execution_service,
        leaders_repo=leaders_repo,
        allocation_pct=follow.allocation_pct,
        buy_min_amount=config.sizing.buy_min_amount,
        buy_max_amount=min(config.sizing.buy_max_amount, follow.max_trade_usdc),
        max_market_exposure_usdc=follow.max_market_exposure_usdc,
        readiness=readiness or ReadinessTracker(),
        market_data_service=getattr(user_state, "market_data_service", None),
        extra={
            "slippage_tolerance_bps": int(config.execution.slippage_tolerance * 100),
            "clob_gateway": user_state.clob_gateway,
        },
    )
    if user_state.risk_triggers_repo is not None:
        context.risk_triggers_repo = user_state.risk_triggers_repo
    # PR6 — bind execution coordinator and give it the per-context scope lock.
    coord = getattr(user_state, "execution_coordinator", None)
    if coord is not None:
        try:
            coord.position_coordinator = context.position_coordinator
        except Exception:
            pass
        context.execution_coordinator = coord
    # PR7 — bind position drift service so the coordinator can check
    # ``is_scope_paused`` before submission.
    drift_svc = getattr(user_state, "position_drift_service", None)
    if drift_svc is not None:
        context.position_drift_service = drift_svc
        if coord is not None:
            try:
                coord.position_drift_service = drift_svc
            except Exception:
                pass
    rehydrate_position_manager(context)
    return context


async def startup_drift_check(
    *,
    runtime_users,
    user_states: dict,
    config: AppConfig,
    readiness,
) -> dict[str, bool]:
    """Run position drift sweep for every user before monitors start.

    Updates ``readiness.drift_ok`` to the overall result and returns a per
    user dict for observability. Each unresolved scope pauses through the
    PositionDriftService's internal state — the coordinator gate honours it.
    """
    all_clean = True
    per_user: dict[str, bool] = {}

    if not config.position_drift.check_on_startup:
        readiness.update(drift_ok=True)
        return per_user

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        if user_state is None:
            continue
        drift_svc = getattr(user_state, "position_drift_service", None)
        if drift_svc is None:
            # No drift service wired (e.g. partial test state). Skip
            # gracefully; in production this is unreachable.
            per_user[runtime_user.user_id] = True
            continue
        try:
            report = await drift_svc.scan_user(
                user_id=runtime_user.user_id,
                wallet=runtime_user.wallet.funder_address,
            )
        except Exception as exc:
            logger.warning(
                "startup_drift_check_failed | user=%s error=%r",
                runtime_user.user_id, exc,
            )
            per_user[runtime_user.user_id] = False
            all_clean = False
            continue
        per_user[runtime_user.user_id] = bool(report.clean)
        if not report.clean:
            all_clean = False
            logger.warning(
                "startup_drift_check_unresolved | user=%s paused_scopes=%d",
                runtime_user.user_id, len(report.paused_scopes),
            )

    readiness.update(drift_ok=all_clean)
    return per_user


def build_context_maps(
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    active_leaders: list[str],
    config: AppConfig,
    *,
    leaders_repo=None,
    readiness: ReadinessTracker | None = None,
) -> tuple[dict[str, list[TradingContext]], dict[str, TradingContext]]:
    contexts_by_leader: dict[str, list[TradingContext]] = {leader: [] for leader in active_leaders}
    risk_contexts_by_user: dict[str, TradingContext] = {}

    for runtime_user in runtime_users:
        user_state = user_states[runtime_user.user_id]
        for leader in active_leaders:
            follow = runtime_user.follows.get(leader)
            if not follow or not follow.enabled:
                continue
            context = build_trading_context(
                user_state,
                follow,
                config,
                leaders_repo=leaders_repo,
                readiness=readiness,
            )
            contexts_by_leader[leader].append(context)
            if context.user_id not in risk_contexts_by_user:
                risk_contexts_by_user[context.user_id] = context

    return contexts_by_leader, risk_contexts_by_user
