from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from domain.trading_context import TradingContext
from services.accountingService import AccountingService
from services.executionService import ExecutionService
from services.feeService import FeeService
from services.marketMetaService import MarketMetaService
from services.multiUserConfig import RuntimeUser, UserFollowConfig
from services.orderRepository import OrderRepository
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.riskEngine import RiskEngine
from services.healthServer import ReadinessTracker
from services.entryPermissionService import EntryPermissionService
from services.riskMonitor import rehydrate_position_manager, reconcile_unconfirmed_risk_orders
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo
from storage.sqlite.risk_blocks_repo import RiskBlocksRepo
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


def build_user_state(runtime_user: RuntimeUser, client: object, db_path: str) -> UserRuntimeState:
    order_repo = OrdersRepo(db_path)
    fills_repo = FillsRepo(db_path)
    positions_repo = PositionsRepo(db_path)
    risk_blocks_repo = RiskBlocksRepo(db_path)
    pnl_tracker = PnLTracker(log_path=f"log/pnl_{runtime_user.user_id}.jsonl")
    risk_engine = RiskEngine()
    accounting_service = AccountingService()
    position_manager = PositionManager()
    execution_service = ExecutionService(
        clob_client=client,
        market_meta_service=MarketMetaService(client),
        fee_service=FeeService(client),
        order_repo=OrderRepository(db_path=db_path),
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
    )


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
        entry_permission_service=EntryPermissionService(user_state.risk_blocks_repo),
        execution_service=user_state.execution_service,
        leaders_repo=leaders_repo,
        allocation_pct=follow.allocation_pct,
        buy_min_amount=config.sizing.buy_min_amount,
        buy_max_amount=min(config.sizing.buy_max_amount, follow.max_trade_usdc),
        max_market_exposure_usdc=follow.max_market_exposure_usdc,
        readiness=readiness or ReadinessTracker(),
        extra={
            "slippage_tolerance_bps": int(config.execution.slippage_tolerance * 100),
        },
    )
    rehydrate_position_manager(context)
    return context


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
