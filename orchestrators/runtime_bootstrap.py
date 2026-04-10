from __future__ import annotations

from dataclasses import dataclass

from config.env import ENV
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
from services.riskMonitor import rehydrate_position_manager
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo
from utils.fetchData import fetch_data
from utils.logger import get_logger

logger = get_logger("RuntimeBootstrap")


@dataclass
class UserRuntimeState:
    user: RuntimeUser
    client: object
    orders_repo: OrdersRepo
    fills_repo: FillsRepo
    positions_repo: PositionsRepo
    pnl_tracker: PnLTracker
    risk_engine: RiskEngine
    accounting_service: AccountingService
    position_manager: PositionManager
    execution_service: ExecutionService


def configured_leaders() -> list[str]:
    leaders = list(ENV.get("LEADER_WALLETS") or [])
    if not leaders:
        single = str(ENV.get("USER_ADDRESS") or "").strip()
        if single:
            leaders = [single]
    result = [str(w).strip() for w in leaders if str(w).strip()]
    if not result:
        if ENV.get("LEADER_AUTO", False):
            logger.info(
                "No leader wallets configured. LEADER_AUTO is enabled, so the bot "
                f"will auto-discover and follow up to {int(ENV.get('LEADER_AUTO_COUNT', 5) or 5)} leaders."
            )
        else:
            logger.warning(
                "No leader wallets configured. Both LEADER_WALLETS and USER_ADDRESS "
                "are empty. Set LEADER_AUTO=true to auto-discover leaders, or add "
                "manual leader wallets."
            )
    return result


def default_follow(user_id: str, leader_id: str) -> UserFollowConfig:
    return UserFollowConfig(
        user_id=user_id,
        leader_id=leader_id,
        enabled=True,
        allocation_pct=float(ENV.get("PERCENTAGE", 5.0)) / 100.0,
        max_trade_usdc=float(ENV.get("BUY_MAX_AMOUNT", 100)),
        max_market_exposure_usdc=float(ENV.get("MAX_MARKET_EXPOSURE_USDC", 300)),
    )


def ensure_runtime_follows(runtime_users: list[RuntimeUser], leaders: list[str]) -> None:
    for runtime_user in runtime_users:
        for leader in leaders:
            if leader not in runtime_user.follows:
                runtime_user.follows[leader] = default_follow(runtime_user.user_id, leader)


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
        pnl_tracker=pnl_tracker,
        risk_engine=risk_engine,
        accounting_service=accounting_service,
        position_manager=position_manager,
        execution_service=execution_service,
    )


def build_trading_context(
    user_state: UserRuntimeState,
    follow: UserFollowConfig,
    leaders_repo=None,
) -> TradingContext:
    context = TradingContext(
        user_id=user_state.user.user_id,
        wallet_address=user_state.user.wallet.funder_address,
        client=user_state.client,
        positions_repo=user_state.positions_repo,
        fills_repo=user_state.fills_repo,
        orders_repo=user_state.orders_repo,
        pnl_tracker=user_state.pnl_tracker,
        risk_engine=user_state.risk_engine,
        accounting_service=user_state.accounting_service,
        position_manager=user_state.position_manager,
        execution_service=user_state.execution_service,
        leaders_repo=leaders_repo,
        allocation_pct=follow.allocation_pct,
        buy_min_amount=float(ENV.get("BUY_MIN_AMOUNT", 1)),
        buy_max_amount=min(float(ENV.get("BUY_MAX_AMOUNT", 100)), follow.max_trade_usdc),
        max_market_exposure_usdc=follow.max_market_exposure_usdc,
        extra={
            "slippage_tolerance_bps": int(float(ENV.get("SLIPPAGE_TOLERANCE", 2.0)) * 100),
        },
    )
    rehydrate_position_manager(context)
    return context


def build_context_maps(
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    active_leaders: list[str],
    *,
    leaders_repo=None,
) -> tuple[dict[str, list[TradingContext]], dict[str, TradingContext]]:
    contexts_by_leader: dict[str, list[TradingContext]] = {leader: [] for leader in active_leaders}
    risk_contexts_by_user: dict[str, TradingContext] = {}

    for runtime_user in runtime_users:
        user_state = user_states[runtime_user.user_id]
        for leader in active_leaders:
            follow = runtime_user.follows.get(leader)
            if not follow or not follow.enabled:
                continue
            context = build_trading_context(user_state, follow, leaders_repo=leaders_repo)
            contexts_by_leader[leader].append(context)
            if context.user_id not in risk_contexts_by_user:
                risk_contexts_by_user[context.user_id] = context

    return contexts_by_leader, risk_contexts_by_user
