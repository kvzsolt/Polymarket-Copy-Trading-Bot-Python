from __future__ import annotations

import asyncio
import signal
import time
from dataclasses import dataclass

# Load .env as early as possible for script execution.
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from config.db import connect_db, get_db_path
from config.env import ENV, EnvValidationError, get_funder_address, is_valid_eth_address, validate_runtime_env
from domain.trading_context import TradingContext
from orchestrators.leader_registry import LeaderRegistry
from orchestrators.trade_pipeline import execute_trades as execute_trade_pipeline
from services.accountingService import AccountingService
from services.autoRedeem import auto_redeem_loop
from services.compliance import check_geoblock
from services.discovery.capture_service import DiscoveryCaptureService
from services.executionService import ExecutionService
from services.feeService import FeeService
from services.healthServer import start_health_server, update_readiness
from services.marketMetaService import MarketMetaService
from services.metrics import metrics
from services.multiUserConfig import RuntimeUser, UserFollowConfig, load_runtime_users
from services.reconciliation import ReconciliationService, reconciliation_loop
from services.negRiskArb import arb_scanner_loop
from services.orderRepository import OrderRepository
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.riskEngine import RiskEngine
from services.riskMonitor import rehydrate_position_manager, risk_monitor_loop_for_contexts
from services.tradeMonitor import trade_monitor
from services.traderDiscoveryService import TraderDiscoveryService
from storage.sqlite.fills_repo import FillsRepo
from storage.sqlite.leaders_repo import LeadersRepo
from storage.sqlite.orders_repo import OrdersRepo
from storage.sqlite.positions_repo import PositionsRepo
from utils.createClobClient import create_clob_client
from utils.getMyBalance import get_my_balance
from utils.logger import (
    configure_logging,
    console,
    get_logger,
    print_clob_client_status,
    print_error,
    print_first_run_init,
    print_historical_done,
    print_monitor_start,
    print_positions,
    print_service_start,
    print_startup_banner,
    print_status_checks,
    print_traders_copying,
    redact_error_detail,
)

logger = get_logger("Main")


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


def _configured_leaders() -> list[str]:
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


def _default_follow(user_id: str, leader_id: str) -> UserFollowConfig:
    return UserFollowConfig(
        user_id=user_id,
        leader_id=leader_id,
        enabled=True,
        allocation_pct=float(ENV.get("PERCENTAGE", 5.0)) / 100.0,
        max_trade_usdc=float(ENV.get("BUY_MAX_AMOUNT", 100)),
        max_market_exposure_usdc=float(ENV.get("MAX_MARKET_EXPOSURE_USDC", 300)),
    )


def _ensure_runtime_follows(runtime_users: list[RuntimeUser], leaders: list[str]) -> None:
    for runtime_user in runtime_users:
        for leader in leaders:
            if leader not in runtime_user.follows:
                runtime_user.follows[leader] = _default_follow(runtime_user.user_id, leader)


def _build_user_state(runtime_user: RuntimeUser, client: object, db_path: str) -> UserRuntimeState:
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


def _build_trading_context(
    user_state: UserRuntimeState, follow: UserFollowConfig, leaders_repo=None
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


async def _wait_for_active_leaders(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
) -> list[str]:
    state = await registry.refresh(configured_leaders)
    if state.active_monitored:
        return state.active_monitored

    if not configured_leaders:
        if ENV.get("LEADER_AUTO", False):
            logger.error(
                "LEADER_AUTO is enabled, but discovery did not find any eligible leaders "
                "to promote. Relax MIN_LEADER_* or AUTO_DISCOVER_MIN_SCORE, or add "
                "manual LEADER_WALLETS."
            )
        else:
            logger.error(
                "No active leaders after initial refresh. No LEADER_WALLETS configured and "
                "discovery did not promote any leaders. Set LEADER_WALLETS=0x... in your .env, "
                "or enable LEADER_AUTO=true to auto-discover and promote leaders."
            )
    else:
        logger.warning(
            "No active leaders available; entering discovery-only mode and continuing refresh."
        )
    while True:
        await metrics.inc("no_leaders_monitored_total")
        await asyncio.sleep(refresh_interval)
        state = await registry.refresh(configured_leaders)
        if state.active_monitored:
            logger.info(
                "Discovery produced active leaders; resuming live monitoring",
                extra={"leaders": state.active_monitored},
            )
            return state.active_monitored


async def _discovery_refresh_loop(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    leaders_repo: "LeadersRepo | None" = None,
    on_new_leaders: "Callable[[list[str]], Awaitable[None]] | None" = None,
    already_monitored: set[str] | None = None,
) -> None:
    """Periodically refresh leader discovery and promote newly-monitorable leaders.

    When *leaders_repo* and *on_new_leaders* are provided, the loop checks after
    each refresh whether any leaders have been promoted to ``paper_follow`` or
    ``live_follow`` that are not yet being monitored, and calls *on_new_leaders*
    with the new wallet addresses so that ``main()`` can spawn monitor tasks.
    """
    _monitored: set[str] = set(already_monitored) if already_monitored else set()
    while True:
        await asyncio.sleep(refresh_interval)
        try:
            await registry.refresh(configured_leaders)
            if leaders_repo is not None and on_new_leaders is not None:
                live = set(leaders_repo.get_leaders_by_tier("live_follow"))
                paper = set(leaders_repo.get_leaders_by_tier("paper_follow"))
                monitorable = live | paper
                new_leaders = sorted(monitorable - _monitored)
                if new_leaders:
                    logger.info(
                        "discovery_new_monitorable_leaders",
                        extra={"count": len(new_leaders), "wallets": new_leaders},
                    )
                    _monitored.update(new_leaders)
                    await on_new_leaders(new_leaders)
        except Exception as exc:
            logger.warning(
                "Background discovery refresh failed",
                extra={"error": redact_error_detail(exc)},
            )


async def main():
    configure_logging(level="INFO")

    try:
        validate_runtime_env()
    except EnvValidationError as exc:
        print_error(str(exc))
        return

    if ENV.get("ENFORCE_GEOBLOCK", True):
        allowed, reason = await check_geoblock()
        if not allowed:
            print_error(f"Trading disabled by geoblock policy: {reason}")
            return

    configured_leaders = _configured_leaders()
    invalid = [w for w in configured_leaders if not is_valid_eth_address(w)]
    if invalid:
        for wallet in invalid:
            print_error(
                f'Invalid leader wallet "{wallet}" — must be 42-char hex (0x + 40 hex digits).'
            )
        return

    runtime_users = load_runtime_users(configured_leaders)
    if not runtime_users:
        print_error("No runtime users configured.")
        return

    update_readiness(runtime_user_count=len(runtime_users))

    await connect_db()
    update_readiness(db_ok=True)

    db_path = get_db_path()

    # Startup housekeeping: purge stale records from SQLite
    from utils.idempotency import purge_stale_records
    purged = purge_stale_records()
    if purged:
        logger.info(f"Startup cleanup: purged {purged} stale records")

    research_capture_service = DiscoveryCaptureService(db_path)
    leaders_repo = LeadersRepo(db_path)
    registry = LeaderRegistry(
        discovery_service=TraderDiscoveryService(),
        leaders_repo=leaders_repo,
        logger_instance=logger,
    )
    refresh_interval = int(ENV.get("DISCOVERY_REFRESH_INTERVAL_SECONDS", 300) or 300)
    active_leaders = await _wait_for_active_leaders(registry, configured_leaders, refresh_interval)
    _ensure_runtime_follows(runtime_users, active_leaders)

    logger.info("Initializing CLOB clients...")
    user_states: dict[str, UserRuntimeState] = {}
    for runtime_user in runtime_users:
        client = await create_clob_client(wallet_config=runtime_user.wallet)
        user_states[runtime_user.user_id] = _build_user_state(runtime_user, client, db_path)

    first_user = runtime_users[0]
    first_client = user_states[first_user.user_id].client
    print_startup_banner()
    print_clob_client_status(first_client)

    leader_balances_raw = await asyncio.gather(*(get_my_balance(w) for w in active_leaders))
    leader_balances = dict(zip(active_leaders, leader_balances_raw))
    user_balances_raw = await asyncio.gather(*(get_my_balance(u.wallet.funder_address) for u in runtime_users))
    user_balances = {u.user_id: float(b or 0.0) for u, b in zip(runtime_users, user_balances_raw)}

    api_ok = all(v is not None for v in leader_balances_raw) and all(v is not None for v in user_balances_raw)
    update_readiness(api_ok=api_ok)
    print_status_checks(
        rpc_ok=api_ok,
        balance=float(user_balances.get(first_user.user_id, 0.0)),
        api_ok=api_ok,
        database_ok=True,
    )
    print_positions(
        wallet=first_user.wallet.funder_address,
        available_cash=float(user_balances.get(first_user.user_id, 0.0)),
        total_portfolio=float(user_balances.get(first_user.user_id, 0.0)),
        positions=None,
    )
    print_traders_copying(
        [
            {
                "address": wallet,
                "leader_balance": float(leader_balances.get(wallet) or 0.0),
                "positions": 0,
            }
            for wallet in active_leaders
        ]
    )
    print_service_start(trader_count=len(active_leaders))
    print_monitor_start(trader_count=len(active_leaders), interval_s=int(ENV.get("FETCH_INTERVAL", 1)))
    print_first_run_init()
    print_historical_done()

    console.print(
        (
            f"Mode={ENV.get('SIZING_MODE', 'proportional')}, "
            f"Rate={ENV.get('PERCENTAGE')}%, "
            f"BUY: Min=${ENV.get('BUY_MIN_AMOUNT')} Max=${ENV.get('BUY_MAX_AMOUNT')}, "
            f"SL={ENV.get('STOP_LOSS_PCT')}% TP={ENV.get('TAKE_PROFIT_PCT')}%, "
            f"Slippage≤{ENV.get('SLIPPAGE_TOLERANCE')}%, "
            f"Leaders={len(active_leaders)}"
        ),
        style="muted",
    )
    console.print()

    contexts_by_leader: dict[str, list[TradingContext]] = {leader: [] for leader in active_leaders}
    risk_contexts_by_user: dict[str, TradingContext] = {}
    for runtime_user in runtime_users:
        user_state = user_states[runtime_user.user_id]
        for leader in active_leaders:
            follow = runtime_user.follows.get(leader)
            if not follow or not follow.enabled:
                continue
            context = _build_trading_context(user_state, follow, leaders_repo=leaders_repo)
            contexts_by_leader[leader].append(context)
            if context.user_id not in risk_contexts_by_user:
                risk_contexts_by_user[context.user_id] = context

    monitored_leaders = [leader for leader, contexts in contexts_by_leader.items() if contexts]
    monitored_set: set[str] = set(monitored_leaders)
    monitor_tasks: list[asyncio.Task] = []
    update_readiness(monitored_leader_count=len(monitored_leaders))
    if not monitored_leaders:
        logger.warning("no_leaders_monitored", extra={"reason": "no_user_follows_active_leaders"})
        await metrics.inc("no_leaders_monitored_total")

    metrics_runner = None
    if ENV.get("ENABLE_METRICS_SERVER", False):
        metrics_runner = await start_health_server(int(ENV.get("METRICS_PORT", 9102)))

    async def _on_new_leaders(new_wallets: list[str]) -> None:
        """Callback invoked by the discovery loop when new leaders become monitorable."""
        _ensure_runtime_follows(runtime_users, new_wallets)
        for wallet in new_wallets:
            if wallet in contexts_by_leader:
                continue
            contexts_by_leader[wallet] = []
            for ru in runtime_users:
                us = user_states.get(ru.user_id)
                follow = ru.follows.get(wallet)
                if not us or not follow or not follow.enabled:
                    continue
                ctx = _build_trading_context(us, follow, leaders_repo=leaders_repo)
                contexts_by_leader[wallet].append(ctx)
            if contexts_by_leader[wallet]:
                task = asyncio.create_task(run_leader(wallet))
                monitor_tasks.append(task)
                logger.info(
                    "dynamic_leader_monitor_started",
                    extra={"leader": wallet},
                )
        update_readiness(monitored_leader_count=len([
            w for w, ctxs in contexts_by_leader.items() if ctxs
        ]))

    discovery_task = asyncio.create_task(
        _discovery_refresh_loop(
            registry,
            configured_leaders,
            refresh_interval,
            leaders_repo=leaders_repo,
            on_new_leaders=_on_new_leaders,
            already_monitored=monitored_set,
        )
    )

    risk_task = None
    if ENV.get("ENABLE_RISK_MGMT", True):
        risk_task = asyncio.create_task(
            risk_monitor_loop_for_contexts(list(risk_contexts_by_user.values()))
        )

    redeem_task = None
    if ENV.get("ENABLE_AUTO_REDEEM", True):
        redeem_task = asyncio.create_task(auto_redeem_loop(first_client, {}))

    arb_task = None
    if str(ENV.get("ENABLE_NEG_RISK_ARB", "false")).lower() in ("1", "true", "yes"):
        arb_task = asyncio.create_task(arb_scanner_loop(first_client, {}))

    reconcile_tasks: list[asyncio.Task] = []
    if str(ENV.get("ENABLE_RECONCILIATION", "true")).lower() in ("1", "true", "yes"):
        for runtime_user in runtime_users:
            user_state = user_states[runtime_user.user_id]
            recon_svc = ReconciliationService(
                clob_client=user_state.client,
                orders_repo=user_state.orders_repo,
            )
            reconcile_tasks.append(asyncio.create_task(reconciliation_loop(recon_svc)))

    async def run_leader(leader_wallet: str):
        contexts = contexts_by_leader.get(leader_wallet, [])
        if not contexts:
            logger.warning("no_leaders_monitored", extra={"leader_id": leader_wallet, "reason": "no_followers"})
            await metrics.inc("no_leaders_monitored_total")
            return

        trade_queue: asyncio.Queue[list[dict]] = asyncio.Queue(maxsize=200)

        async def worker():
            while True:
                trades = await trade_queue.get()
                try:
                    now_ts = time.time()
                    tasks = [
                        execute_trade_pipeline(context, leader_wallet, trades, now_ts)
                        for context in contexts
                    ]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for i, result in enumerate(results):
                        if isinstance(result, Exception):
                            ctx = contexts[i]
                            logger.error(
                                "trade_pipeline_exception",
                                extra={
                                    "leader_id": leader_wallet,
                                    "user": ctx.user_id,
                                    "error": redact_error_detail(result),
                                    "error_type": type(result).__name__,
                                },
                            )
                            await metrics.inc("trade_pipeline_crashes_total")
                finally:
                    trade_queue.task_done()

        async def on_trade(trades: list[dict]):
            await trade_queue.put(trades)

        worker_task = asyncio.create_task(worker())
        try:
            await trade_monitor(
                {
                    "leaderWallet": leader_wallet,
                    "FETCH_INTERVAL": ENV.get("FETCH_INTERVAL", 1),
                },
                on_trade,
                research_capture_service=research_capture_service,
            )
        finally:
            worker_task.cancel()

    monitor_tasks.extend(asyncio.create_task(run_leader(wallet)) for wallet in monitored_leaders)

    try:
        # Keep main alive: wait for monitors, or sleep-poll if none yet.
        # Dynamic monitors may be added by the discovery callback.
        while True:
            active = [t for t in monitor_tasks if not t.done()]
            if active:
                done, _ = await asyncio.wait(active, timeout=refresh_interval)
                for t in done:
                    if t.exception():
                        logger.error(
                            "monitor_task_crashed",
                            extra={"error": redact_error_detail(t.exception())},
                        )
            else:
                await asyncio.sleep(refresh_interval)
                await metrics.inc("no_leaders_monitored_total")
    finally:
        shutdown_tasks = list(monitor_tasks)
        shutdown_tasks.append(discovery_task)
        if risk_task:
            shutdown_tasks.append(risk_task)
        if redeem_task:
            shutdown_tasks.append(redeem_task)
        if arb_task:
            shutdown_tasks.append(arb_task)
        if reconcile_tasks:
            shutdown_tasks.extend(reconcile_tasks)

        for task in shutdown_tasks:
            task.cancel()
        if shutdown_tasks:
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)

        try:
            from utils.fetchData import close_session

            await close_session()
        except Exception as exc:
            logger.warning(
                "failed_to_close_http_session",
                extra={"error": redact_error_detail(exc)},
            )
        if metrics_runner:
            await metrics_runner.cleanup()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    main_task = loop.create_task(main())

    def _shutdown_handler(sig, _frame):
        logger.info("Received %s — initiating graceful shutdown", signal.Signals(sig).name)
        main_task.cancel()

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    try:
        loop.run_until_complete(main_task)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    except Exception:
        import sys

        sys.exit(1)
    finally:
        loop.close()
