import asyncio

# Load .env as early as possible for script execution.
# This must happen before importing config.env/ENV because ENV is built at import time.
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

from config.env import (
    ENV,
    EnvValidationError,
    build_wallet_config,
    get_funder_address,
    validate_runtime_env,
)
from config.db import connect_db
from services.tradeMonitor import trade_monitor
from services.tradeExecutor import execute_trades
from services.multiUserConfig import load_runtime_users
from services.compliance import check_geoblock
from services.riskMonitor import risk_monitor_loop
from services.autoRedeem import auto_redeem_loop
from services.negRiskArb import arb_scanner_loop
from services.healthServer import start_health_server
from services.positionManager import get_position_manager
from services.pnlTracker import get_pnl_tracker
from services.traderDiscoveryService import TraderDiscoveryService
from utils.createClobClient import create_clob_client
from utils.getMyBalance import get_my_balance
from utils.logger import (
    configure_logging,
    get_logger,
    print_startup_banner,
    print_status_checks,
    print_clob_client_status,
    print_service_start,
    print_positions,
    print_traders_copying,
    print_error,
    console,
)

logger = get_logger("Main")


async def main():
    configure_logging(level="INFO")

    try:
        validate_runtime_env()
    except EnvValidationError as e:
        print_error(str(e))
        return

    leader_wallets = ENV.get('LEADER_WALLETS') or []
    # Fallback to single USER_ADDRESS for backward compatibility
    if not leader_wallets:
        single = ENV.get('USER_ADDRESS') or ''
        if single:
            leader_wallets = [single]
    leader_wallets = await TraderDiscoveryService().resolve_leaders(leader_wallets)

    connected_wallet = get_funder_address()
    wallet_cfg = build_wallet_config()
    if not connected_wallet or not wallet_cfg.private_key or not leader_wallets:
        print_error('Set LEADER_WALLETS, FUNDER_ADDRESS (or PROXY_WALLET), and PRIVATE_KEY in .env')
        return

    if ENV.get("ENFORCE_GEOBLOCK", True):
        allowed, reason = await check_geoblock()
        if not allowed:
            print_error(f"Trading disabled by geoblock policy: {reason}")
            return

    # Validate address format (Polymarket API requires full 42-char hex addresses)
    from config.env import is_valid_eth_address
    bad = [w for w in leader_wallets if not is_valid_eth_address(w)]
    if bad:
        for w in bad:
            print_error(
                f'Invalid leader wallet "{w}" — must be 42-char hex (0x + 40 hex digits). '
                f'The Polymarket API rejects malformed addresses.'
            )
        return

    runtime_users = load_runtime_users(leader_wallets)
    if not runtime_users:
        print_error("No runtime users configured.")
        return

    logger.info("Fetching wallet balances...")
    leader_balance_tasks = [get_my_balance(w) for w in leader_wallets]
    leader_balances_raw = await asyncio.gather(*leader_balance_tasks)
    leader_balances = dict(zip(leader_wallets, leader_balances_raw))

    user_balances_raw = await asyncio.gather(
        *(get_my_balance(u.wallet.funder_address) for u in runtime_users)
    )
    user_balances = {
        u.user_id: float(bal or 0.0) for u, bal in zip(runtime_users, user_balances_raw)
    }
    # Primary wallet for startup status display
    connected_wallet = runtime_users[0].wallet.funder_address
    my_balance = user_balances.get(runtime_users[0].user_id, 0.0)

    print_startup_banner()
    database_ok = None
    if ENV.get('MONGO_URI'):
        try:
            await connect_db()
            database_ok = True
        except Exception as e:
            database_ok = False
            logger.error(f'DB connection error: {e}')

    rpc_ok = my_balance is not None and all(b is not None for b in leader_balances.values())
    api_ok = True
    print_status_checks(
        rpc_ok=rpc_ok,
        balance=float(my_balance or 0.0),
        api_ok=api_ok,
        database_ok=(database_ok if database_ok is not None else False)
    )

    logger.info("Initializing CLOB clients...")
    user_clients = {}
    for runtime_user in runtime_users:
        user_clients[runtime_user.user_id] = await create_clob_client(wallet_config=runtime_user.wallet)
    print_clob_client_status(user_clients[runtime_users[0].user_id])

    print_positions(
        wallet=connected_wallet,
        available_cash=float(my_balance or 0.0),
        total_portfolio=float(my_balance or 0.0),
        positions=None,
    )

    print_traders_copying([{
        "address": w,
        "leader_balance": float(leader_balances.get(w) or 0.0),
        "positions": 0,
    } for w in leader_wallets])

    print_service_start(trader_count=len(leader_wallets))

    sizing_mode = ENV.get('SIZING_MODE', 'proportional')
    console.print(
        f'Mode={sizing_mode}, '
        f'Rate={ENV.get("PERCENTAGE")}%, '
        f'BUY: Min=${ENV.get("BUY_MIN_AMOUNT")} Max=${ENV.get("BUY_MAX_AMOUNT")}, '
        f'MinShare={ENV.get("MIN_SHARE_PRICE")}, '
        f'SL={ENV.get("STOP_LOSS_PCT")}% TP={ENV.get("TAKE_PROFIT_PCT")}%, '
        f'Slippage≤{ENV.get("SLIPPAGE_TOLERANCE")}%, '
        f'Leaders={len(leader_wallets)}, '
        f'AutoRedeem={"on" if ENV.get("ENABLE_AUTO_REDEEM", True) else "off"}',
        style="muted",
    )
    console.print()

    # Start risk monitor as a background task
    risk_task = None
    metrics_runner = None
    if ENV.get("ENABLE_METRICS_SERVER", False):
        metrics_runner = await start_health_server(int(ENV.get("METRICS_PORT", 9102)))

    if ENV.get('ENABLE_RISK_MGMT', True):
        # Risk monitor runs on the first configured user by default.
        primary_client = user_clients[runtime_users[0].user_id]
        risk_task = asyncio.create_task(
            risk_monitor_loop(
                primary_client,
                {"proxyWallet": runtime_users[0].wallet.funder_address},
            )
        )

    # Start auto-redeem as a background task
    redeem_task = None
    if ENV.get('ENABLE_AUTO_REDEEM', True):
        primary_client = user_clients[runtime_users[0].user_id]
        redeem_task = asyncio.create_task(auto_redeem_loop(primary_client, {}))

    # Start neg-risk arb scanner as a background task
    arb_task = None
    if str(ENV.get('ENABLE_NEG_RISK_ARB', 'false')).lower() in ('1', 'true', 'yes'):
        arb_client = user_clients[runtime_users[0].user_id]
        arb_task = asyncio.create_task(arb_scanner_loop(arb_client, {}))

    # Create a monitor coroutine for each leader wallet
    async def run_leader(wallet: str):
        enabled_follows: list[tuple] = []
        for runtime_user in runtime_users:
            follow = runtime_user.follows.get(wallet)
            if follow and follow.enabled:
                enabled_follows.append((runtime_user, follow, user_clients[runtime_user.user_id]))
        if not enabled_follows:
            logger.info(f"No users following leader {wallet[:10]}…, monitor skipped.")
            return

        trade_queue: asyncio.Queue[list[dict]] = asyncio.Queue(maxsize=200)

        async def worker():
            while True:
                trades = await trade_queue.get()
                try:
                    tasks = []
                    for runtime_user, follow, client in enabled_follows:
                        bot_params = {
                            'leaderWallet': wallet,
                            'proxyWallet': runtime_user.wallet.funder_address,
                            'buyMinAmount': ENV.get('BUY_MIN_AMOUNT', 1),
                            'buyMaxAmount': min(ENV.get('BUY_MAX_AMOUNT', 100), follow.max_trade_usdc),
                            'tradeRate': follow.allocation_pct * 100.0,
                            'positionThreshold': int(ENV.get('POSITION_THRESHOLD', 0)),
                            'FETCH_INTERVAL': ENV.get('FETCH_INTERVAL', 1),
                        }
                        tasks.append(execute_trades(client, bot_params, trades))
                    if tasks:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for res in results:
                            if isinstance(res, Exception):
                                logger.error(
                                    f'Trade execution error for leader {wallet[:10]}…: {res}',
                                    exc_info=True,
                                )
                finally:
                    trade_queue.task_done()

        async def on_trade(trades: list[dict]):
            await trade_queue.put(trades)

        worker_task = asyncio.create_task(worker())
        try:
            await trade_monitor(
                {
                    'leaderWallet': wallet,
                    'FETCH_INTERVAL': ENV.get('FETCH_INTERVAL', 1),
                },
                on_trade,
            )
        finally:
            worker_task.cancel()

    # Run all leader monitors concurrently
    monitor_tasks = [asyncio.create_task(run_leader(w)) for w in leader_wallets]

    try:
        await asyncio.gather(*monitor_tasks)
    finally:
        if risk_task:
            risk_task.cancel()
        if redeem_task:
            redeem_task.cancel()
        if metrics_runner:
            await metrics_runner.cleanup()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Print P&L summary on shutdown
        try:
            tracker = get_pnl_tracker()
            tracker.print_summary()
            pm = get_position_manager()
            summary = pm.summary()
            logger.info(f"📋 Position summary: {summary}")
        except Exception:
            pass
    except Exception:
        import sys
        sys.exit(1)
