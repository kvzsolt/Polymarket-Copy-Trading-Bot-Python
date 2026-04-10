from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable

# Load non-sensitive runtime config + local .env as early as possible.
try:
    from runtime_env import load_runtime_env

    load_runtime_env()
except Exception:
    pass

from config.db import connect_db, get_db_path
from config.env import ENV, EnvValidationError, is_valid_eth_address, validate_runtime_env
from orchestrators.leader_registry import LeaderRegistry
from orchestrators.runtime_bootstrap import (
    UserRuntimeState,
    build_context_maps,
    build_trading_context,
    build_user_state,
    configured_leaders,
    ensure_runtime_follows,
    fetch_live_positions,
)
from orchestrators.runtime_discovery import (
    discovery_refresh_loop as _discovery_refresh_loop_impl,
    wait_for_active_leaders as _wait_for_active_leaders_impl,
)
from orchestrators.runtime_monitor import run_leader_monitor
from orchestrators.runtime_summary import print_startup_summary
from services.autoRedeem import auto_redeem_loop
from services.compliance import check_geoblock
from services.discovery.capture_service import DiscoveryCaptureService
from services.healthServer import start_health_server, update_readiness
from services.metrics import metrics
from services.multiUserConfig import RuntimeUser, UserFollowConfig, load_runtime_users
from services.negRiskArb import arb_scanner_loop
from services.reconciliation import ReconciliationService, reconciliation_loop
from services.traderDiscoveryService import TraderDiscoveryService
from storage.sqlite.leaders_repo import LeadersRepo
from utils.createClobClient import create_clob_client
from utils.getMyBalance import get_my_balance
from utils.logger import configure_logging, get_logger, print_error, redact_error_detail

logger = get_logger("Main")


async def _wait_for_active_leaders(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
) -> list[str]:
    return await _wait_for_active_leaders_impl(registry, configured_leaders, refresh_interval)


async def _discovery_refresh_loop(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    leaders_repo: LeadersRepo | None = None,
    on_new_leaders: Callable[[list[str]], Awaitable[None]] | None = None,
    already_monitored: set[str] | None = None,
) -> None:
    await _discovery_refresh_loop_impl(
        registry,
        configured_leaders,
        refresh_interval,
        leaders_repo=leaders_repo,
        on_new_leaders=on_new_leaders,
        already_monitored=already_monitored,
    )


def _refresh_contexts_for_new_leaders(
    *,
    new_wallets: list[str],
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    contexts_by_leader: dict[str, list],
    leaders_repo: LeadersRepo,
) -> list[str]:
    started_wallets: list[str] = []
    ensure_runtime_follows(runtime_users, new_wallets)
    for wallet in new_wallets:
        if wallet in contexts_by_leader:
            continue
        contexts_by_leader[wallet] = []
        for runtime_user in runtime_users:
            user_state = user_states.get(runtime_user.user_id)
            follow: UserFollowConfig | None = runtime_user.follows.get(wallet)
            if not user_state or not follow or not follow.enabled:
                continue
            context = build_trading_context(user_state, follow, leaders_repo=leaders_repo)
            contexts_by_leader[wallet].append(context)
        if contexts_by_leader[wallet]:
            started_wallets.append(wallet)
    return started_wallets


async def _shutdown_runtime(
    *,
    monitor_tasks: list[asyncio.Task],
    discovery_task: asyncio.Task | None,
    risk_task: asyncio.Task | None,
    redeem_task: asyncio.Task | None,
    arb_task: asyncio.Task | None,
    reconcile_tasks: list[asyncio.Task],
    metrics_runner,
) -> None:
    shutdown_tasks = list(monitor_tasks)
    if discovery_task:
        shutdown_tasks.append(discovery_task)
    if risk_task:
        shutdown_tasks.append(risk_task)
    if redeem_task:
        shutdown_tasks.append(redeem_task)
    if arb_task:
        shutdown_tasks.append(arb_task)
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

    configured = configured_leaders()
    invalid = [wallet for wallet in configured if not is_valid_eth_address(wallet)]
    if invalid:
        for wallet in invalid:
            print_error(
                f'Invalid leader wallet "{wallet}" — must be 42-char hex (0x + 40 hex digits).'
            )
        return

    runtime_users = load_runtime_users(configured)
    if not runtime_users:
        print_error("No runtime users configured.")
        return

    update_readiness(runtime_user_count=len(runtime_users))

    await connect_db()
    update_readiness(db_ok=True)
    db_path = get_db_path()

    # Startup housekeeping: purge stale idempotency records.
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
    active_leaders = await _wait_for_active_leaders(registry, configured, refresh_interval)
    ensure_runtime_follows(runtime_users, active_leaders)

    user_states: dict[str, UserRuntimeState] = {}
    for runtime_user in runtime_users:
        client = await create_clob_client(wallet_config=runtime_user.wallet)
        user_states[runtime_user.user_id] = build_user_state(runtime_user, client, db_path)

    leader_balances_raw = await asyncio.gather(*(get_my_balance(wallet) for wallet in active_leaders))
    user_balances_raw = await asyncio.gather(
        *(get_my_balance(runtime_user.wallet.funder_address) for runtime_user in runtime_users)
    )
    user_balances = {
        runtime_user.user_id: float(balance or 0.0)
        for runtime_user, balance in zip(runtime_users, user_balances_raw)
    }
    api_ok = all(value is not None for value in leader_balances_raw) and all(
        value is not None for value in user_balances_raw
    )
    update_readiness(api_ok=api_ok)

    contexts_by_leader, risk_contexts_by_user = build_context_maps(
        runtime_users,
        user_states,
        active_leaders,
        leaders_repo=leaders_repo,
    )
    monitored_leaders = [leader for leader, contexts in contexts_by_leader.items() if contexts]
    update_readiness(monitored_leader_count=len(monitored_leaders))
    if not monitored_leaders:
        logger.warning("no_leaders_monitored", extra={"reason": "no_user_follows_active_leaders"})
        await metrics.inc("no_leaders_monitored_total")

    first_user = runtime_users[0]
    live_positions = await fetch_live_positions(first_user.wallet.funder_address)
    live_positions_value = sum(float(position.get("currentValue") or 0.0) for position in live_positions)
    available_cash = float(user_balances.get(first_user.user_id, 0.0))
    print_startup_summary(
        wallet=first_user.wallet.funder_address,
        available_cash=available_cash,
        total_portfolio=available_cash + live_positions_value,
        positions=live_positions,
        active_leaders=monitored_leaders,
    )

    metrics_runner = None
    if ENV.get("ENABLE_METRICS_SERVER", False):
        metrics_runner = await start_health_server(int(ENV.get("METRICS_PORT", 9102)))

    monitor_tasks: list[asyncio.Task] = []
    monitored_set: set[str] = set(monitored_leaders)
    fetch_interval = int(ENV.get("FETCH_INTERVAL", 1) or 1)

    async def _on_new_leaders(new_wallets: list[str]) -> None:
        started_wallets = _refresh_contexts_for_new_leaders(
            new_wallets=new_wallets,
            runtime_users=runtime_users,
            user_states=user_states,
            contexts_by_leader=contexts_by_leader,
            leaders_repo=leaders_repo,
        )
        monitored_leader_count = len([wallet for wallet, contexts in contexts_by_leader.items() if contexts])
        for wallet in started_wallets:
            monitor_tasks.append(
                asyncio.create_task(
                    run_leader_monitor(
                        wallet,
                        contexts_by_leader[wallet],
                        fetch_interval=fetch_interval,
                        monitored_leader_count=monitored_leader_count,
                        research_capture_service=research_capture_service,
                    )
                )
            )
            logger.info("dynamic_leader_monitor_started", extra={"leader": wallet})
        update_readiness(
            monitored_leader_count=len([wallet for wallet, contexts in contexts_by_leader.items() if contexts])
        )

    discovery_task = None
    if ENV.get("ENABLE_TRADER_DISCOVERY", False):
        discovery_task = asyncio.create_task(
            _discovery_refresh_loop(
                registry,
                configured,
                refresh_interval,
                leaders_repo=leaders_repo,
                on_new_leaders=_on_new_leaders,
                already_monitored=monitored_set,
            )
        )

    risk_task = None
    if ENV.get("ENABLE_RISK_MGMT", True):
        from services.riskMonitor import risk_monitor_loop_for_contexts

        risk_task = asyncio.create_task(risk_monitor_loop_for_contexts(list(risk_contexts_by_user.values())))

    redeem_task = None
    if ENV.get("ENABLE_AUTO_REDEEM", True):
        first_client = user_states[first_user.user_id].client
        redeem_task = asyncio.create_task(auto_redeem_loop(first_client, {}))

    arb_task = None
    if str(ENV.get("ENABLE_NEG_RISK_ARB", "false")).lower() in ("1", "true", "yes"):
        first_client = user_states[first_user.user_id].client
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

    monitor_tasks.extend(
        asyncio.create_task(
            run_leader_monitor(
                wallet,
                contexts_by_leader[wallet],
                fetch_interval=fetch_interval,
                monitored_leader_count=len(monitored_leaders),
                research_capture_service=research_capture_service,
            )
        )
        for wallet in monitored_leaders
    )

    try:
        while True:
            active = [task for task in monitor_tasks if not task.done()]
            if active:
                done, _ = await asyncio.wait(active, timeout=refresh_interval)
                for task in done:
                    if task.exception():
                        logger.error(
                            "monitor_task_crashed",
                            extra={"error": redact_error_detail(task.exception())},
                        )
            else:
                await asyncio.sleep(refresh_interval)
                await metrics.inc("no_leaders_monitored_total")
    finally:
        await _shutdown_runtime(
            monitor_tasks=monitor_tasks,
            discovery_task=discovery_task,
            risk_task=risk_task,
            redeem_task=redeem_task,
            arb_task=arb_task,
            reconcile_tasks=reconcile_tasks,
            metrics_runner=metrics_runner,
        )


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
