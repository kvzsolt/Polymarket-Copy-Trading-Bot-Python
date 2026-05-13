from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable

# Load non-sensitive runtime config + local .env as early as possible.
# IMPORTANT: importing from config triggers config/__init__.py which freezes
# the ENV snapshot before load_runtime_env() runs. We must call rehydrate_env()
# immediately after so ENV reflects the freshly populated os.environ.
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orchestrators.runtime_bootstrap import TradingContext
try:
    from config.runtime_env import load_runtime_env
    from config.env import rehydrate_env

    load_runtime_env()
    rehydrate_env()
except Exception as exc:  # pragma: no cover
    import sys

    print(f"[WARNING] Failed to load runtime env: {exc}", file=sys.stderr)

from bootstrap import build_app
from config.app_config import EnvValidationError
from config.env import is_valid_eth_address, validate_runtime_env
from orchestrators.leader_registry import LeaderRegistry
from orchestrators.market_data_runtime import (
    MarketDataRuntime,
    extract_token_ids_from_contexts,
    extract_token_ids_from_live_positions,
)
from orchestrators.runtime_bootstrap import (
    UserRuntimeState,
    build_context_maps,
    build_trading_context,
    build_user_state,
    configured_leaders,
    ensure_runtime_follows,
    fetch_live_positions,
    startup_reconcile_unconfirmed,
)
from orchestrators.runtime_discovery import (
    discovery_refresh_loop as _discovery_refresh_loop_impl,
    wait_for_active_leaders as _wait_for_active_leaders_impl,
)
from orchestrators.runtime_monitor import run_leader_monitor
from orchestrators.runtime_summary import print_startup_summary
from orchestrators.startup_health import perform_startup_health_check
from orchestrators.user_data_runtime import UserDataRuntime
from services.compliance import check_geoblock
from services.discovery.capture_service import DiscoveryCaptureService
from services.healthServer import start_health_server
from services.metrics import metrics
from services.multiUserConfig import RuntimeUser, UserFollowConfig, load_runtime_users
from storage.sqlite.leaders_repo import LeadersRepo
from utils.createClobClient import create_clob_client
from utils.getMyBalance import get_my_balance
from utils.logger import (
    configure_logging,
    get_logger,
    print_error,
    redact_error_detail,
)

logger = get_logger("Main")


async def _wait_for_active_leaders(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    *,
    config,
) -> list[str]:
    return await _wait_for_active_leaders_impl(
        registry,
        configured_leaders,
        refresh_interval,
        config=config,
    )


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


def _flatten_contexts(
    contexts_by_leader: dict[str, list["TradingContext"]],
) -> list["TradingContext"]:
    return [
        context
        for contexts in contexts_by_leader.values()
        for context in contexts
    ]


def _refresh_contexts_for_new_leaders(
    *,
    new_wallets: list[str],
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    contexts_by_leader: dict[str, list],
    leaders_repo: LeadersRepo,
    config,
    readiness,
) -> list[str]:
    started_wallets: list[str] = []
    ensure_runtime_follows(runtime_users, new_wallets, config)

    for wallet in new_wallets:
        if wallet in contexts_by_leader:
            continue

        contexts_by_leader[wallet] = []

        for runtime_user in runtime_users:
            user_state = user_states.get(runtime_user.user_id)
            follow: UserFollowConfig | None = runtime_user.follows.get(wallet)

            if not user_state or not follow or not follow.enabled:
                continue

            context = build_trading_context(
                user_state,
                follow,
                config,
                leaders_repo=leaders_repo,
                readiness=readiness,
            )
            contexts_by_leader[wallet].append(context)

        if contexts_by_leader[wallet]:
            started_wallets.append(wallet)

    return started_wallets


def _get_clob_wrapper_from_user_state(user_state: UserRuntimeState) -> Any | None:
    """Best-effort access to the ClobClientWrapper.

    Preferred source is user_state.clob_client. The wrapper should expose
    `.api_creds`, set by create_clob_client() after create_or_derive_api_key().
    """
    for attr_name in (
        "clob_client",
        "client",
        "clob_wrapper",
        "clob",
    ):
        value = getattr(user_state, attr_name, None)
        if value is not None:
            return value

    execution_service = getattr(user_state, "execution_service", None)
    if execution_service is not None:
        for attr_name in (
            "clob_client",
            "_clob_client",
            "client",
            "_client",
        ):
            value = getattr(execution_service, attr_name, None)
            if value is not None:
                return value

    clob_gateway = getattr(user_state, "clob_gateway", None)
    if clob_gateway is not None:
        for attr_name in (
            "clob_client",
            "_clob_client",
            "client",
            "_client",
            "wrapper",
            "_wrapper",
        ):
            value = getattr(clob_gateway, attr_name, None)
            if value is not None:
                return value

    return None


def _normalise_user_ws_api_creds(raw_creds: Any) -> dict[str, str]:
    """Normalize py-clob-client API creds into UserWebSocket's expected shape."""

    if raw_creds is None:
        return {
            "api_key": "",
            "api_secret": "",
            "api_passphrase": "",
        }

    def read(*names: str) -> str:
        if isinstance(raw_creds, dict):
            for name in names:
                value = raw_creds.get(name)
                if value:
                    return str(value)

        for name in names:
            value = getattr(raw_creds, name, None)
            if value:
                return str(value)

        return ""

    return {
        "api_key": read("api_key", "key", "apiKey"),
        "api_secret": read("api_secret", "secret", "apiSecret"),
        "api_passphrase": read(
            "api_passphrase",
            "passphrase",
            "apiPassphrase",
        ),
    }


def _user_ws_api_creds(user_state: UserRuntimeState) -> dict[str, str]:
    """Read already-derived CLOB API credentials from the CLOB client wrapper.

    Do not read these from AppConfig or wallet config. The bot uses one private
    key to create the L1 signer, then create_or_derive_api_key() produces the
    CLOB API creds used for user websocket auth.
    """
    wrapper = _get_clob_wrapper_from_user_state(user_state)

    if wrapper is None:
        return {
            "api_key": "",
            "api_secret": "",
            "api_passphrase": "",
        }

    for attr_name in (
        "api_creds",
        "user_api_creds",
        "creds",
    ):
        raw_creds = getattr(wrapper, attr_name, None)
        creds = _normalise_user_ws_api_creds(raw_creds)
        if all(creds.values()):
            return creds

    inner_client = getattr(wrapper, "client", None)
    if inner_client is not None:
        for attr_name in (
            "api_creds",
            "creds",
            "_api_creds",
            "_creds",
        ):
            raw_creds = getattr(inner_client, attr_name, None)
            creds = _normalise_user_ws_api_creds(raw_creds)
            if all(creds.values()):
                return creds

    return {
        "api_key": "",
        "api_secret": "",
        "api_passphrase": "",
    }


async def _start_market_data_runtimes(
    *,
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    live_positions_by_user: dict[str, list[dict]],
    contexts_by_leader: dict[str, list["TradingContext"]],
    readiness,
    config,
) -> dict[str, MarketDataRuntime]:
    """Start WSS-backed market data runtimes, keyed by user_id.

    Trade detection remains HTTP polling. These runtimes only keep market
    quote/book/tick state fresh for execution, risk, and near-one exits. The
    caller must attach the matching runtime to each user's TradingContext so
    the trade pipeline can subscribe new signal tokens dynamically.
    """
    market_data_runtimes: dict[str, MarketDataRuntime] = {}
    context_tokens = extract_token_ids_from_contexts(
        _flatten_contexts(contexts_by_leader)
    )

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)

        if user_state is None:
            logger.error(
                "market_data_runtime_skipped",
                extra={
                    "user_id": runtime_user.user_id,
                    "reason": "missing user runtime state",
                },
            )
            continue

        market_store = getattr(user_state, "market_store", None)
        clob_gateway = getattr(user_state, "clob_gateway", None)

        if market_store is None or clob_gateway is None:
            logger.warning(
                "market_data_runtime_skipped",
                extra={
                    "user_id": runtime_user.user_id,
                    "reason": "missing market_store or clob_gateway",
                },
            )
            continue

        live_position_tokens = extract_token_ids_from_live_positions(
            live_positions_by_user.get(runtime_user.user_id, [])
        )

        initial_tokens = sorted(set(live_position_tokens) | set(context_tokens))

        market_runtime = MarketDataRuntime(
            market_store=market_store,
            gateway=clob_gateway,
            readiness=readiness,
            config=config,
        )

        await market_runtime.start(initial_tokens)
        market_data_runtimes[runtime_user.user_id] = market_runtime

        logger.info(
            "market_data_runtime_started_for_user",
            extra={
                "user_id": runtime_user.user_id,
                "subscribed_token_count": len(initial_tokens),
            },
        )

    return market_data_runtimes


def _attach_market_data_runtime_to_contexts(
    *,
    contexts_by_leader: dict[str, list["TradingContext"]],
    risk_contexts_by_user: dict[str, "TradingContext"],
    runtimes_by_user: dict[str, MarketDataRuntime],
) -> None:
    """Bind each user's MarketDataRuntime onto their TradingContext.

    Trade pipeline reads ``context.market_data_runtime.subscribe_tokens`` to
    register new signal tokens with WSS before placing orders.
    """
    for contexts in contexts_by_leader.values():
        for context in contexts:
            runtime = runtimes_by_user.get(context.user_id)
            if runtime is not None:
                context.market_data_runtime = runtime
    for context in risk_contexts_by_user.values():
        runtime = runtimes_by_user.get(context.user_id)
        if runtime is not None:
            context.market_data_runtime = runtime


def _build_reconcile_wake_closure(context):
    """Return an async wake closure that converges ReconciliationService.

    Throttled by a per-context asyncio.Lock so that a burst of WSS fill events
    cannot stack reconcile passes. The closure delegates to the canonical
    ``reconcile_submitted_unconfirmed``; it never touches positions directly.
    """
    from orchestrators.trade_pipeline import reconcile_submitted_unconfirmed

    lock = asyncio.Lock()

    async def _wake(event):
        if lock.locked():
            return
        async with lock:
            try:
                await reconcile_submitted_unconfirmed(context)
            except Exception as exc:
                logger.warning(
                    "user_wss_wake_reconcile_error",
                    extra={
                        "user_id": context.user_id,
                        "error": redact_error_detail(exc),
                    },
                )

    return _wake


async def _start_user_data_runtimes(
    *,
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    readiness,
    risk_contexts_by_user: dict[str, "TradingContext"] | None = None,
) -> list[UserDataRuntime]:
    """Start user WSS runtimes.

    User WSS is authenticated with CLOB API credentials derived by
    create_clob_client(). It sets wss_user_ok=True only after auth success.

    When ``risk_contexts_by_user`` is provided, each user's runtime gets a
    wake-reconciler closure so own fills converge through the canonical
    ReconciliationService without waiting for the next polled cycle.
    """
    user_data_runtimes: list[UserDataRuntime] = []

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)

        if user_state is None:
            logger.error(
                "user_data_runtime_skipped",
                extra={
                    "user_id": runtime_user.user_id,
                    "reason": "missing user runtime state",
                },
            )
            continue

        market_store = getattr(user_state, "market_store", None)
        if market_store is None:
            logger.warning(
                "user_data_runtime_skipped",
                extra={
                    "user_id": runtime_user.user_id,
                    "reason": "missing market_store",
                },
            )
            continue

        api_creds = _user_ws_api_creds(user_state)

        wake_closure = None
        if risk_contexts_by_user is not None:
            user_context = risk_contexts_by_user.get(runtime_user.user_id)
            if user_context is not None:
                wake_closure = _build_reconcile_wake_closure(user_context)

        runtime = UserDataRuntime(
            market_store=market_store,
            readiness=readiness,
            api_creds=api_creds,
            user_id=runtime_user.user_id,
            on_reconcile_signal=wake_closure,
        )

        await runtime.start()
        user_data_runtimes.append(runtime)

    return user_data_runtimes


async def _subscribe_context_tokens(
    *,
    market_data_runtimes: dict[str, MarketDataRuntime],
    contexts_by_leader: dict[str, list],
) -> None:
    """Subscribe tokens visible from runtime contexts.

    New trade-signal tokens should also be subscribed from the trade pipeline
    when they appear. This helper only handles context-visible tokens.
    """
    token_ids = extract_token_ids_from_contexts(
        _flatten_contexts(contexts_by_leader)
    )

    if not token_ids:
        return

    for runtime in market_data_runtimes.values():
        try:
            await runtime.subscribe_tokens(token_ids)
        except Exception as exc:
            logger.warning(
                "market_data_dynamic_subscribe_failed",
                extra={"error": redact_error_detail(exc)},
            )


async def _shutdown_runtime(
    *,
    monitor_tasks: list[asyncio.Task],
    discovery_task: asyncio.Task | None,
    risk_task: asyncio.Task | None,
    near_one_tasks: list[asyncio.Task],
    market_data_runtimes: dict[str, MarketDataRuntime],
    user_data_runtimes: list[UserDataRuntime],
    metrics_runner,
) -> None:
    shutdown_tasks = list(monitor_tasks)

    if discovery_task:
        shutdown_tasks.append(discovery_task)
    if risk_task:
        shutdown_tasks.append(risk_task)
    shutdown_tasks.extend(near_one_tasks)

    for task in shutdown_tasks:
        task.cancel()

    if shutdown_tasks:
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)

    for runtime in user_data_runtimes:
        try:
            await runtime.stop()
        except Exception as exc:
            logger.warning(
                "user_data_runtime_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )

    for runtime in market_data_runtimes.values():
        try:
            await runtime.stop()
        except Exception as exc:
            logger.warning(
                "market_data_runtime_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )

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
        application = build_app()
    except EnvValidationError as exc:
        print_error(str(exc))
        return

    application.readiness.update(
        wss_market_ok=False,
        wss_market_connecting=False,
        wss_user_ok=False,
        wss_user_connecting=False,
        market_data_ready=False,
    )

    from bot_cli.commands.doctor import run_doctor_checks

    mode = "paper" if application.config.execution.paper_trade_only else "live"

    if mode == "live":
        doctor_result = await run_doctor_checks("live")
        if doctor_result.get("status") == "blocked":
            print_error("Live mode doctor check failed. Refusing to start.")
            import json

            print(json.dumps(doctor_result, indent=2))
            return
    elif application.config.network.enforce_geoblock:
        decision = await check_geoblock()
        if not decision.allowed:
            print_error(f"Trading disabled by geoblock policy: {decision.reason}")
            return

    configured = configured_leaders(application.config)
    invalid = [wallet for wallet in configured if not is_valid_eth_address(wallet)]

    if invalid:
        for wallet in invalid:
            print_error(
                f'Invalid leader wallet "{wallet}" — must be 42-char hex '
                "(0x + 40 hex digits)."
            )
        return

    runtime_users = load_runtime_users(configured, application.config)
    if not runtime_users:
        print_error("No runtime users configured.")
        return

    application.readiness.update(runtime_user_count=len(runtime_users))

    db_path = application.db_path
    application.readiness.update(db_ok=True)

    from utils.idempotency import purge_stale_records

    purged = purge_stale_records()
    if purged:
        logger.info("Startup cleanup: purged %s stale records", purged)

    research_capture_service = DiscoveryCaptureService(db_path)
    leaders_repo = application.leaders_repo
    registry = application.registry
    refresh_interval = application.config.discovery.discovery_interval_seconds

    active_leaders = await _wait_for_active_leaders(
        registry,
        configured,
        refresh_interval,
        config=application.config,
    )
    ensure_runtime_follows(runtime_users, active_leaders, application.config)

    user_states: dict[str, UserRuntimeState] = {}

    for runtime_user in runtime_users:
        client = await create_clob_client(wallet_config=runtime_user.wallet)
        user_states[runtime_user.user_id] = build_user_state(
            runtime_user,
            client,
            db_path,
            application.config,
        )

    application.readiness.update(
        gateway_ok=True,
        circuit_breaker_state="closed",
    )

    leader_balances_raw = await asyncio.gather(
        *(get_my_balance(wallet) for wallet in active_leaders)
    )
    user_balances_raw = await asyncio.gather(
        *(
            get_my_balance(runtime_user.wallet.funder_address)
            for runtime_user in runtime_users
        )
    )

    user_balances = {
        runtime_user.user_id: float(balance or 0.0)
        for runtime_user, balance in zip(runtime_users, user_balances_raw)
    }

    api_ok = all(value is not None for value in leader_balances_raw) and all(
        value is not None for value in user_balances_raw
    )
    application.readiness.update(api_ok=api_ok)

    live_positions_by_user: dict[str, list[dict]] = {}
    health_reports_by_user = {}

    for runtime_user in runtime_users:
        live_positions = await fetch_live_positions(runtime_user.wallet.funder_address)
        live_positions_by_user[runtime_user.user_id] = live_positions

        health_reports_by_user[runtime_user.user_id] = await perform_startup_health_check(
            wallet=runtime_user.wallet.funder_address,
            user_id=runtime_user.user_id,
            db_path=db_path,
            positions_repo=user_states[runtime_user.user_id].positions_repo,
            live_positions=live_positions,
            available_cash=float(user_balances.get(runtime_user.user_id, 0.0)),
            database_ok=True,
            rpc_url=application.config.wallet.polygon_rpc_url,
            clob_gateway=user_states[runtime_user.user_id].clob_gateway,
            require_live_clob=not application.config.dry_run,
        )

    contexts_by_leader, risk_contexts_by_user = build_context_maps(
        runtime_users,
        user_states,
        active_leaders,
        application.config,
        leaders_repo=leaders_repo,
        readiness=application.readiness,
    )

    monitored_leaders = [
        leader
        for leader, contexts in contexts_by_leader.items()
        if contexts
    ]
    application.readiness.update(monitored_leader_count=len(monitored_leaders))

    if not monitored_leaders:
        logger.warning(
            "no_leaders_monitored",
            extra={"reason": "no_user_follows_active_leaders"},
        )
        metrics.inc("no_leaders_monitored_total")

    market_data_runtimes = await _start_market_data_runtimes(
        runtime_users=runtime_users,
        user_states=user_states,
        live_positions_by_user=live_positions_by_user,
        contexts_by_leader=contexts_by_leader,
        readiness=application.readiness,
        config=application.config,
    )

    _attach_market_data_runtime_to_contexts(
        contexts_by_leader=contexts_by_leader,
        risk_contexts_by_user=risk_contexts_by_user,
        runtimes_by_user=market_data_runtimes,
    )

    user_data_runtimes = await _start_user_data_runtimes(
        runtime_users=runtime_users,
        user_states=user_states,
        readiness=application.readiness,
        risk_contexts_by_user=risk_contexts_by_user,
    )

    startup_reconciled = await startup_reconcile_unconfirmed(
        list(risk_contexts_by_user.values()),
    )

    application.readiness.update(reconciler_ok=True)

    if startup_reconciled:
        application.readiness.update(
            startup_reconciled_orders=sum(startup_reconciled.values()),
        )

    first_user = runtime_users[0]
    live_positions = live_positions_by_user.get(first_user.user_id, [])
    health_report = health_reports_by_user[first_user.user_id]

    live_positions_value = sum(
        float(position.get("currentValue") or 0.0)
        for position in live_positions
    )
    available_cash = float(user_balances.get(first_user.user_id, 0.0))

    all_health_reports = list(health_reports_by_user.values())
    startup_health_healthy = all(report.healthy for report in all_health_reports)
    startup_health_status = "healthy" if startup_health_healthy else "unhealthy"
    startup_pruned_positions = sum(
        report.position_sync.pruned_positions
        for report in all_health_reports
    )
    startup_purged_failed_orders = sum(
        report.order_cleanup.purged_failed
        for report in all_health_reports
    )

    for runtime_user in runtime_users[1:]:
        secondary_report = health_reports_by_user.get(runtime_user.user_id)
        if secondary_report is not None and not secondary_report.healthy:
            logger.warning(
                "secondary_startup_health_unhealthy",
                extra={
                    "user_id": runtime_user.user_id,
                    "wallet": runtime_user.wallet.funder_address,
                    "status": secondary_report.status,
                },
            )

    application.readiness.update(
        startup_health_status=startup_health_status,
        startup_health_healthy=startup_health_healthy,
        startup_pruned_positions=startup_pruned_positions,
        startup_purged_failed_orders=startup_purged_failed_orders,
    )

    print_startup_summary(
        wallet=first_user.wallet.funder_address,
        available_cash=available_cash,
        total_portfolio=available_cash + live_positions_value,
        positions=live_positions,
        active_leaders=monitored_leaders,
        config=application.config,
        health_report=health_report,
    )

    metrics_runner = None

    if application.config.network.enable_metrics_server:
        metrics_runner = await start_health_server(
            application.config.network.metrics_port,
            tracker=application.readiness,
        )

    monitor_tasks: list[asyncio.Task] = []
    monitored_set: set[str] = set(monitored_leaders)
    fetch_interval = application.config.polling.fetch_interval

    async def _on_new_leaders(new_wallets: list[str]) -> None:
        started_wallets = _refresh_contexts_for_new_leaders(
            new_wallets=new_wallets,
            runtime_users=runtime_users,
            user_states=user_states,
            contexts_by_leader=contexts_by_leader,
            leaders_repo=leaders_repo,
            config=application.config,
            readiness=application.readiness,
        )

        monitored_leader_count = len(
            [
                wallet
                for wallet, contexts in contexts_by_leader.items()
                if contexts
            ]
        )

        await _subscribe_context_tokens(
            market_data_runtimes=market_data_runtimes,
            contexts_by_leader=contexts_by_leader,
        )

        for wallet in started_wallets:
            monitor_tasks.append(
                asyncio.create_task(
                    run_leader_monitor(
                        wallet,
                        contexts_by_leader[wallet],
                        readiness_tracker=application.readiness,
                        config=application.config,
                        fetch_interval=fetch_interval,
                        monitored_leader_count=monitored_leader_count,
                        research_capture_service=research_capture_service,
                    )
                )
            )
            logger.info("dynamic_leader_monitor_started", extra={"leader": wallet})

        application.readiness.update(
            monitored_leader_count=len(
                [
                    wallet
                    for wallet, contexts in contexts_by_leader.items()
                    if contexts
                ]
            )
        )

    discovery_task = None

    if application.config.discovery.enable_trader_discovery:
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

    if application.config.risk.enable_risk_mgmt:
        from services.riskMonitor import risk_monitor_loop_for_contexts

        risk_task = asyncio.create_task(
            risk_monitor_loop_for_contexts(
                list(risk_contexts_by_user.values()),
                readiness_tracker=application.readiness,
                config=application.config,
            )
        )

    near_one_tasks: list[asyncio.Task] = []

    if application.config.near_one_exit.enable_near_one_exit:
        from services.near_one_exit import NearOneExitService

        for runtime_user in runtime_users:
            user_state = user_states.get(runtime_user.user_id)
            if user_state is None or user_state.market_data_service is None:
                continue
            near_one_tasks.append(
                asyncio.create_task(
                    NearOneExitService(
                        position_manager=user_state.position_manager,
                        execution_service=user_state.execution_service,
                        market_data_service=user_state.market_data_service,
                        config=application.config,
                        readiness=application.readiness,
                        user_id=runtime_user.user_id,
                        funder_address=runtime_user.wallet.funder_address,
                        execution_coordinator=getattr(user_state, "execution_coordinator", None),
                    ).loop(),
                    name=f"near-one-exit-{runtime_user.user_id}",
                )
            )

    monitor_tasks.extend(
        asyncio.create_task(
            run_leader_monitor(
                wallet,
                contexts_by_leader[wallet],
                readiness_tracker=application.readiness,
                config=application.config,
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
                    exc = task.exception()
                    if exc:
                        logger.error(
                            "monitor_task_crashed",
                            extra={"error": redact_error_detail(exc)},
                        )
            else:
                await asyncio.sleep(refresh_interval)
                metrics.inc("no_leaders_monitored_total")

    finally:
        await _shutdown_runtime(
            monitor_tasks=monitor_tasks,
            discovery_task=discovery_task,
            risk_task=risk_task,
            near_one_tasks=near_one_tasks,
            market_data_runtimes=market_data_runtimes,
            user_data_runtimes=user_data_runtimes,
            metrics_runner=metrics_runner,
        )


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    main_task = loop.create_task(main())

    def _shutdown_handler(sig, _frame):
        logger.info(
            "Received %s — initiating graceful shutdown",
            signal.Signals(sig).name,
        )
        main_task.cancel()

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    try:
        loop.run_until_complete(main_task)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    except Exception as exc:
        import sys
        import traceback

        print_error(f"Fatal startup error: {exc}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        loop.close()
