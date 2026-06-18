from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

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
    import traceback

    # Pre-fix this was a WARNING-and-continue. A secrets-load failure
    # (missing python-dotenv, permission error on secrets.env, malformed
    # runtime env file) silently let the bot proceed with un-hydrated
    # ENV; in the worst case a partial env from a prior process let
    # validate_runtime_env appear to pass on a stale snapshot. Boot
    # failure is the correct fail-closed response.
    print(f"[FATAL] Failed to load runtime env: {exc}", file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)

from bootstrap import build_app
from config.app_config import EnvValidationError, load_config
from config.env import is_valid_eth_address, validate_runtime_env
from orchestrators.auto_trader_bootstrap import resolve_startup_leaders
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
    hydrate_resolved_market_state,
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
from services.discovery.auto_trader_selector import AutoTraderSelector
from services.discovery.balance_provider import build_wallet_balance_provider
from services.discovery.capture_service import DiscoveryCaptureService
from services.discovery.position_snapshot_service import (
    SnapshotResult,
    snapshot_followed_leaders,
)
from services.tradeMonitor import _guarded_leader_fetch
from services.healthServer import start_health_server
from services.metrics import metrics
from services.multiUserConfig import RuntimeUser, UserFollowConfig, load_runtime_users
from storage.postgres.leaders_repo import LeadersRepo
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


def _to_decimal_balance(value: object) -> Decimal:
    """Coerce a raw venue balance to Decimal at the startup boundary.

    ``get_my_balance`` may return ``None`` (API down), a string, a float,
    or already a Decimal. We normalise once so the entire startup health
    pass holds money as Decimal — CLAUDE.md non-negotiable.
    """
    if value is None or value == "":
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        # Review fix (greptile P2 on PR #110): surface the parse failure
        # so an operator can spot a bad upstream payload (e.g. a venue
        # rate-limit string instead of a numeric balance) rather than
        # silently undercounting the portfolio at startup. We still
        # return ``Decimal("0")`` so the ``_balance_check`` path can
        # raise the "zero cash balance" health item — the warning makes
        # the root cause observable instead of leaving it as a mystery.
        logger.warning(
            "balance_parse_failed | raw=%r — defaulting to 0",
            value,
        )
        return Decimal("0")


def _flatten_contexts(
    contexts_by_leader: dict[str, list["TradingContext"]],
) -> list["TradingContext"]:
    return [
        context
        for contexts in contexts_by_leader.values()
        for context in contexts
    ]


async def _reap_reservations_once(contexts: list["TradingContext"]) -> None:
    """Reconcile-and-release TTL-expired reservations across *contexts*.

    Standalone equivalent of the risk loop's in-session reaper, used when risk
    management is disabled. Reuses the vetted per-context reaper, which only
    releases reservations whose linked order reconciliation confirms is
    venue-terminal — duplicate-order / stale-lock protections are preserved.
    See ``docs/concurrency-audit-response.md`` (C8-adjacent gap).
    """
    from services.risk.risk_monitor import _reap_expired_reservations_for_context

    for ctx in contexts:
        try:
            await _reap_expired_reservations_for_context(ctx)
        except Exception as exc:
            # Match the risk-monitor reaper convention (risk_monitor.py): a
            # persistent failure here strands capital/shares, so it must be
            # visible in prod (where DEBUG is suppressed) via an error log with
            # the user_id and a metric — not swallowed at debug level.
            metrics.inc("reservation_reaper_failed_total")
            logger.error(
                "reservation_reaper_failed user=%s: %r",
                getattr(ctx, "user_id", "?"), exc,
            )


def _wallets_blocked_by_reconcile_failures(
    started_wallets: list[str],
    contexts_by_leader: dict[str, list["TradingContext"]],
    failed_user_ids: list[str],
) -> list[str]:
    """Return wallets whose downstream contexts include any failed user_id.

    Used by the dynamic-leader discovery refresh to refuse starting a
    leader monitor when startup reconciliation failed for a user under
    that leader: starting the monitor on stale state is the same
    double-fill exposure the cold-start path already gates against.
    """
    if not failed_user_ids:
        return []
    failed = set(failed_user_ids)
    return [
        wallet for wallet in started_wallets
        if any(
            ctx.user_id in failed
            for ctx in contexts_by_leader.get(wallet, [])
        )
    ]


def _cold_start_monitored_after_reconcile(
    monitored_leaders: list[str],
    contexts_by_leader: dict[str, list["TradingContext"]],
    reconcile_failures: list[str],
) -> list[str]:
    """Filter the cold-start leader set so monitors do NOT start for wallets
    whose users failed startup reconciliation.

    Starting a leader monitor on an un-reconciled snapshot is the double-fill
    exposure (an unconfirmed prior-session fill is invisible, so the next
    leader signal re-buys). The dynamic-discovery path already refuses such
    wallets via ``_wallets_blocked_by_reconcile_failures``; the cold-start path
    must apply the same gate instead of merely recording ``reconciler_ok``.
    """
    blocked = set(
        _wallets_blocked_by_reconcile_failures(
            monitored_leaders, contexts_by_leader, reconcile_failures
        )
    )
    if not blocked:
        return monitored_leaders
    return [wallet for wallet in monitored_leaders if wallet not in blocked]


async def _refresh_contexts_for_new_leaders(
    *,
    new_wallets: list[str],
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    contexts_by_leader: dict[str, list],
    leaders_repo: LeadersRepo,
    config,
    readiness,
    trust_store=None,
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

            context = await build_trading_context(
                user_state,
                follow,
                config,
                leaders_repo=leaders_repo,
                readiness=readiness,
                trust_store=trust_store,
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
                reconciled = await reconcile_submitted_unconfirmed(context)
                reservation_reaped = 0
                extra = getattr(context, "extra", {}) or {}
                reservation_service = extra.get("reservation_service")
                if reservation_service is None:
                    coordinator = getattr(context, "execution_coordinator", None)
                    reservation_service = getattr(coordinator, "reservation_service", None)
                release_expired = getattr(
                    reservation_service,
                    "release_expired_active",
                    None,
                )
                if callable(release_expired):
                    released_ids = await release_expired()
                    try:
                        reservation_reaped = len(released_ids or [])
                    except TypeError:
                        reservation_reaped = 0
                if reconciled or reservation_reaped:
                    logger.info(
                        "user_wss_wake_reconcile_complete | user=%s reconciled=%d reservations_reaped=%d order_id=%s",
                        context.user_id,
                        reconciled,
                        reservation_reaped,
                        (
                            event.get("order_id")
                            or event.get("orderID")
                            or event.get("id")
                        )
                        if isinstance(event, dict)
                        else None,
                    )
            except Exception as exc:
                logger.warning(
                    "user_wss_wake_reconcile_error",
                    extra={
                        "user_id": context.user_id,
                        "error": redact_error_detail(exc),
                    },
                )

    return _wake


def _build_risk_cache_evict_closure(context):
    """Build a sync callable that evicts a token from the per-context
    ``RiskPriceCache`` so a WSS fill on a dormant (zero-bid) market re-arms
    the price probe immediately. Returns None when the cache is not yet
    constructed; the runtime treats None as "no risk cache wired".
    """

    def _evict(token_id: str) -> None:
        cache = context.extra.get("risk_price_cache")
        if cache is None:
            return
        try:
            cache.evict(token_id)
        except Exception:
            # Cache is best-effort hygiene; never let an evict raise into
            # the WSS message loop.
            return

    return _evict


def _build_market_token_subscribe_closure(runtime: MarketDataRuntime):
    """Return a user-fill hook that subscribes newly seen tokens to MarketWS."""

    async def _subscribe(token_id: str) -> None:
        token_id = str(token_id or "").strip()
        if not token_id:
            return
        await runtime.subscribe_tokens([token_id])

    return _subscribe


async def _start_user_data_runtimes(
    *,
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    readiness,
    risk_contexts_by_user: dict[str, "TradingContext"] | None = None,
    market_data_runtimes: dict[str, MarketDataRuntime] | None = None,
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
        risk_cache_evict = None
        market_token_subscribe = None
        if risk_contexts_by_user is not None:
            user_context = risk_contexts_by_user.get(runtime_user.user_id)
            if user_context is not None:
                wake_closure = _build_reconcile_wake_closure(user_context)
                risk_cache_evict = _build_risk_cache_evict_closure(user_context)
        if market_data_runtimes is not None:
            market_runtime = market_data_runtimes.get(runtime_user.user_id)
            if market_runtime is not None:
                market_token_subscribe = _build_market_token_subscribe_closure(
                    market_runtime
                )

        runtime = UserDataRuntime(
            market_store=market_store,
            readiness=readiness,
            api_creds=api_creds,
            user_id=runtime_user.user_id,
            on_reconcile_signal=wake_closure,
            manual_sync_service=getattr(user_state, "manual_sync_service", None),
            on_risk_cache_evict=risk_cache_evict,
            on_market_token_seen=market_token_subscribe,
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


@dataclass
class _Supervised:
    """A long-lived auxiliary background loop owned by the main supervisor.

    ``factory`` must return a FRESH awaitable on every call so a crashed loop
    can be restarted by re-invoking it. The supervisor restarts a crashed loop
    up to ``max_restarts`` times within a rolling window; surviving longer than
    ``recovery_seconds`` resets the counter (transient blips don't accumulate).
    Once the budget is exhausted the supervisor fail-closes the whole process
    rather than limp along with a dead safety subsystem.
    """

    name: str
    factory: Callable[[], Awaitable[None]]
    max_restarts: int = 5
    recovery_seconds: float = 60.0
    task: asyncio.Task | None = None
    restarts: int = 0
    started: float = 0.0

    def spawn(self) -> asyncio.Task:
        self.started = asyncio.get_running_loop().time()
        self.task = asyncio.create_task(self.factory(), name=self.name)
        return self.task


def _live_monitor_count(monitor_tasks: list[asyncio.Task]) -> int:
    """Count leader-monitor tasks that are still running.

    P2-h accounting helper: after a leader monitor crashes (its task is
    ``done`` at that point), the readiness ``monitored_leader_count`` gauge must
    be re-derived from the survivors so health/metrics stop advertising a
    monitor that no longer exists. Kept pure for unit-testing.
    """
    return sum(1 for task in monitor_tasks if not task.done())


def _on_supervised_crash(spec: _Supervised, now: float) -> str:
    """Pure restart decision for a crashed supervised loop.

    Mutates ``spec.restarts`` and returns ``"restart"`` or ``"fail_closed"``.
    Kept free of asyncio/logging so it is unit-testable in isolation.
    """
    if now - spec.started >= spec.recovery_seconds:
        spec.restarts = 0  # survived long enough — reset before counting this crash
    spec.restarts += 1
    return "fail_closed" if spec.restarts >= spec.max_restarts else "restart"


async def _shutdown_runtime(
    *,
    monitor_tasks: list[asyncio.Task],
    supervised: list[_Supervised],
    market_data_runtimes: dict[str, MarketDataRuntime],
    user_data_runtimes: list[UserDataRuntime],
    metrics_runner,
) -> None:
    shutdown_tasks = list(monitor_tasks)

    # Cancel the CURRENT task ref of each supervised loop (a restarted loop
    # holds a new task object, so we must read spec.task here, not a captured
    # reference from startup).
    shutdown_tasks.extend(spec.task for spec in supervised if spec.task)

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

    # Honour the connection layer's documented shutdown contract: the
    # process-wide asyncpg pool is opened during startup and must be closed
    # here. Idempotent (no-op when the pool was never initialised, e.g. paper
    # mode with no DATABASE_URL) and warning-only so a teardown hiccup never
    # masks the real shutdown reason.
    try:
        from storage.postgres.connection import shutdown_pool

        await shutdown_pool()
    except Exception as exc:
        logger.warning(
            "db_pool_shutdown_failed",
            extra={"error": redact_error_detail(exc)},
        )


async def main() -> int:
    configure_logging(level="INFO")

    try:
        validate_runtime_env()
        startup_config = load_config()
    except EnvValidationError as exc:
        print_error(str(exc))
        return 1

    from bot_cli.commands.doctor import run_doctor_checks

    mode = "paper" if startup_config.execution.paper_trade_only else "live"

    if mode == "live":
        doctor_result = await run_doctor_checks("live")
        if doctor_result.get("status") == "blocked":
            print_error("Live mode doctor check failed. Refusing to start.")
            blocked = [
                c for c in doctor_result.get("checks", [])
                if isinstance(c, dict) and c.get("status") == "blocked"
            ]
            for check in blocked:
                print_error(
                    f"  ❌ {check.get('name')}: {check.get('detail', '')}"
                )
                fix = check.get("fix")
                if fix:
                    print_error(f"     fix: {fix}")
            import json

            print(json.dumps(doctor_result, indent=2))
            return 1
        if doctor_result.get("status") == "warning":
            warnings = [
                c for c in doctor_result.get("checks", [])
                if isinstance(c, dict) and c.get("status") == "warning"
            ]
            for check in warnings:
                logger.warning(
                    "doctor_warning | name=%s detail=%s fix=%s",
                    check.get("name"),
                    check.get("detail"),
                    check.get("fix"),
                )
    elif startup_config.network.enforce_geoblock:
        decision = await check_geoblock()
        if not decision.allowed:
            print_error(f"Trading disabled by geoblock policy: {decision.reason}")
            return 1

    application = await build_app()

    application.readiness.update(
        wss_market_ok=False,
        wss_market_connecting=False,
        wss_user_ok=False,
        wss_user_connecting=False,
        market_data_ready=False,
    )

    configured = configured_leaders(application.config)
    invalid = [wallet for wallet in configured if not is_valid_eth_address(wallet)]

    if invalid:
        for wallet in invalid:
            print_error(
                f'Invalid leader wallet "{wallet}" — must be 42-char hex '
                "(0x + 40 hex digits)."
            )
        return 1

    startup_selection = await resolve_startup_leaders(
        configured_leaders=configured,
        config=application.config,
        selector=AutoTraderSelector(
            repo=application.discovery_repo,
            config=application.config,
            balance_provider=build_wallet_balance_provider(application.config),
        ),
        discovery_service=application.discovery_service,
    )
    active_leaders = startup_selection.active_leaders

    invalid_active = [
        wallet for wallet in active_leaders if not is_valid_eth_address(wallet)
    ]
    if invalid_active:
        logger.error(
            "startup_blocked_no_active_leaders",
            extra={
                "mode": startup_selection.mode,
                "reason": "invalid_active_leaders",
                "invalid_wallets": invalid_active,
                "allow_auto_trader_detection": (
                    application.config.discovery.allow_auto_trader_detection
                ),
                "max_auto_traders_count": (
                    application.config.discovery.max_auto_traders_count
                ),
            },
        )
        print_error("No active leaders available. Auto-selected leaders were invalid.")
        return 1

    if not active_leaders:
        logger.error(
            "startup_blocked_no_active_leaders",
            extra={
                "mode": startup_selection.mode,
                "reason": startup_selection.reason,
                "allow_auto_trader_detection": (
                    application.config.discovery.allow_auto_trader_detection
                ),
                "max_auto_traders_count": (
                    application.config.discovery.max_auto_traders_count
                ),
            },
        )
        print_error(
            "No active leaders available. Manual LEADER_WALLETS are empty and "
            "auto trader detection did not find eligible candidates."
        )
        return 1

    runtime_users = load_runtime_users(active_leaders, application.config)
    if not runtime_users:
        print_error("No runtime users configured.")
        return 1

    application.readiness.update(runtime_user_count=len(runtime_users))

    application.readiness.update(db_ok=True)

    # Greptile P1 (PR #107): the bot used to reach
    # ``DiscoveryCaptureService.create()`` without ever calling
    # ``connect_db()`` — that service internally calls ``get_pool()``
    # which would raise ``RuntimeError`` and crash startup. Call
    # ``connect_db()`` here so the asyncpg pool exists before any
    # downstream component asks for it.
    from config.db import connect_db as _connect_db

    await _connect_db()

    from storage.postgres.connection import get_pool
    from storage.postgres.orders_repo import OrdersRepo as _PgOrdersRepo

    _max_age = float(application.config.polling.retention_max_age_seconds)
    # Acquire the pool first so it's available to the per-user wiring below
    # even when the (non-critical) purge step fails. Previously `_pool` was
    # scoped inside the try/except, so a degraded purge would NameError on
    # the subsequent `build_user_state(..., _pool, ...)` reference.
    _pool = await get_pool()
    try:
        purged = await _PgOrdersRepo(_pool).purge_old_processed_trades(_max_age)
    except RuntimeError:
        # Defensive: should not fire now that connect_db() runs above,
        # but kept so a degraded PG init does not block startup on a
        # non-critical purge.
        purged = 0
    if purged:
        logger.info("Startup cleanup: purged %s stale records", purged)

    research_capture_service = await DiscoveryCaptureService.create()
    leaders_repo = application.leaders_repo
    registry = application.registry
    refresh_interval = application.config.discovery.discovery_interval_seconds

    ensure_runtime_follows(runtime_users, active_leaders, application.config)

    user_states: dict[str, UserRuntimeState] = {}

    for runtime_user in runtime_users:
        client = await create_clob_client(wallet_config=runtime_user.wallet, config=application.config)
        user_states[runtime_user.user_id] = build_user_state(
            runtime_user,
            client,
            _pool,
            application.config,
        )

    logger.info(
        "startup_stage | stage=clob_clients_ready users=%d",
        len(user_states),
    )
    application.readiness.update(
        gateway_ok=True,
        circuit_breaker_state="closed",
    )

    logger.info(
        "startup_stage | stage=balance_checks_started leaders=%d users=%d",
        len(active_leaders),
        len(runtime_users),
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

    # INF-01 (CLAUDE.md no-float-money): keep balances as ``Decimal`` for
    # the entire startup health / cap / threshold pass; downstream
    # consumers cast to float only at display boundaries.
    user_balances: dict[str, Decimal] = {
        runtime_user.user_id: _to_decimal_balance(balance)
        for runtime_user, balance in zip(runtime_users, user_balances_raw)
    }

    api_ok = all(value is not None for value in leader_balances_raw) and all(
        value is not None for value in user_balances_raw
    )
    logger.info(
        "startup_stage | stage=balance_checks_completed api_ok=%s",
        api_ok,
    )
    application.readiness.update(api_ok=api_ok)

    live_positions_by_user: dict[str, list[dict]] = {}
    health_reports_by_user = {}

    for runtime_user in runtime_users:
        logger.info(
            "startup_stage | stage=live_positions_fetch_started user=%s",
            runtime_user.user_id,
        )
        # None == fetch failed (fail-closed); the health-check prune treats it
        # distinctly from a genuinely-empty snapshot. Store [] for downstream
        # consumers but pass the raw value (None-aware) to the health check.
        live_positions = await fetch_live_positions(runtime_user.wallet.funder_address)
        live_positions_by_user[runtime_user.user_id] = live_positions or []
        logger.info(
            "startup_stage | stage=live_positions_fetch_completed user=%s positions=%s",
            runtime_user.user_id,
            "fetch_failed" if live_positions is None else len(live_positions),
        )

        logger.info(
            "startup_stage | stage=startup_health_started user=%s",
            runtime_user.user_id,
        )
        health_reports_by_user[runtime_user.user_id] = await perform_startup_health_check(
            wallet=runtime_user.wallet.funder_address,
            user_id=runtime_user.user_id,
            pool=_pool,
            positions_repo=user_states[runtime_user.user_id].positions_repo,
            # PR #105 review: must pass orders_repo so the atomic-lock gate
            # actually probes the repo. Without it the check fails closed
            # (was a silent warning previously).
            orders_repo=user_states[runtime_user.user_id].orders_repo,
            live_positions=live_positions,
            available_cash=user_balances.get(runtime_user.user_id, Decimal("0")),
            database_ok=True,
            rpc_url=application.config.wallet.polygon_rpc_url,
            clob_gateway=user_states[runtime_user.user_id].clob_gateway,
            require_live_clob=not application.config.dry_run,
        )
        _health_report = health_reports_by_user[runtime_user.user_id]
        logger.info(
            "startup_stage | stage=startup_health_completed user=%s status=%s healthy=%s",
            runtime_user.user_id,
            getattr(_health_report, "status", "unknown"),
            getattr(_health_report, "healthy", False),
        )

    contexts_by_leader, risk_contexts_by_user = await build_context_maps(
        runtime_users,
        user_states,
        active_leaders,
        application.config,
        leaders_repo=leaders_repo,
        readiness=application.readiness,
        trust_store=application.trust_store,
    )
    from orchestrators.trade_pipeline import seed_position_pct_cap_snapshot

    seeded_user_ids: set[str] = set()
    for context in risk_contexts_by_user.values():
        seed_position_pct_cap_snapshot(
            context,
            user_balances.get(context.user_id, Decimal("0")),
        )
        seeded_user_ids.add(context.user_id)
    for contexts in contexts_by_leader.values():
        for context in contexts:
            if context.user_id in seeded_user_ids:
                continue
            seed_position_pct_cap_snapshot(
                context,
                user_balances.get(context.user_id, Decimal("0")),
            )
            seeded_user_ids.add(context.user_id)

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

    # CLAUDE.md non-negotiable: startup reconciliation MUST run before risk /
    # trade loops AND before the user WSS attaches its on_reconcile_signal
    # callback. Pre-fix the WSS runtime started first and could fire
    # reconcile-wake events while startup_reconcile_unconfirmed was still
    # running on the same orders, double-applying fills under the race.
    reconcile_failures: list[str] = []
    logger.info(
        "startup_stage | stage=startup_reconcile_started users=%d",
        len(risk_contexts_by_user),
    )
    startup_reconciled = await startup_reconcile_unconfirmed(
        list(risk_contexts_by_user.values()),
        failures_out=reconcile_failures,
    )
    logger.info(
        "startup_stage | stage=startup_reconcile_completed failures=%d",
        len(reconcile_failures),
    )

    if reconcile_failures:
        # Refuse to advertise readiness; downstream monitors will see
        # reconciler_ok=False and stay closed. Operators must inspect the
        # WARNING logs emitted by startup_reconcile_unconfirmed for the
        # affected users.
        logger.error(
            "startup_reconcile_partial_failure | failed_users=%s — "
            "reconciler_ok stays False, monitors for affected wallets blocked",
            reconcile_failures,
        )
        application.readiness.update(reconciler_ok=False)

        # Fail closed: actually remove the affected wallets from the monitored
        # set. Recording reconciler_ok=False is not enough — no monitor reads
        # it, so without this filter the leader monitors would still spawn and
        # re-buy on un-reconciled state (B4-01 / P0-4).
        _before = len(monitored_leaders)
        monitored_leaders = _cold_start_monitored_after_reconcile(
            monitored_leaders, contexts_by_leader, reconcile_failures
        )
        if len(monitored_leaders) != _before:
            logger.error(
                "startup_reconcile_block_monitors | kept=%d of %d wallets "
                "(blocked wallets had a user with a failed startup reconcile)",
                len(monitored_leaders),
                _before,
            )
            application.readiness.update(
                monitored_leader_count=len(monitored_leaders)
            )
    else:
        application.readiness.update(reconciler_ok=True)

    # Boot ordering (read carefully — the race fix lives here):
    #
    # 1. ``manual_sync.initial_sync`` seeds positions from the venue's
    #    ``data-api /positions`` snapshot, excluding zero-value stale rows.
    # 2. ``startup_drift_check`` validates canonical DB state against live
    #    truth and timestamps every prune in ``_prune_timestamps``.
    # 3. ``hydrate_resolved_market_state`` runs LAST: marks resolved /
    #    archived / inactive markets in MarketStateStore so the risk
    #    monitor short-circuits 404-prone tokens.
    # 4. ``_start_user_data_runtimes`` finally attaches the WSS. When auth
    #    triggers ``manual_sync.trigger_sync``, the upsert path now sees
    #    the drift prune timestamps and honours the holdoff — so the
    #    eventually-consistent data-api cannot resurrect what drift just
    #    pruned, which is what produced the /price 404 + breaker loop.
    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        manual_sync = getattr(user_state, "manual_sync_service", None) if user_state else None
        if manual_sync is None:
            continue
        try:
            logger.info(
                "startup_stage | stage=manual_sync_initial_started user=%s",
                runtime_user.user_id,
            )
            await manual_sync.initial_sync()
            logger.info(
                "startup_stage | stage=manual_sync_initial_completed user=%s",
                runtime_user.user_id,
            )
        except Exception as exc:
            logger.warning(
                "manual_sync_initial_failed | user=%s error=%r",
                runtime_user.user_id, exc,
            )

    # PR7 — proactive position drift sweep. Must complete before monitors
    # start AND before the user WSS attaches so its first auth-triggered
    # manual_sync sees the prune timestamps.
    from orchestrators.runtime_bootstrap import startup_drift_check

    logger.info(
        "startup_stage | stage=startup_drift_check_started users=%d",
        len(runtime_users),
    )
    drift_per_user = await startup_drift_check(
        runtime_users=runtime_users,
        user_states=user_states,
        config=application.config,
        readiness=application.readiness,
    )
    logger.info(
        "startup_stage | stage=startup_drift_check_completed unresolved=%d",
        sum(1 for ok in drift_per_user.values() if not ok),
    )

    # Boot-time resolved-market gate. Catches markets that resolved /
    # archived / went inactive while the bot was offline (WSS
    # ``market_resolved`` only fires on live resolution). Runs AFTER
    # initial_sync + drift_check so the position set is stable.
    # Best-effort: a failure here logs and the runtime continues; the
    # dormant-token cache + 404 sentinel still bound traffic.
    try:
        logger.info("startup_stage | stage=hydrate_resolved_market_state_started")
        await hydrate_resolved_market_state(
            runtime_users=runtime_users,
            user_states=user_states,
        )
        logger.info("startup_stage | stage=hydrate_resolved_market_state_completed")
    except Exception as exc:
        logger.warning(
            "hydrate_resolved_market_state_failed",
            extra={"error": redact_error_detail(exc)},
        )

    # Start market-data WSS only after portfolio/database convergence. Starting
    # it earlier can subscribe/probe stale tokens that manual_sync has not yet
    # filtered and drift has not yet pruned.
    live_positions_by_user = {}
    for runtime_user in runtime_users:
        logger.info(
            "startup_stage | stage=live_positions_refresh_started user=%s",
            runtime_user.user_id,
        )
        _refreshed = await fetch_live_positions(runtime_user.wallet.funder_address)
        live_positions_by_user[runtime_user.user_id] = _refreshed or []
        logger.info(
            "startup_stage | stage=live_positions_refresh_completed user=%s positions=%s",
            runtime_user.user_id,
            "fetch_failed" if _refreshed is None else len(_refreshed),
        )

    logger.info("startup_stage | stage=market_data_runtimes_starting")
    market_data_runtimes = await _start_market_data_runtimes(
        runtime_users=runtime_users,
        user_states=user_states,
        live_positions_by_user=live_positions_by_user,
        contexts_by_leader=contexts_by_leader,
        readiness=application.readiness,
        config=application.config,
    )
    logger.info(
        "startup_stage | stage=market_data_runtimes_started users=%d",
        len(market_data_runtimes),
    )

    _attach_market_data_runtime_to_contexts(
        contexts_by_leader=contexts_by_leader,
        risk_contexts_by_user=risk_contexts_by_user,
        runtimes_by_user=market_data_runtimes,
    )

    # User WSS runtimes are started AFTER reconciler readiness is set AND
    # after drift_check populated prune timestamps so on_reconcile_signal
    # + manual_sync.trigger_sync(reason="wss_authenticated") respect the
    # holdoff window.
    logger.info("startup_stage | stage=user_data_runtimes_starting")
    user_data_runtimes = await _start_user_data_runtimes(
        runtime_users=runtime_users,
        user_states=user_states,
        readiness=application.readiness,
        risk_contexts_by_user=risk_contexts_by_user,
        market_data_runtimes=market_data_runtimes,
    )
    logger.info(
        "startup_stage | stage=user_data_runtimes_started users=%d",
        len(user_data_runtimes),
    )
    if drift_per_user and not all(drift_per_user.values()):
        unresolved = [u for u, ok in drift_per_user.items() if not ok]
        logger.warning(
            "startup_drift_unresolved | users=%s — affected scopes are paused",
            unresolved,
        )

    if startup_reconciled:
        application.readiness.update(
            startup_reconciled_orders=sum(startup_reconciled.values()),
        )

    first_user = runtime_users[0]
    live_positions = live_positions_by_user.get(first_user.user_id, [])
    health_report = health_reports_by_user[first_user.user_id]

    # INF-01: keep portfolio totals as Decimal end-to-end. ``currentValue``
    # is a venue-returned string/float; ``_to_decimal_balance`` normalises
    # both representations before summing so a single bad row cannot
    # silently coerce the running total back to float.
    live_positions_value: Decimal = sum(
        (_to_decimal_balance(position.get("currentValue")) for position in live_positions),
        start=Decimal("0"),
    )
    available_cash: Decimal = user_balances.get(first_user.user_id, Decimal("0"))

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

    # Boot-time position snapshot — persists leader + own wallet
    # snapshots into discovery_position_snapshots tagged with
    # wallet_role, and surfaces an in-memory tree so the new
    # TRADERS YOU'RE COPYING + YOUR POSITIONS panels can render
    # without re-fetching. Fail-soft so PG-pool-not-online (transition)
    # / data-api degradation never blocks boot.
    snapshot_result: SnapshotResult | None = None
    try:
        logger.info(
            "startup_stage | stage=boot_snapshot_started leaders=%d users=%d",
            len(monitored_leaders),
            len(runtime_users),
        )
        from storage.postgres.connection import get_pool
        from storage.postgres.discovery_repo import DiscoveryRepo

        pool = await get_pool()
        discovery_repo = DiscoveryRepo(pool)

        async def _boot_snapshot_fetcher(url: str):
            # Reuse the rate-limited leader fetcher so a 20-leader
            # watchlist does not 429 the data-api on boot. The kind
            # tag separates the boot snapshot from the live trade
            # monitor's positions probe.
            wallet = url.split("user=")[-1].split("&")[0]
            return await _guarded_leader_fetch("boot_positions", wallet, url)

        snapshot_result = await snapshot_followed_leaders(
            leader_wallets=list(monitored_leaders),
            own_wallets=[u.wallet.funder_address for u in runtime_users],
            repo=discovery_repo,
            fetcher=_boot_snapshot_fetcher,
        )
        logger.info("startup_stage | stage=boot_snapshot_completed")
    except Exception as exc:
        logger.warning(
            "boot_snapshot_skipped",
            extra={
                "reason": "snapshot_pipeline_unavailable",
                "error": repr(exc),
            },
        )

    print_startup_summary(
        wallet=first_user.wallet.funder_address,
        available_cash=available_cash,
        total_portfolio=available_cash + live_positions_value,
        positions=live_positions,
        active_leaders=monitored_leaders,
        config=application.config,
        health_report=health_report,
        snapshot_result=snapshot_result,
    )

    metrics_runner = None

    # P2-g: derive the strict-readiness heartbeat staleness budget from the
    # slowest monitored loop interval (trade-poll FETCH_INTERVAL vs risk
    # RISK_CHECK_INTERVAL), times 3 — mirroring the per-loop lag tolerance in
    # ``tradeMonitor._monitor_loop_healthy``. ReadinessTracker floors this so a
    # sub-second poll interval cannot produce a flappy budget. Configured BEFORE
    # the metrics server begins serving /health/ready so a loop that dies after
    # ticking flips the endpoint to 503 instead of staying green.
    _slowest_loop_interval = max(
        float(application.config.polling.fetch_interval),
        float(application.config.polling.risk_check_interval),
    )
    application.readiness.configure_heartbeat_budget(
        staleness_seconds=_slowest_loop_interval * 3.0,
    )

    if application.config.network.enable_metrics_server:
        metrics_runner = await start_health_server(
            application.config.network.metrics_port,
            tracker=application.readiness,
        )

    monitor_tasks: list[asyncio.Task] = []
    monitored_set: set[str] = set(monitored_leaders)
    fetch_interval = application.config.polling.fetch_interval

    async def _on_new_leaders(new_wallets: list[str]) -> None:
        started_wallets = await _refresh_contexts_for_new_leaders(
            new_wallets=new_wallets,
            runtime_users=runtime_users,
            user_states=user_states,
            contexts_by_leader=contexts_by_leader,
            leaders_repo=leaders_repo,
            config=application.config,
            readiness=application.readiness,
            trust_store=application.trust_store,
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

        # CLAUDE.md non-negotiable: startup reconciliation MUST run before
        # risk / trade loops for every context — including those wired
        # dynamically by the discovery refresh loop. Pre-fix, the new
        # monitor task started without ever running reconcile, leaving any
        # unconfirmed fills from the prior session unobserved.
        new_contexts: list["TradingContext"] = []
        for wallet in started_wallets:
            new_contexts.extend(contexts_by_leader.get(wallet, []))
        dynamic_recon_failures: list[str] = []
        if new_contexts:
            await startup_reconcile_unconfirmed(
                new_contexts,
                failures_out=dynamic_recon_failures,
            )

        # SHP-11: drop any leader whose downstream contexts include a
        # failed user_id. Starting a monitor on stale state is the same
        # double-fill exposure the startup path already gates against —
        # the dynamic-discovery path must match.
        blocked_wallets = _wallets_blocked_by_reconcile_failures(
            started_wallets, contexts_by_leader, dynamic_recon_failures,
        )
        if blocked_wallets:
            logger.error(
                "dynamic_leader_reconcile_failed | blocked_leaders=%s "
                "failed_users=%s — monitors will NOT start for these "
                "leaders; operators must inspect and re-trigger",
                blocked_wallets,
                dynamic_recon_failures,
            )
            blocked_set = set(blocked_wallets)
            started_wallets = [
                wallet for wallet in started_wallets
                if wallet not in blocked_set
            ]

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

    # Auxiliary long-lived loops are registered as _Supervised specs and driven
    # by the main wait loop below (bounded restart, fail-closed on exhaustion)
    # instead of being fire-and-forget. Each factory must return a FRESH
    # awaitable per call so a crashed loop can be respawned.
    supervised: list[_Supervised] = []

    if application.config.discovery.enable_trader_discovery:
        logger.info(
            "discovery_dynamic_monitoring_disabled",
            extra={
                "discovery_dynamic_monitoring_enabled": False,
                "configured_value": bool(
                    application.config.discovery.discovery_dynamic_monitoring_enabled
                ),
                "effect": "background discovery will refresh DB only and will not hot-add monitors",
            },
        )
        supervised.append(
            _Supervised(
                name="discovery-refresh",
                factory=lambda: _discovery_refresh_loop(
                    registry,
                    configured,
                    refresh_interval,
                    leaders_repo=None,
                    on_new_leaders=None,
                    already_monitored=monitored_set,
                ),
            )
        )

    if application.config.risk.enable_risk_mgmt:
        from services.risk.risk_monitor import risk_monitor_loop_for_contexts

        supervised.append(
            _Supervised(
                name="risk-monitor",
                factory=lambda: risk_monitor_loop_for_contexts(
                    list(risk_contexts_by_user.values()),
                    readiness_tracker=application.readiness,
                    config=application.config,
                ),
            )
        )

    # Standalone reservation reaper for risk-disabled runs. When risk
    # management is on, its loop already reaps expired reservations in-session;
    # with it off, a TTL-expired reservation on a stuck submitted_unconfirmed
    # order would otherwise strand capital/shares until restart. Registered as a
    # supervised task (only when risk mgmt is off) so it inherits the same
    # bounded-restart / fail-closed / shutdown treatment as the other aux loops.
    # See docs/concurrency-audit-response.md (C8-adjacent gap).
    if not application.config.risk.enable_risk_mgmt:
        _reaper_interval = max(
            30.0, float(application.config.polling.reconcile_interval_seconds)
        )

        async def _reservation_reaper_loop() -> None:
            # Re-derive the context list every tick: dynamic leader discovery
            # (``_on_new_leaders``) mutates ``contexts_by_leader`` in place after
            # startup, so a one-time snapshot would silently miss any
            # later-discovered leader — reinstating the exact gap this reaper
            # closes. Sweep first, then sleep, so an already-expired reservation
            # is reaped promptly instead of waiting a full interval.
            while True:
                await _reap_reservations_once(_flatten_contexts(contexts_by_leader))
                await asyncio.sleep(_reaper_interval)

        supervised.append(
            _Supervised(name="reservation-reaper", factory=_reservation_reaper_loop)
        )

    # Auto-redemption (opt-in, default off). Conservative settlement-claim loop
    # for operators who hold to resolution. This restores a conservative
    # settlement-claim loop for operators who hold to resolution. It is gated
    # twice: the data-api must flag the position ``redeemable`` AND the local
    # market cache must show the token pinned at a terminal price
    # (REDEEM_RESOLVED_LOW / REDEEM_RESOLVED_HIGH). The Web3 redeem path runs in
    # a worker thread so it never blocks trade/risk/WSS loops.
    if application.config.redemption.enable_auto_redeem:
        from services.redemption import RedemptionService

        for runtime_user in runtime_users:
            user_state = user_states.get(runtime_user.user_id)
            if user_state is None:
                continue
            market_store = getattr(user_state, "market_store", None)
            if market_store is None:
                logger.warning(
                    "redemption_service_skipped",
                    extra={
                        "user_id": runtime_user.user_id,
                        "reason": "missing market_store",
                    },
                )
                continue
            redemption_service = RedemptionService(
                config=application.config,
                wallet_address=runtime_user.wallet.funder_address,
                market_store=market_store,
                private_key=runtime_user.wallet.private_key,
            )
            supervised.append(
                _Supervised(
                    name=f"redemption-{runtime_user.user_id}",
                    # Bind per-user service via default arg to avoid late-binding.
                    factory=lambda s=redemption_service: s.loop(),
                )
            )

    for spec in supervised:
        spec.spawn()

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

    loop = asyncio.get_running_loop()
    try:
        while True:
            # Wait on leader monitors AND every live supervised aux loop, so a
            # crash in risk / discovery / near-one / redemption is observed here
            # rather than dying silently while the process stays "up".
            active = [task for task in monitor_tasks if not task.done()]
            active += [
                spec.task for spec in supervised if spec.task and not spec.task.done()
            ]

            if not active:
                await asyncio.sleep(refresh_interval)
                metrics.inc("no_leaders_monitored_total")
                continue

            done, _ = await asyncio.wait(active, timeout=refresh_interval)

            fail_closed = False
            for task in done:
                spec = next((s for s in supervised if s.task is task), None)

                # A cancelled task is a normal lifecycle event (shutdown, or a
                # loop that re-raises CancelledError) — never a crash. Calling
                # task.exception() on it would raise CancelledError and tear
                # down the supervisor, turning graceful cancellation into an
                # exit(1). Treat it as a benign drop.
                if task.cancelled():
                    if spec is not None:
                        logger.info(
                            "supervised_task_cancelled",
                            extra={"task": spec.name},
                        )
                    continue

                exc = task.exception()

                if spec is None:
                    # Leader monitor: log and drop; dynamic discovery
                    # re-introduces monitors as leaders change. P2-h: a crashed
                    # leader monitor used to leave ``monitored_leader_count``
                    # advertising a monitor that no longer exists, so health /
                    # metrics over-reported live coverage. Re-derive the count
                    # from the still-live monitor tasks (the crashed one is
                    # ``done`` here) so the gauge reflects reality and an
                    # operator can see coverage dropped.
                    if exc:
                        logger.error(
                            "monitor_task_crashed",
                            extra={"error": redact_error_detail(exc)},
                        )
                    metrics.inc("leader_monitor_crashed_total")
                    application.readiness.update(
                        monitored_leader_count=_live_monitor_count(monitor_tasks)
                    )
                    continue

                if exc is None:
                    # Supervised loops run forever; a clean return means the
                    # subsystem disabled itself (e.g. config gate). Leave it
                    # dropped rather than tear the process down.
                    logger.warning(
                        "supervised_task_exited_clean",
                        extra={"task": spec.name},
                    )
                    continue

                metrics.inc("supervised_task_restart_total")
                decision = _on_supervised_crash(spec, loop.time())
                logger.error(
                    "supervised_task_crashed",
                    extra={
                        "task": spec.name,
                        "restarts": spec.restarts,
                        "decision": decision,
                        "error": redact_error_detail(exc),
                    },
                )
                if decision == "restart":
                    spec.spawn()
                else:
                    logger.critical(
                        "supervised_task_fail_closed",
                        extra={"task": spec.name, "restarts": spec.restarts},
                    )
                    application.readiness.update(supervisor_ok=False)
                    fail_closed = True

            if fail_closed:
                return 1

    finally:
        await _shutdown_runtime(
            monitor_tasks=monitor_tasks,
            supervised=supervised,
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

    import sys

    exit_code = 0
    try:
        # main() returns 1 on a fatal/fail-closed startup or a supervisor
        # exhaustion; orderly SIGTERM/SIGINT shutdown cancels the task and
        # exits 0.
        exit_code = loop.run_until_complete(main_task) or 0
    except (asyncio.CancelledError, KeyboardInterrupt):
        exit_code = 0
    except Exception as exc:
        import traceback

        print_error(f"Fatal startup error: {exc}")
        traceback.print_exc()
        exit_code = 1
    finally:
        loop.close()

    sys.exit(exit_code)
