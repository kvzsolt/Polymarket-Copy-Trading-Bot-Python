"""Compose, run, supervise, and close the Polymarket copy-trading runtime."""

from __future__ import annotations

import asyncio
import functools
import inspect
import signal
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Mapping, Sequence, Sized
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from importlib import metadata
from pathlib import Path

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from interfaces.source_transactions import SourceTransactionStateStore
    from orchestrators.detection_source_factory import (
        ConfirmedSourceBuilder,
        DetectionSourceRuntime,
    )
    from orchestrators.runtime_bootstrap import TradingContext

from bootstrap import build_app
from config.app_config import AppConfigSemanticValidator, EnvValidationError, load_config
from config.env import is_valid_eth_address
from domain.detection import DetectionMode
from integrations.control_plane import SystemMonotonicClock
from integrations.polymarket.venue_runtime import build_polymarket_runtime
from orchestrators.control_plane_bootstrap import (
    AiohttpControlPlaneServerFactory,
    ControlPlaneDatabasePool,
    ControlPlaneRuntime,
    ControlPlaneRuntimeFactory,
)
from orchestrators.control_plane_reads import (
    ControlPlaneRuntimeReads,
    FollowerScopedPositions,
    PendingConfirmationStatsProbe,
)
from orchestrators.control_plane_runtime import (
    InactiveWorkerStateReader,
    WorkerStateReader,
)
from orchestrators.control_plane_discovery import (
    DiscoveryRunQueue,
    LeaderRegistryDiscoveryRunner,
)
from orchestrators.control_plane_portfolio import (
    PortfolioAccountBinding,
    PortfolioRuntimeFactory,
)
from orchestrators.control_plane_tasks import (
    LeaderMonitoringState,
    RuntimeTaskActivationGuard,
)
from orchestrators.detection_startup_reporter import DetectionStartupReporter
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
from orchestrators.telemetry_bootstrap import TelemetryRuntime
from orchestrators.user_data_runtime import UserDataRuntime
from services.healthServer import ReadinessTracker, start_health_server
from services.metrics import MetricsRegistry, metrics
from services.control_plane.runtime import OpenOrderControlGateway
from services.control_plane.commands import DiscoveryCommandPort
from services.control_plane.contracts_reads import AccountView, PendingConfirmationStatsView
from services.control_plane.queries import CachedPortfolioQueryPort
from services.control_plane.tasks import TaskActivationGuard
from services.telemetry import SystemClock, TelemetryEmitters, WssTelemetryEmitter
from services.multiUserConfig import (
    RuntimeUser,
    UserFollowConfig,
    load_runtime_users,
)
from storage.postgres.leaders_repo import LeadersRepo
from utils.getMyBalance import get_my_balance
from utils.logger import (
    configure_logging,
    get_logger,
    print_error,
    redact_error_detail,
)

logger = get_logger("Main")
_CONFIG_ROOT = Path(__file__).resolve().parent / "config"


async def _wait_for_active_leaders(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    *,
    config,
    initial_state=None,
) -> list[str]:
    return await _wait_for_active_leaders_impl(
        registry,
        configured_leaders,
        refresh_interval,
        config=config,
        initial_state=initial_state,
    )


async def _discovery_refresh_loop(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    on_new_leaders: Callable[[list[str]], Awaitable[Iterable[str] | None]] | None = None,
    already_monitored: set[str] | None = None,
) -> None:
    await _discovery_refresh_loop_impl(
        registry,
        configured_leaders,
        refresh_interval,
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
    return [context for contexts in contexts_by_leader.values() for context in contexts]


def _refresh_onchain_watched_wallets(
    onchain_runtime: object | None,
    wallets: Iterable[str],
) -> bool:
    """Point the on-chain runtime at the current monitored-leader set.

    The runtime is built once at startup, so leaders promoted later by
    discovery must be pushed into its mined/pending wallet filters or they
    never receive fast-path signals. Returns False (never raises) when
    on-chain detection is absent or disabled: it is an accelerator, and the
    REST-polling path already covers the hot-added leader.
    """
    if onchain_runtime is None:
        return False
    update = getattr(onchain_runtime, "update_watched_wallets", None)
    if not callable(update):
        return False
    watched = {str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()}
    try:
        update(watched)
    except Exception as exc:
        logger.warning(
            "onchain_watched_wallets_update_failed | error=%s — hot-added "
            "leaders keep the REST-polling path only",
            redact_error_detail(exc),
        )
        return False
    logger.info("onchain_watched_wallets_updated | watched=%d", len(watched))
    return True


def _should_wait_for_discovery_roster(config: object) -> bool:
    """Whether waiting for a discovery roster can actually yield leaders.

    Mirrors ``LeaderRegistry.refresh``, which only reads the roster when
    ``start_from_roster`` is set. Without that flag the wait can never
    succeed, so startup fails fast on the empty-roster path instead of
    blocking for ``startup_wait_timeout_seconds``.
    """
    discovery = getattr(config, "discovery", None)
    scheduler = getattr(discovery, "scheduler", None)
    api = getattr(discovery, "api", None)
    return bool(
        getattr(scheduler, "enabled", False)
        and getattr(scheduler, "start_from_roster", False)
        and getattr(api, "has_token", False)
    )


def _detection_confidence_from_config(value: object):
    """Parse ``detection.onchain.pending_tx_detection_confidence_required``.

    Fails closed. Silently substituting ``PENDING_PROBABLE`` turned a typo (or
    a future enum rename) into a detection policy the operator never chose:
    for ``detection.mode=on_chain_pending`` that floor decides whether every
    mempool sighting is copied or dropped. ``load_config`` already rejects an
    unknown value, so reaching this branch means the config was constructed
    outside the loader or the two value sets have drifted apart.

    Args:
        value: Raw configured value; compared case-insensitively after strip.

    Returns:
        The matching ``DetectionConfidence`` member.

    Raises:
        EnvValidationError: If *value* is not a known confidence level. The
            raw value is deliberately not echoed (a mis-mapped override could
            carry a credential), matching the config layer's own message.
    """

    from services.onchain_detection.trade_detection_models import DetectionConfidence

    try:
        return DetectionConfidence(str(value or "").strip().lower())
    except ValueError as exc:
        allowed = ", ".join(level.value for level in DetectionConfidence)
        raise EnvValidationError(
            f"detection.onchain.pending_tx_detection_confidence_required must be one of: {allowed}."
        ) from exc


def _validate_detection_confidence_floor(config: object) -> None:
    """Fail closed on the pending-path confidence floor before startup proceeds.

    In ``on_chain_pending`` this floor decides whether every mempool sighting
    is copied or dropped. Validating it here — right after the config loads,
    before any runtime, socket, or pool exists — means an unparseable value
    aborts the process instead of being absorbed by the on-chain composition
    guard, which would silently downgrade the operator's chosen mode to REST
    polling. Other modes are unaffected.

    Every read is defensive because ``app`` is also driven by partial config
    doubles in the startup tests; a loader-produced ``AppConfig`` always
    carries both sections.

    Args:
        config: Freshly loaded application configuration.

    Raises:
        EnvValidationError: If a non-polling mode carries an unparseable
            confidence floor (a missing ``onchain`` section included).
    """

    detection = getattr(config, "detection", None)
    if detection is None:
        return
    if getattr(detection, "detection_mode", DetectionMode.POLLING) is DetectionMode.POLLING:
        return
    onchain = getattr(detection, "onchain", None)
    _detection_confidence_from_config(
        getattr(onchain, "pending_tx_detection_confidence_required", None)
    )


def _build_pending_tx_config(oc_cfg: object):
    from services.onchain_detection.pending_tx_detector import PendingTxConfig

    return PendingTxConfig(
        max_signal_age_ms=int(getattr(oc_cfg, "pending_tx_max_signal_age_ms", 500)),
        hashes_only=bool(getattr(oc_cfg, "pending_tx_hashes_only", False)),
        confidence_required=_detection_confidence_from_config(
            getattr(
                oc_cfg,
                "pending_tx_detection_confidence_required",
                "pending_probable",
            )
        ),
    )


def _build_onchain_condition_id_resolver():
    from services.onchain_detection.condition_id_resolver import GammaConditionIdResolver

    return GammaConditionIdResolver().resolve


def _build_onchain_ws_health_reporter(
    readiness,
    metrics_registry: MetricsRegistry = metrics,
    *,
    mode: DetectionMode,
):
    """Thin glue: the on-chain WebSocket health reporter, when it applies.

    Only the gen-1 provisional runtime opens ``OnchainWebSocket``
    subscriptions. Registering ``onchain_ws_ok`` in any other mode would
    publish a check that nothing can ever mark ready — a permanent false "WS
    down" for a socket the process never opens.

    Args:
        readiness: Readiness tracker the ``onchain_ws_ok`` check registers on.
        metrics_registry: Registry receiving the bounded on-chain WS metrics.
        mode: Selected detection mode.

    Returns:
        ``OnchainWsHealthReporter`` for ``ON_CHAIN_PENDING``; ``None``
        otherwise.
    """

    from services.onchain_detection.ws_health_reporter import OnchainWsHealthReporter

    if mode is not DetectionMode.ON_CHAIN_PENDING:
        return None
    return OnchainWsHealthReporter(metrics_registry, readiness)


def _build_onchain_websocket_factory(state_listener):
    """Thin glue: the subscription constructor, health-reporting when asked.

    Args:
        state_listener: Connection-state observer, or ``None``.

    Returns:
        ``OnchainWebSocket`` itself when no listener is supplied, else a
        partial with the listener bound.
    """

    from clients.onchain_ws import OnchainWebSocket

    if state_listener is None:
        return OnchainWebSocket
    return functools.partial(OnchainWebSocket, state_listener=state_listener)


def _build_pending_copy_tracker(
    config: object,
    *,
    telemetry_service: object | None,
    metrics_registry: MetricsRegistry = metrics,
):
    """Thin glue: the pending-copy confirmation tracker, when it applies.

    Only ``detection.mode=on_chain_pending`` copies off an unmined sighting,
    so only that mode needs the follow-up. Returns ``None`` (feature off) for
    every other mode, when the operator disabled it, or when no HTTP RPC
    endpoint is configured — receipt confirmation cannot run without one, and
    a tracker that can never confirm would report every copy as ``rpc_error``.

    Args:
        config: Typed application configuration.
        telemetry_service: Process telemetry service, or ``None``.
        metrics_registry: Registry receiving the ``pending_copy_*`` metrics.

    Returns:
        A not-yet-started ``PendingCopyConfirmationTracker``, or ``None``.
    """

    from clients.network.web3_receipt_fetcher import Web3ReceiptStatusFetcher
    from services.onchain_detection.pending_copy_confirmation_tracker import (
        PendingCopyConfirmationTracker,
    )

    detection = getattr(config, "detection", None)
    if detection is None or detection.detection_mode is not DetectionMode.ON_CHAIN_PENDING:
        return None
    oc_cfg = getattr(detection, "onchain", None)
    if oc_cfg is None or not bool(getattr(oc_cfg, "pending_copy_confirmation_enabled", False)):
        return None
    polymarket = getattr(config, "polymarket", None)
    rpc_url = str(getattr(polymarket, "rpc_url", "") or "")
    if not rpc_url.strip():
        # The endpoint is a secret; report only that it is absent.
        logger.warning(
            "pending_copy_tracker_disabled | reason=no_rpc_endpoint — provisional "
            "copies will not be confirmed against the leader transaction receipt"
        )
        return None
    return PendingCopyConfirmationTracker(
        receipt_fetcher=Web3ReceiptStatusFetcher.from_rpc_url(
            rpc_url,
            str(getattr(polymarket, "fallback_rpc_url", "") or ""),
        ),
        metrics=metrics_registry,
        telemetry=telemetry_service,
        deadline_seconds=float(getattr(oc_cfg, "pending_copy_confirmation_deadline_seconds", 60.0)),
        max_tracked=int(getattr(oc_cfg, "pending_copy_confirmation_max_tracked", 2048)),
        # Auto-unwind stays design-only: the flag is accepted but no requester
        # is wired, so enabling it cannot move money (see
        # docs/known_limitations.md).
        unwind_hook=None,
    )


def _confirmed_source_builder(
    state_store_provider: "Callable[[], Awaitable[SourceTransactionStateStore | None]]",
) -> "ConfirmedSourceBuilder":
    """Thin glue: the receipt-confirmed builder over both CTF exchanges.

    Args:
        state_store_provider: Awaitable factory for the durable source
            reservation/idempotency store (Postgres in production, ``None``
            for an in-memory registry in tests).

    Returns:
        ``ConfirmedSourceBuilder`` filtering on the CTF and neg-risk exchange
        contracts.
    """

    from config.addresses import (
        POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
        POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    )
    from orchestrators.detection_source_factory import ConfirmedSourceBuilder

    return ConfirmedSourceBuilder(
        exchange_addresses=(
            POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
            POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
        ),
        state_store_provider=state_store_provider,
    )


async def _start_onchain_runtime(runtime: "DetectionSourceRuntime", *, mode: DetectionMode) -> None:
    """Thin glue: start ``runtime``; on failure stop it before re-raising.

    A partially started runtime (WebSocket connected, dispatcher task spawned)
    would otherwise leak because ``app.main`` drops its reference and the
    shutdown path never sees it. ``stop()`` errors are logged and suppressed.

    Args:
        runtime: Not-yet-started detection source runtime.
        mode: Selected detection mode (log context only).

    Raises:
        Exception: Whatever ``runtime.start()`` raised, after ``stop()``.
    """

    try:
        await runtime.start()
    except BaseException:
        try:
            await runtime.stop()
        except Exception as stop_exc:
            logger.warning(
                "onchain_detection_stop_after_failed_start_failed | mode=%s error=%s",
                mode.value,
                redact_error_detail(stop_exc),
            )
        raise


async def _default_source_state_store_provider():
    """Return the Postgres-backed source state store on the shared pool."""

    from adapters.persistence.source_transaction_state_store import (
        PostgresSourceTransactionStateStore,
    )
    from storage.postgres.connection import get_pool

    return PostgresSourceTransactionStateStore(await get_pool())


def _build_detection_source_factory(
    config: object,
    *,
    state_store_provider=None,
    websocket_factory=None,
):
    """Thin glue: one ``DetectionSourceFactory`` covering both on-chain modes.

    Args:
        config: Typed application configuration (``detection`` section read).
        state_store_provider: Awaitable factory for the confirmed runtime's
            durable state store; defaults to the shared Postgres pool.
        websocket_factory: ``OnchainWebSocket``-compatible constructor for the
            provisional runtime's subscription; ``app.main`` binds the health
            reporter here. Defaults to the plain client.

    Returns:
        ``DetectionSourceFactory`` with the provisional-pending and
        receipt-confirmed builders registered.
    """

    from clients.onchain_ws import OnchainWebSocket
    from orchestrators.detection_source_factory import (
        DetectionSourceFactory,
        ProvisionalPendingSourceBuilder,
    )

    oc_cfg = getattr(getattr(config, "detection", None), "onchain", None)
    return DetectionSourceFactory(
        [
            ProvisionalPendingSourceBuilder(
                pending_config=_build_pending_tx_config(oc_cfg),
                condition_id_resolver=_build_onchain_condition_id_resolver(),
                websocket_factory=websocket_factory or OnchainWebSocket,
            ),
            _confirmed_source_builder(state_store_provider or _default_source_state_store_provider),
        ]
    )


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
                getattr(ctx, "user_id", "?"),
                exc,
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
        wallet
        for wallet in started_wallets
        if any(ctx.user_id in failed for ctx in contexts_by_leader.get(wallet, []))
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
            )
            contexts_by_leader[wallet].append(context)

        if contexts_by_leader[wallet]:
            started_wallets.append(wallet)

    return started_wallets


def _get_clob_wrapper_from_user_state(user_state: UserRuntimeState) -> Any | None:
    """Best-effort access to the CCXT venue client that owns L2 credentials.

    Preferred source is ``user_state.client`` (the ``CcxtPredictionClient``),
    which exposes derived ``api_creds`` for user-stream auth. Fallbacks keep
    older test fixtures working.
    """
    for attr_name in (
        "client",
        "clob_client",
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


def _empty_user_ws_api_creds() -> dict[str, str]:
    """Return the unauthenticated user-stream credential shape."""
    return {field: "" for field in ("api_key", "api_secret", "api_passphrase")}


def _normalise_user_ws_api_creds(raw_creds: Any) -> dict[str, str]:
    """Normalize SDK API credentials into UserWebSocket's expected shape."""

    if raw_creds is None:
        return _empty_user_ws_api_creds()

    def read(*names: str) -> str:
        """Return the first populated credential attribute without logging it."""
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
    key to create the secure SDK client, which exposes the CLOB API credentials
    used for user websocket authentication.
    """
    wrapper = _get_clob_wrapper_from_user_state(user_state)

    if wrapper is None:
        return _empty_user_ws_api_creds()

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

    return _empty_user_ws_api_creds()


async def _start_market_data_runtimes(
    *,
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    live_positions_by_user: dict[str, list[dict]],
    contexts_by_leader: dict[str, list["TradingContext"]],
    readiness,
    config,
    quote_sink=None,
    telemetry: WssTelemetryEmitter | None = None,
) -> dict[str, MarketDataRuntime]:
    """Start WSS-backed market data runtimes, keyed by user_id.

    Trade detection remains HTTP polling. These runtimes only keep market
    quote/book/tick state fresh for execution and risk exits. The
    caller must attach the matching runtime to each user's TradingContext so
    the trade pipeline can subscribe new signal tokens dynamically.
    """
    market_data_runtimes: dict[str, MarketDataRuntime] = {}
    context_tokens = extract_token_ids_from_contexts(_flatten_contexts(contexts_by_leader))

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
            quote_sink=quote_sink,
            telemetry=telemetry,
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
                    release_result = release_expired()
                    released_ids = (
                        await release_result
                        if inspect.isawaitable(release_result)
                        else release_result
                    )
                    reservation_reaped = len(released_ids) if isinstance(released_ids, Sized) else 0
                if reconciled or reservation_reaped:
                    logger.info(
                        "user_wss_wake_reconcile_complete | user=%s reconciled=%d reservations_reaped=%d order_id=%s",
                        context.user_id,
                        reconciled,
                        reservation_reaped,
                        (event.get("order_id") or event.get("orderID") or event.get("id"))
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
    telemetry: WssTelemetryEmitter | None = None,
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
                market_token_subscribe = _build_market_token_subscribe_closure(market_runtime)

        runtime = UserDataRuntime(
            market_store=market_store,
            readiness=readiness,
            api_creds=api_creds,
            user_id=runtime_user.user_id,
            on_reconcile_signal=wake_closure,
            manual_sync_service=getattr(user_state, "manual_sync_service", None),
            on_risk_cache_evict=risk_cache_evict,
            on_market_token_seen=market_token_subscribe,
            telemetry=telemetry,
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
    token_ids = extract_token_ids_from_contexts(_flatten_contexts(contexts_by_leader))

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
class _HealthServerResource:
    """Idempotent owner for an aiohttp health-server runner."""

    runner: Any
    closed: bool = False

    async def close(self) -> None:
        """Close the health server once."""

        if self.closed:
            return
        self.closed = True
        await self.runner.cleanup()

    async def cleanup(self) -> None:
        """Provide the aiohttp-compatible cleanup alias."""

        await self.close()


async def _start_telemetry_runtime(
    *,
    application: object,
    client_registry: list[Any],
    metrics_registry: MetricsRegistry = metrics,
) -> TelemetryRuntime:
    """Start and register the process-wide bounded telemetry owner.

    Args:
        application: Composed application exposing the readiness tracker.
        client_registry: Outer lifecycle registry closed from ``main``.
        metrics_registry: Existing process-wide metrics truth source.

    Returns:
        The started telemetry runtime used by later control-plane composition.
    """

    readiness = getattr(application, "readiness", None)
    if readiness is None:
        raise TypeError("application readiness tracker is required")
    runtime = TelemetryRuntime(
        readiness=readiness,
        metrics=metrics_registry,
    )
    await runtime.start()
    client_registry.append(runtime)
    return runtime


def _build_telemetry_emitters(telemetry_runtime: TelemetryRuntime | None) -> TelemetryEmitters:
    """Return the process-wide operator telemetry emitter bundle.

    Args:
        telemetry_runtime: Started telemetry owner, or ``None`` when telemetry
            was not composed for this process.

    Returns:
        A bundle bound to the running telemetry service, or the disabled
        null-object bundle so no collaborator has to branch on availability.
    """

    if telemetry_runtime is None:
        return TelemetryEmitters.disabled()
    return TelemetryEmitters.build(telemetry_runtime.service)


def _detection_connection_probe(runtime: object) -> Callable[[], bool | None]:
    """Return an O(1) transport-health probe for one detection runtime.

    The control plane starts before the detection source exists, so the
    telemetry emitter is given this probe only after the runtime started. A
    runtime that exposes no connection state reports ``None`` (unknown) rather
    than a fabricated ``False``.

    Args:
        runtime: Started detection source runtime.

    Returns:
        A callable returning the runtime's connection state, or ``None``.
    """

    def probe() -> bool | None:
        state = getattr(runtime, "connected", None)
        return state if isinstance(state, bool) else None

    return probe


def _build_control_plane_reads(
    *,
    readiness: ReadinessTracker,
    metrics_registry: MetricsRegistry,
    contexts_by_leader: Mapping[str, Sequence[Any]],
    risk_contexts_by_user: Mapping[str, Any],
    user_states: Mapping[str, Any],
    configured_leaders: tuple[str, ...],
    monitored_leaders: Callable[[], Iterable[str]],
    detection_counters: Any,
    pending_stats: Callable[[], PendingConfirmationStatsView | None],
    portfolio_cache: CachedPortfolioQueryPort | None,
) -> ControlPlaneRuntimeReads:
    """Bundle the local runtime state the extended operator reads may observe.

    Only state this process actually composed is published: a follower without
    a market store contributes no quote source, and a leader with no bound
    context is not counted for any account.

    Args:
        readiness: Canonical process readiness truth.
        metrics_registry: Process metrics registry backing WSS reconnects.
        contexts_by_leader: Bound trading contexts per leader wallet.
        risk_contexts_by_user: One canonical trading context per follower.
        user_states: Composed follower runtimes exposing market stores.
        configured_leaders: Operator-configured leader wallets.
        monitored_leaders: Late-bound reader of monitored leader wallets.
        detection_counters: Detection telemetry emitter counter source.
        pending_stats: Late-bound provisional-copy tracker probe.
        portfolio_cache: Optional local-only follower portfolio cache.

    Returns:
        One immutable read bundle for the control-plane factory.
    """

    pnl_trackers: dict[str, Any] = {}
    positions: dict[str, Any] = {}
    market_stores: dict[str, Any] = {}
    for user_id, context in risk_contexts_by_user.items():
        tracker = getattr(context, "pnl_tracker", None)
        if tracker is not None:
            pnl_trackers[user_id] = tracker
        positions_repo = getattr(context, "positions_repo", None)
        if positions_repo is not None:
            positions[user_id] = positions_repo
        market_store = getattr(user_states.get(user_id), "market_store", None)
        if market_store is not None:
            market_stores[user_id] = market_store

    def _accounts() -> tuple[AccountView, ...]:
        """Project the live follower/leader binding on every request.

        ``contexts_by_leader`` changes while the process runs (discovery
        promotions, roster refreshes), so a snapshot taken at composition
        would report a stale ``leader_count`` for the life of the process.
        """

        leader_counts: dict[str, int] = {}
        for contexts in contexts_by_leader.values():
            for context in contexts:
                bound_user = str(getattr(context, "user_id", "") or "")
                if bound_user:
                    leader_counts[bound_user] = leader_counts.get(bound_user, 0) + 1
        views: list[AccountView] = []
        for account_id, account_context in risk_contexts_by_user.items():
            wallet = str(getattr(account_context, "wallet_address", "") or "")
            if wallet:
                views.append(
                    AccountView(
                        follower_account=account_id,
                        wallet_address=wallet,
                        leader_count=leader_counts.get(account_id, 0),
                    )
                )
        return tuple(views)

    return ControlPlaneRuntimeReads(
        accounts=_accounts,
        detection_counters=detection_counters,
        pending_stats=pending_stats,
        pnl_trackers=pnl_trackers,
        positions=FollowerScopedPositions(positions),
        market_stores=market_stores,
        monitored_leaders=monitored_leaders,
        configured_leaders=tuple(configured_leaders),
        portfolio=portfolio_cache,
        readiness=readiness,
        metrics=metrics_registry,
    )


async def _start_control_plane_runtime(
    *,
    application: object,
    pool: ControlPlaneDatabasePool,
    telemetry: TelemetryRuntime,
    worker_state: WorkerStateReader,
    runtime_gateways: tuple[OpenOrderControlGateway, ...],
    task_activation_guard: TaskActivationGuard | None,
    discovery_command_port: DiscoveryCommandPort | None,
    client_registry: list[Any],
    factory: ControlPlaneRuntimeFactory,
    portfolio_cache: CachedPortfolioQueryPort | None = None,
    reads: ControlPlaneRuntimeReads | None = None,
) -> ControlPlaneRuntime | None:
    """Build, register, and start the optional privileged adapter safely.

    Args:
        application: Composed application exposing config and readiness.
        pool: Existing shared PostgreSQL pool.
        telemetry: Existing process-wide telemetry runtime.
        worker_state: Local bounded copy-worker statistics source.
        runtime_gateways: One canonical live gateway per follower account.
        task_activation_guard: Optional fail-closed task resume policy.
        discovery_command_port: Optional started manual-discovery queue.
        client_registry: Outer lifecycle registry closed from ``main``.
        factory: Injected application-service and transport composition root.
        portfolio_cache: Optional local-only follower portfolio cache.
        reads: Optional local runtime state backing the extended reads.

    Returns:
        The owned runtime when enabled, otherwise ``None``.

    Notes:
        Transport startup failure is isolated from trading. The runtime stays
        registered so outer shutdown can close any partially started adapter,
        while its optional readiness check remains false.
    """

    config = getattr(application, "config", None)
    readiness = getattr(application, "readiness", None)
    if config is None or readiness is None:
        raise TypeError("application config and readiness are required")
    runtime = factory.build(
        config=config,
        pool=pool,
        readiness=readiness,
        telemetry_query=telemetry.query,
        telemetry_hub=telemetry.hub,
        telemetry_service=telemetry.service,
        worker_state=worker_state,
        runtime_gateways=runtime_gateways,
        task_activation_guard=task_activation_guard,
        discovery_command_port=discovery_command_port,
        portfolio_cache=portfolio_cache,
        reads=reads,
    )
    if runtime is None:
        return None
    client_registry.append(runtime)
    try:
        await runtime.start()
    except Exception as exc:
        logger.error(
            "control_plane_start_failed",
            extra={"error": redact_error_detail(exc)},
        )
    return runtime


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
    factory: Callable[[], Coroutine[object, object, None]]
    max_restarts: int = 5
    recovery_seconds: float = 60.0
    task: asyncio.Task | None = None
    restarts: int = 0
    started: float = 0.0

    def spawn(self) -> asyncio.Task:
        """Start and track a supervised background task."""
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
    onchain_runtime=None,
    pending_copy_tracker=None,
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

    if onchain_runtime is not None:
        try:
            await onchain_runtime.stop()
        except Exception as exc:
            logger.warning(
                "onchain_runtime_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )

    if pending_copy_tracker is not None:
        try:
            await pending_copy_tracker.stop()
        except Exception as exc:
            logger.warning(
                "pending_copy_tracker_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )

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


async def _run_application(client_registry: list[Any]) -> int:
    configure_logging(level="INFO")
    monotonic_clock = SystemMonotonicClock()
    process_started_monotonic = monotonic_clock.monotonic()

    try:
        startup_config = load_config()
        _validate_detection_confidence_floor(startup_config)
    except EnvValidationError as exc:
        print_error(str(exc))
        return 1

    configure_logging(
        level=startup_config.logging.level,
        log_dir=startup_config.logging.log_dir,
        jsonl_file=startup_config.logging.app_log_jsonl_file,
        max_bytes=startup_config.logging.log_max_bytes,
        backup_count=startup_config.logging.log_backup_count,
    )

    from bot_cli.commands.doctor import run_doctor_checks

    mode = "paper" if startup_config.execution.paper_trade_only else "live"

    if mode == "live":
        doctor_result = await run_doctor_checks("live", config=startup_config)
        if doctor_result.get("status") == "blocked":
            print_error("Live mode doctor check failed. Refusing to start.")
            blocked = [
                c
                for c in doctor_result.get("checks", [])
                if isinstance(c, dict) and c.get("status") == "blocked"
            ]
            for check in blocked:
                print_error(f"  ❌ {check.get('name')}: {check.get('detail', '')}")
                fix = check.get("fix")
                if fix:
                    print_error(f"     fix: {fix}")
            import json

            print(json.dumps(doctor_result, indent=2))
            return 1
        if doctor_result.get("status") == "warning":
            warnings = [
                c
                for c in doctor_result.get("checks", [])
                if isinstance(c, dict) and c.get("status") == "warning"
            ]
            for check in warnings:
                logger.warning(
                    "doctor_warning | name=%s detail=%s fix=%s",
                    check.get("name"),
                    check.get("detail"),
                    check.get("fix"),
                )
    application = await build_app(config=startup_config)
    telemetry_runtime = await _start_telemetry_runtime(
        application=application,
        client_registry=client_registry,
    )
    telemetry_emitters = _build_telemetry_emitters(telemetry_runtime)
    if application.falcon_client is not None:
        client_registry.append(application.falcon_client)

    application.readiness.update(
        wss_market_ok=False,
        wss_market_connecting=False,
        wss_user_ok=False,
        wss_user_connecting=False,
        market_data_ready=False,
    )

    # Start observability before any discovery-only promotion wait. A fresh
    # roster can require multiple longitudinal cycles; operators must still be
    # able to inspect a fail-closed readiness payload during that warm-up.
    _slowest_loop_interval = max(
        float(application.config.detection.fetch_interval),
        float(application.config.risk.risk_check_interval),
    )
    application.readiness.configure_heartbeat_budget(
        staleness_seconds=_slowest_loop_interval * 3.0,
    )
    metrics_runner: _HealthServerResource | None = None
    if application.config.app.enable_metrics_server:
        metrics_runner = _HealthServerResource(
            await start_health_server(
                application.config.app.metrics_port,
                tracker=application.readiness,
                host=application.config.app.metrics_bind_host,
            )
        )
        client_registry.append(metrics_runner)

    configured = configured_leaders(application.config)
    invalid = [wallet for wallet in configured if not is_valid_eth_address(wallet)]

    if invalid:
        for wallet in invalid:
            print_error(
                f'Invalid leader wallet "{wallet}" — must be 42-char hex (0x + 40 hex digits).'
            )
        return 1

    registry_state = await application.registry.refresh(configured)
    active_leaders = registry_state.active_monitored

    invalid_active = [wallet for wallet in active_leaders if not is_valid_eth_address(wallet)]
    if invalid_active:
        logger.error(
            "startup_blocked_no_active_leaders",
            extra={
                "mode": registry_state.mode,
                "reason": "invalid_active_leaders",
                "invalid_wallets": invalid_active,
                "discovery_enabled": application.config.discovery.scheduler.enabled,
                "max_active_leaders": (application.config.discovery.scheduler.max_active_leaders),
            },
        )
        print_error("No active leaders available. Auto-selected leaders were invalid.")
        return 1

    if not active_leaders:
        if _should_wait_for_discovery_roster(application.config):
            logger.info(
                "startup_waiting_for_discovery_roster",
                extra={
                    "rediscover_seconds": application.config.discovery.scheduler.rediscover_seconds,
                    "required_cycles": application.config.discovery.hysteresis.min_tenure_cycles,
                },
            )
            active_leaders = await _wait_for_active_leaders(
                application.registry,
                configured,
                application.config.discovery.scheduler.rediscover_seconds,
                config=application.config,
                initial_state=registry_state,
            )
        else:
            # Fall through to the fail-closed branch below rather than waiting
            # startup_wait_timeout_seconds for a roster that cannot arrive.
            logger.info(
                "startup_roster_wait_skipped",
                extra={
                    "reason": "roster cannot supply startup wallets",
                    "discovery_enabled": (application.config.discovery.scheduler.enabled),
                    "start_from_roster": (application.config.discovery.scheduler.start_from_roster),
                    "discovery_has_token": application.config.discovery.api.has_token,
                },
            )

    if not active_leaders:
        logger.error(
            "startup_blocked_no_active_leaders",
            extra={
                "mode": registry_state.mode,
                "reason": "manual_and_roster_empty",
                "discovery_enabled": application.config.discovery.scheduler.enabled,
                "max_active_leaders": (application.config.discovery.scheduler.max_active_leaders),
            },
        )
        print_error(
            "No active leaders available. leader.wallets is empty and "
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

    _max_age = float(application.config.app.retention_max_age_seconds)
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

    research_capture_service = None
    leaders_repo = application.leaders_repo
    registry = application.registry
    refresh_interval = application.config.discovery.scheduler.rediscover_seconds

    ensure_runtime_follows(runtime_users, active_leaders, application.config)

    user_states: dict[str, UserRuntimeState] = {}

    for runtime_user in runtime_users:
        venue_runtime = await build_polymarket_runtime(
            runtime_user=runtime_user,
            config=application.config,
        )
        # The CCXT runtime is the single venue owner; close it on shutdown.
        client_registry.append(venue_runtime)
        user_states[runtime_user.user_id] = build_user_state(
            runtime_user,
            venue_runtime.client,
            _pool,
            application.config,
            clob_gateway=venue_runtime.gateway,
            telemetry=telemetry_emitters,
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
        *(get_my_balance(runtime_user.wallet.funder_address) for runtime_user in runtime_users)
    )

    # INF-01 (CLAUDE.md no-float-money): keep balances as ``Decimal`` for
    # the entire startup health / cap / threshold pass; downstream
    # consumers cast to float only at display boundaries.
    user_balances: dict[str, Decimal] = {
        runtime_user.user_id: _to_decimal_balance(balance)
        for runtime_user, balance in zip(runtime_users, user_balances_raw, strict=True)
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

    monitored_leaders = [leader for leader, contexts in contexts_by_leader.items() if contexts]
    application.readiness.update(monitored_leader_count=len(monitored_leaders))

    if not monitored_leaders:
        logger.warning(
            "no_leaders_monitored",
            extra={"reason": "no_user_follows_active_leaders"},
        )
        metrics.inc("no_leaders_monitored_total")

    # Prepare the task lifecycle before startup reconciliation so orders-table
    # evidence can settle retained task budgets. Workers remain stopped until
    # account state and drift have converged and no detection source exists yet.
    from orchestrators.v1_runtime_bootstrap import (
        RuntimeFollowerBalanceReader,
        start_v1_copy_task_runtime,
    )

    logger.info("startup_stage | stage=v1_copy_task_restoration_started")
    initial_v1_contexts = tuple(
        context for leader_contexts in contexts_by_leader.values() for context in leader_contexts
    )
    v1_runtime = await start_v1_copy_task_runtime(
        pool=_pool,
        config=application.config,
        copy_tasks_config=application.config.copy_trading.copy_tasks,
        contexts=initial_v1_contexts,
        start_workers=False,
    )
    if v1_runtime is not None:
        client_registry.append(v1_runtime)
    logger.info(
        "startup_stage | stage=v1_copy_task_restoration_completed enabled=%s",
        v1_runtime is not None,
    )

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
    from orchestration.trading_permission_runtime import TradingPermissionRuntime

    trading_permission_runtime = TradingPermissionRuntime(
        permissions={
            user_id: state.trading_permission
            for user_id, state in user_states.items()
            if state.trading_permission is not None
        }
    )
    control_failures = await trading_permission_runtime.initialize(
        blocked_user_ids=set(reconcile_failures)
    )
    reconcile_failures.extend(
        user_id for user_id in control_failures if user_id not in reconcile_failures
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
            application.readiness.update(monitored_leader_count=len(monitored_leaders))
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
                runtime_user.user_id,
                exc,
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

    if v1_runtime is not None:
        await v1_runtime.start()

    task_monitoring_state = LeaderMonitoringState(())
    task_activation_guard = (
        RuntimeTaskActivationGuard(
            bindings=v1_runtime,
            monitoring=task_monitoring_state,
        )
        if v1_runtime is not None
        else None
    )
    discovery_command_queue = None
    portfolio_runtime = None
    if application.config.control_plane.enabled:
        discovery_command_queue = DiscoveryRunQueue(
            runner=LeaderRegistryDiscoveryRunner(
                registry=registry,
                configured_leaders=tuple(configured),
            ),
            telemetry=telemetry_runtime.service,
        )
        await discovery_command_queue.start()
        client_registry.append(discovery_command_queue)

        portfolio_bindings = []
        for context in risk_contexts_by_user.values():
            reservations = context.extra.get("reservations_repo")
            if reservations is None:
                continue
            portfolio_bindings.append(
                PortfolioAccountBinding(
                    follower_account=context.user_id,
                    wallet=context.wallet_address,
                    positions=context.positions_repo,
                    reservations=reservations,
                )
            )
        portfolio_runtime = PortfolioRuntimeFactory(
            clock=SystemClock(),
            balance=RuntimeFollowerBalanceReader(get_my_balance),
        ).build(
            bindings=tuple(portfolio_bindings),
            refresh_interval_seconds=(
                application.config.control_plane.portfolio_refresh_interval_seconds
            ),
            stale_after_seconds=(application.config.control_plane.portfolio_stale_after_seconds),
        )
        if portfolio_runtime is not None:
            await portfolio_runtime.refresh.start()
            client_registry.append(portfolio_runtime.refresh)

    try:
        backend_version = metadata.version("polymarket-copy-trading-bot")
    except metadata.PackageNotFoundError:
        backend_version = "0.1.0"
    pending_confirmation_stats = PendingConfirmationStatsProbe()
    control_plane_reads = _build_control_plane_reads(
        readiness=application.readiness,
        metrics_registry=metrics,
        contexts_by_leader=contexts_by_leader,
        risk_contexts_by_user=risk_contexts_by_user,
        user_states=user_states,
        configured_leaders=tuple(configured),
        monitored_leaders=lambda: tuple(monitored_leaders),
        detection_counters=telemetry_emitters.detection,
        pending_stats=pending_confirmation_stats,
        portfolio_cache=(portfolio_runtime.cache if portfolio_runtime is not None else None),
    )
    await _start_control_plane_runtime(
        application=application,
        pool=_pool,
        telemetry=telemetry_runtime,
        worker_state=v1_runtime if v1_runtime is not None else InactiveWorkerStateReader(),
        runtime_gateways=tuple(
            state.clob_gateway for state in user_states.values() if state.clob_gateway is not None
        ),
        task_activation_guard=task_activation_guard,
        discovery_command_port=discovery_command_queue,
        portfolio_cache=(portfolio_runtime.cache if portfolio_runtime is not None else None),
        reads=control_plane_reads,
        client_registry=client_registry,
        factory=ControlPlaneRuntimeFactory(
            server_factory=AiohttpControlPlaneServerFactory(),
            config_root=_CONFIG_ROOT,
            version=backend_version,
            clock=SystemClock(),
            monotonic_clock=monotonic_clock,
            semantic_validator=AppConfigSemanticValidator(),
            started_monotonic=process_started_monotonic,
        ),
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
        quote_sink=v1_runtime.quote_sink if v1_runtime is not None else None,
        telemetry=telemetry_emitters.wss,
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
        telemetry=telemetry_emitters.wss,
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
        report.position_sync.pruned_positions for report in all_health_reports
    )
    startup_purged_failed_orders = sum(
        report.order_cleanup.purged_failed for report in all_health_reports
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

    monitor_tasks: list[asyncio.Task] = []
    monitor_wallet_by_task: dict[asyncio.Task, str] = {}
    monitored_set: set[str] = set(monitored_leaders)
    fetch_interval = application.config.detection.fetch_interval
    # Bound before ``_on_new_leaders`` closes over it: the discovery loop is
    # spawned before the on-chain runtime is constructed below, so the name
    # must already exist for the closure to read at call time.
    onchain_runtime = None
    # Bound here so the shutdown path can stop it even when the on-chain
    # composition below never runs or fails part-way through.
    pending_copy_tracker = None

    async def _on_new_leaders(new_wallets: list[str]) -> list[str]:
        started_wallets = await _refresh_contexts_for_new_leaders(
            new_wallets=new_wallets,
            runtime_users=runtime_users,
            user_states=user_states,
            contexts_by_leader=contexts_by_leader,
            leaders_repo=leaders_repo,
            config=application.config,
            readiness=application.readiness,
        )

        monitored_leader_count = len(
            [wallet for wallet, contexts in contexts_by_leader.items() if contexts]
        )
        # Off-path operator projection: the roster only changes when discovery
        # promotes leaders, and the emit is synchronous and exception-guarded.
        telemetry_emitters.detection.roster_updated(monitored_leader_count)

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
            started_wallets,
            contexts_by_leader,
            dynamic_recon_failures,
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
            started_wallets = [wallet for wallet in started_wallets if wallet not in blocked_set]

        unblocked_contexts = [
            context for wallet in started_wallets for context in contexts_by_leader.get(wallet, ())
        ]
        if v1_runtime is not None and unblocked_contexts:
            v1_runtime.add_contexts(unblocked_contexts)

        for wallet in started_wallets:
            monitor_task = asyncio.create_task(
                run_leader_monitor(
                    wallet,
                    contexts_by_leader[wallet],
                    readiness_tracker=application.readiness,
                    config=application.config,
                    fetch_interval=fetch_interval,
                    monitored_leader_count=monitored_leader_count,
                    research_capture_service=research_capture_service,
                    v1_sink=v1_runtime.submit_raw_batch if v1_runtime is not None else None,
                    telemetry=telemetry_emitters.detection,
                )
            )
            monitor_tasks.append(monitor_task)
            monitor_wallet_by_task[monitor_task] = wallet
            logger.info("dynamic_leader_monitor_started", extra={"leader": wallet})
        task_monitoring_state.add(started_wallets)

        # Give the promoted leaders the mined/pending fast path too. Uses the
        # wallets whose monitors actually started, so a leader blocked by a
        # reconcile failure above is not watched on-chain either.
        if started_wallets:
            _refresh_onchain_watched_wallets(
                onchain_runtime,
                monitored_set | set(started_wallets),
            )

        application.readiness.update(
            monitored_leader_count=len(
                [wallet for wallet, contexts in contexts_by_leader.items() if contexts]
            )
        )
        return started_wallets

    # Auxiliary long-lived loops are registered as _Supervised specs and driven
    # by the main wait loop below (bounded restart, fail-closed on exhaustion)
    # instead of being fire-and-forget. Each factory must return a FRESH
    # awaitable per call so a crashed loop can be respawned.
    supervised: list[_Supervised] = []

    if not application.config.execution.paper_trade_only:
        control_refresh_interval = float(
            application.config.reconciliation.reconcile_interval_seconds
        )
        supervised.append(
            _Supervised(
                name="trading-control-refresh",
                factory=lambda: trading_permission_runtime.run(
                    interval_seconds=control_refresh_interval
                ),
            )
        )

    if application.config.discovery.scheduler.enabled and not configured:
        dynamic_monitoring_enabled = bool(application.config.discovery.scheduler.start_from_roster)
        if not dynamic_monitoring_enabled:
            logger.info(
                "discovery_roster_hot_add_disabled",
                extra={
                    "roster_hot_add_enabled": False,
                    "configured_value": False,
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
                    on_new_leaders=(_on_new_leaders if dynamic_monitoring_enabled else None),
                    already_monitored=monitored_set,
                ),
            )
        )
    elif configured:
        logger.info(
            "discovery_skipped_manual_leaders",
            extra={"configured_leader_count": len(configured)},
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
            30.0, float(application.config.reconciliation.reconcile_interval_seconds)
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

        supervised.append(_Supervised(name="reservation-reaper", factory=_reservation_reaper_loop))

    for spec in supervised:
        spec.spawn()

    # On-chain detection runtime: ``detection.mode`` selects exactly one
    # source (``on_chain_pending`` -> gen-1 provisional mempool runtime,
    # ``on_chain_confirmed`` -> gen-2 receipt-confirmed runtime). REST
    # polling below keeps running in every mode as the parallel fallback;
    # the copy layer dedups the same fill by trade key.
    detection_mode = DetectionStartupReporter(logger, metrics).report(application.config)
    if detection_mode is not DetectionMode.POLLING:
        # The confidence floor was already validated at startup (before any
        # runtime was built) precisely so an unparseable value aborts instead
        # of being absorbed by the guard below.
        #
        # The observability collaborators are likewise built OUTSIDE that
        # guard: a failure to compose a tracker or a health reporter is a
        # structural fault, and swallowing it there would silently downgrade
        # the operator's chosen detection mode to REST polling.
        pending_copy_tracker = _build_pending_copy_tracker(
            application.config,
            telemetry_service=telemetry_runtime.service,
            metrics_registry=metrics,
        )
        _ws_health_reporter = _build_onchain_ws_health_reporter(
            application.readiness,
            metrics,
            mode=detection_mode,
        )
        try:
            from orchestrators.detection_source_factory import DetectionSourceRequest
            from orchestrators.onchain_signal_dispatch import OnchainSignalDispatchAdapter
            from orchestrators.trade_pipeline import (
                execute_trades as _execute_trade_pipeline,
            )

            # One adapter routes every on-chain signal into the same copy
            # pipeline the REST-polling leader monitor uses (v1 runtime when
            # enabled, else the legacy per-context pipeline), logging
            # ``detection_signal | source=...`` provenance per signal.
            _onchain_signal_sink = OnchainSignalDispatchAdapter(
                contexts_by_leader=contexts_by_leader,
                v1_runtime=v1_runtime,
                execute_trades=_execute_trade_pipeline,
                pending_copy_tracker=pending_copy_tracker,
                telemetry=telemetry_emitters.detection,
            )
            oc_cfg = application.config.detection.onchain
            onchain_runtime = await _build_detection_source_factory(
                application.config,
                websocket_factory=_build_onchain_websocket_factory(_ws_health_reporter),
            ).build(
                DetectionSourceRequest(
                    mode=detection_mode,
                    watched_wallets=frozenset(monitored_leaders),
                    ws_url=oc_cfg.onchain_ws_url or oc_cfg.pending_tx_ws_url,
                    chain_id=application.config.wallet.chain_id,
                    signal_sink=_onchain_signal_sink,
                )
            )
            if onchain_runtime is not None:
                await _start_onchain_runtime(onchain_runtime, mode=detection_mode)
                logger.info(
                    "onchain_detection_started | mode=%s watched=%d",
                    detection_mode.value,
                    len(monitored_leaders),
                )
                if pending_copy_tracker is not None:
                    await pending_copy_tracker.start()
                # Late-bound operator read probes. Composition order puts the
                # control plane before detection, so both are attached here,
                # after the transport and tracker exist.
                telemetry_emitters.detection.attach_connection_probe(
                    _detection_connection_probe(onchain_runtime)
                )
                pending_confirmation_stats.attach(pending_copy_tracker)
        except Exception as exc:
            logger.warning(
                "onchain_detection_start_failed | mode=%s error=%s — bot continues "
                "without on-chain detection (REST-polling leader path unaffected)",
                detection_mode.value,
                redact_error_detail(exc),
            )
            onchain_runtime = None

    # Seed the stream with the startup roster size, so a UI attached at boot
    # sees the roster without waiting for the first discovery promotion (which
    # is the only other place ``roster_updated`` fires). Off-path: it runs once,
    # before any monitor exists, and the emit is synchronous and guarded.
    telemetry_emitters.detection.roster_updated(len(monitored_leaders))

    for wallet in monitored_leaders:
        monitor_task = asyncio.create_task(
            run_leader_monitor(
                wallet,
                contexts_by_leader[wallet],
                readiness_tracker=application.readiness,
                config=application.config,
                fetch_interval=fetch_interval,
                monitored_leader_count=len(monitored_leaders),
                research_capture_service=research_capture_service,
                v1_sink=v1_runtime.submit_raw_batch if v1_runtime is not None else None,
                telemetry=telemetry_emitters.detection,
            )
        )
        monitor_tasks.append(monitor_task)
        monitor_wallet_by_task[monitor_task] = wallet
    task_monitoring_state.add(monitored_leaders)

    loop = asyncio.get_running_loop()
    try:
        while True:
            # Wait on leader monitors AND every live supervised aux loop, so a
            # crash in risk / discovery is observed here rather than dying
            # silently while the process stays "up".
            active = [task for task in monitor_tasks if not task.done()]
            active += [spec.task for spec in supervised if spec.task and not spec.task.done()]

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
                    crashed_wallet = monitor_wallet_by_task.pop(task, None)
                    if crashed_wallet is not None:
                        task_monitoring_state.remove((crashed_wallet,))
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
            onchain_runtime=onchain_runtime,
            pending_copy_tracker=pending_copy_tracker,
        )


async def _close_polymarket_clients(clients: list[Any]) -> None:
    """Close each distinct SDK owner without masking the shutdown reason."""
    seen: set[int] = set()
    for client in reversed(clients):
        identity = id(client)
        if identity in seen:
            continue
        seen.add(identity)
        close = getattr(client, "close", None)
        if not callable(close):
            continue
        try:
            close_result = close()
            if inspect.isawaitable(close_result):
                await close_result
        except Exception as exc:
            logger.warning(
                "polymarket_client_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )


async def main() -> int:
    """Run the application while owning every created Polymarket SDK client."""
    clients: list[Any] = []
    try:
        return await _run_application(clients)
    finally:
        await _close_polymarket_clients(clients)
        # Startup can return before ``_shutdown_runtime`` exists (doctor,
        # validation, or compliance failures). Always close the process-wide
        # HTTP pool here too; ``close_session`` is intentionally idempotent.
        try:
            from utils.fetchData import close_session

            await close_session()
        except Exception as exc:
            logger.warning(
                "shared_http_client_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )


if __name__ == "__main__":
    from utils.event_loop import new_event_loop

    loop = new_event_loop()
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
