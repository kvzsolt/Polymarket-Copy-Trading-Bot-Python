"""Compose, run, supervise, and close the Polymarket copy-trading runtime."""

from __future__ import annotations
from pmcopy.adapters.clock import SystemReceiptClock
from pmcopy.services.telemetry.latency import PipelineLatency

import asyncio
import functools
from collections.abc import Awaitable, Callable, Iterable, Mapping
from decimal import Decimal, InvalidOperation

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pmcopy.interfaces.source_transactions import SourceTransactionStateStore
    from pmcopy.interfaces.source_time import BlockTimestampReader
    from pmcopy.orchestrators.detection_source_factory import (
        ConfirmedSourceBuilder,
        DetectionSourceRuntime,
    )
    from pmcopy.orchestrators.runtime_bootstrap import TradingContext
    from pmcopy.services.onchain_detection.leader_sell_fraction_resolver import (
        LeaderSellFractionResolver,
    )
    from pmcopy.services.onchain_detection.leader_sell_scope_guard import (
        LeaderSellBuyScopeGuard,
    )

from pmcopy.bootstrap import build_app
from pmcopy.domain.account_identity import CopierAccountId
from pmcopy.orchestrators.account_inventory_startup import InitialInventorySync
from pmcopy.config.app_config import AppConfig, EnvValidationError, load_config
from pmcopy.config.env import is_valid_eth_address
from pmcopy.domain.detection import DetectionMode
from pmcopy.integrations.polymarket.venue_runtime import build_polymarket_runtime
from pmcopy.orchestrators.leader_registry import LeaderRegistry, LeaderRegistryState
from pmcopy.orchestrators.app_lifecycle import (
    HealthServerResource as _HealthServerResource,
    RuntimeShutdownCoordinator,
    RuntimeStreamCoordinator,
    SupervisedTaskSpec as _Supervised,
    TaskSupervisor,
    close_polymarket_clients,
    empty_user_ws_api_creds as _empty_user_ws_api_creds,  # noqa: F401
    flatten_contexts as _flatten_contexts,
    get_clob_wrapper_from_user_state as _get_clob_wrapper_from_user_state,  # noqa: F401
    live_monitor_count as _live_monitor_count,  # noqa: F401
    normalise_user_ws_api_creds as _normalise_user_ws_api_creds,  # noqa: F401
    on_supervised_crash as _on_supervised_crash,  # noqa: F401
    user_ws_api_creds as _user_ws_api_creds,
)
from pmcopy.orchestrators.market_data_runtime import MarketDataRuntime
from pmcopy.orchestrators.runtime_bootstrap import (
    UserRuntimeState,
    build_context_maps,
    build_trading_context,
    build_user_state,
    ensure_runtime_follows,
    fetch_live_positions,
    hydrate_resolved_market_state,
    startup_reconcile_unconfirmed,
)
from pmcopy.orchestrators.runtime_discovery import (
    discovery_refresh_loop as _discovery_refresh_loop_impl,
    startup_refresh_targets as _startup_refresh_targets,
    wait_for_active_leaders as _wait_for_active_leaders_impl,
)
from pmcopy.orchestrators.trader_startup_policy import TraderStartupPolicy
from pmcopy.orchestrators.auto_trader_runtime import AutoTraderRuntime
from pmcopy.orchestrators.runtime_summary import print_startup_summary
from pmcopy.orchestrators.startup_health import perform_startup_health_check
from pmcopy.orchestrators.telemetry_bootstrap import TelemetryRuntime
from pmcopy.orchestrators.user_data_runtime import UserDataRuntime
from pmcopy.services.circuitBreaker import CircuitBreaker
from pmcopy.services.healthServer import start_health_server
from pmcopy.services.account_lease import AccountLeaseManager, account_lease_renewal_loop
from pmcopy.services.metrics import MetricsRegistry, metrics
from pmcopy.services.telemetry import TelemetryEmitters, WssTelemetryEmitter
from pmcopy.services.multiUserConfig import (
    RuntimeUser,
    load_runtime_users,
)
from pmcopy.services.copierConfig import select_runtime_copiers
from pmcopy.storage.postgres.leaders_repo import LeadersRepo
from pmcopy.utils.getMyBalance import get_my_balance
from pmcopy.utils.log_redaction import register_config_secrets
from pmcopy.utils.logger import (
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
    initial_state=None,
    force_discovery: bool = False,
) -> list[str]:
    return await _wait_for_active_leaders_impl(
        registry,
        configured_leaders,
        refresh_interval,
        config=config,
        initial_state=initial_state,
        force_discovery=force_discovery,
    )


async def _discovery_refresh_loop(
    registry: LeaderRegistry,
    configured_leaders: list[str],
    refresh_interval: int,
    on_new_leaders: Callable[[list[str]], Awaitable[Iterable[str] | None]] | None = None,
    already_monitored: set[str] | None = None,
    force_discovery: bool = False,
) -> None:
    await _discovery_refresh_loop_impl(
        registry,
        configured_leaders,
        refresh_interval,
        on_new_leaders=on_new_leaders,
        already_monitored=already_monitored,
        force_discovery=force_discovery,
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


def _refresh_onchain_watched_wallets(
    onchain_runtime: object | None,
    wallets: Iterable[str],
) -> bool:
    """Retire owners immediately and expose additions as awaiting restart."""
    return RuntimeStreamCoordinator(logger=logger).refresh_onchain_watched_wallets(
        onchain_runtime,
        wallets,
    )


def _should_wait_for_discovery_roster(
    config: AppConfig,
    *,
    force_discovery: bool = False,
) -> bool:
    """Whether waiting for a discovery roster can actually yield leaders.

    Mirrors ``LeaderRegistry.refresh``, which only reads the roster when
    ``start_from_roster`` is set. Without that flag the wait can never
    succeed, so startup fails fast on the empty-roster path instead of
    blocking for ``startup_wait_timeout_seconds``.
    """
    return TraderStartupPolicy(auto_trader=force_discovery).should_wait_for_discovery_roster(config)


def _reject_duplicate_user_state(user_states: dict[str, "UserRuntimeState"], user_id: str) -> None:
    """A06 — a second copier with an already-built state must fail fast.

    The strict multi-user loader rejects duplicate configured user ids, but
    identities can still collide at runtime: ``bind_resolved_deposit_wallet``
    re-keys payload users onto their resolved Deposit Wallet address, so two
    differently-named entries sharing one wallet would otherwise silently
    overwrite the first user's built state (repos, gateway, coordinators)
    with the second's. That overwrite is a hard configuration error — name
    the colliding id and refuse to continue.
    """
    if user_id in user_states:
        raise ValueError(
            f"duplicate runtime user id {user_id!r}: a second runtime user "
            "would silently overwrite the already-built user state for that "
            "id; multi-user user ids and bound wallet identities must be "
            "unique across copiers"
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

    from pmcopy.services.onchain_detection.trade_detection_models import DetectionConfidence

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
    det_mode = getattr(detection, "detection_mode", None)
    if det_mode is None or det_mode is DetectionMode.ON_CHAIN_LOGS_ONLY:
        return
    onchain = getattr(detection, "onchain", None)
    _detection_confidence_from_config(
        getattr(onchain, "pending_tx_detection_confidence_required", None)
    )


def _build_pending_tx_config(oc_cfg: object):
    from pmcopy.services.onchain_detection.pending_tx_detector import PendingTxConfig

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
    from pmcopy.services.onchain_detection.condition_id_resolver import GammaConditionIdResolver

    return GammaConditionIdResolver().resolve


def _build_onchain_ws_health_reporter(
    readiness,
    metrics_registry: MetricsRegistry = metrics,
    *,
    mode: DetectionMode,
    confirmed_source: str = "mempool_reserved",
):
    """Thin glue: the on-chain WebSocket health reporter, when it applies.

    Only the gen-1 runtime opens ``OnchainWebSocket`` subscriptions: the
    pending mode (stream ``pendingtx``) and the confirmed mode with
    ``confirmed_source=logs_only`` (stream ``orderfilled``, one socket per
    role filter). Registering ``onchain_ws_ok`` for the gen-2
    ``mempool_reserved`` runtime would publish a check that nothing can ever
    mark ready — a permanent false "WS down" for a socket the process never
    opens.

    Args:
        readiness: Readiness tracker the ``onchain_ws_ok`` check registers on.
        metrics_registry: Registry receiving the bounded on-chain WS metrics.
        mode: Selected detection mode.
        confirmed_source: Selected confirmed-source variant
            (``detection.onchain.confirmed_source``).

    Returns:
        ``OnchainWsHealthReporter`` for ``ON_CHAIN_PENDING`` and for
        ``ON_CHAIN_CONFIRMED`` + ``logs_only``; ``None`` otherwise.
    """

    from pmcopy.services.onchain_detection.ws_health_reporter import OnchainWsHealthReporter

    if mode in {
        DetectionMode.ON_CHAIN_PENDING,
        DetectionMode.ON_CHAIN_LOGS_ONLY,
    }:
        return OnchainWsHealthReporter(metrics_registry, readiness)
    return None


def _build_onchain_websocket_factory(state_listener):
    """Thin glue: the subscription constructor, health-reporting when asked.

    Args:
        state_listener: Connection-state observer, or ``None``.

    Returns:
        ``OnchainWebSocket`` itself when no listener is supplied, else a
        partial with the listener bound.
    """

    from pmcopy.clients.onchain_ws import OnchainWebSocket

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

    A composed tracker is still inert under ``execution.trading_mode=paper``:
    the dispatcher only tracks a copy whose pipeline outcome reached the
    venue, and every paper copy stops at the ``paper`` tier before submission.
    That is announced once here so a paper soak with zero ``pending_copy_*``
    output is read as "not exercised", never as "healthy".

    Args:
        config: Typed application configuration.
        telemetry_service: Process telemetry service, or ``None``.
        metrics_registry: Registry receiving the ``pending_copy_*`` metrics.

    Returns:
        A not-yet-started ``PendingCopyConfirmationTracker``, or ``None``.
    """

    from pmcopy.clients.network.web3_receipt_fetcher import Web3ReceiptStatusFetcher
    from pmcopy.services.onchain_detection.pending_copy_confirmation_tracker import (
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
    if bool(getattr(getattr(config, "execution", None), "paper_trade_only", False)):
        logger.info(
            "pending_copy_tracker_idle_in_paper_mode | the tracker is composed but "
            "records nothing while execution.trading_mode=paper: every copy stops at "
            "the paper tier before reaching the venue, so zero pending_copy_* output "
            "means NOT EXERCISED, not healthy"
        )
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


def _build_leader_sell_fraction_resolver(
    config: object,
    *,
    buy_scope_guard: "LeaderSellBuyScopeGuard | None" = None,
    provider_runtime: Any | None = None,
) -> "LeaderSellFractionResolver":
    """Compose exact and fallback readers for authoritative leader SELLs.

    Args:
        config: Typed application configuration containing Polygon RPC URLs.
        buy_scope_guard: Shared durable degraded-scope BUY guard.

    Returns:
        A resolver that annotates mined SELL signals before copy dispatch.

    Raises:
        ValueError: If the required primary Polygon RPC endpoint is absent.
    """

    from pmcopy.adapters.polygon.historical_erc1155_balance import (
        Web3HistoricalErc1155BalanceReader,
    )
    from pmcopy.integrations.polymarket.leader_sell_fraction_data_api import (
        DataApiLeaderSellFractionReader,
    )
    from pmcopy.services.onchain_detection.leader_sell_fraction_resolver import (
        LeaderSellFractionResolver,
    )
    from pmcopy.clients.network.decimal_json_reader import DecimalJsonReader
    from pmcopy.utils.fetchData import _get_http_client

    polymarket = getattr(config, "polymarket", None)
    historical = Web3HistoricalErc1155BalanceReader.from_rpc_url(
        str(getattr(polymarket, "rpc_url", "") or ""),
        str(getattr(polymarket, "fallback_rpc_url", "") or ""),
        request_timeout_seconds=2.5,
        provider_runtime=provider_runtime,
    )
    fallback = DataApiLeaderSellFractionReader(
        fetch_json=DecimalJsonReader(_get_http_client).get,
    )
    return LeaderSellFractionResolver(
        historical_balances=historical,
        fallback_fractions=fallback,
        buy_scope_guard=buy_scope_guard,
    )


def _copy_task_contexts(
    contexts_by_leader: Mapping[str, list["TradingContext"]],
    risk_contexts_by_user: Mapping[str, "TradingContext"],
) -> tuple["TradingContext", ...]:
    """Bind each context once, including accounts that only manage existing positions."""
    return tuple(
        {
            id(context): context
            for context in (
                *risk_contexts_by_user.values(),
                *(item for contexts in contexts_by_leader.values() for item in contexts),
            )
        }.values()
    )


def _confirmed_source_builder(
    state_store_provider: "Callable[[], Awaitable[SourceTransactionStateStore | None]]",
    block_timestamp_reader_factory: Callable[[], BlockTimestampReader] | None = None,
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

    from pmcopy.config.addresses import (
        POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
        POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    )
    from pmcopy.orchestrators.detection_source_factory import ConfirmedSourceBuilder

    return ConfirmedSourceBuilder(
        exchange_addresses=(
            POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
            POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
        ),
        state_store_provider=state_store_provider,
        block_timestamp_reader_factory=block_timestamp_reader_factory,
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

    from pmcopy.adapters.persistence.source_transaction_state_store import (
        PostgresSourceTransactionStateStore,
    )
    from pmcopy.storage.postgres.connection import get_pool

    return PostgresSourceTransactionStateStore(await get_pool())


def _build_detection_source_factory(
    config: object,
    *,
    state_store_provider=None,
    websocket_factory=None,
):
    """Thin glue: one ``DetectionSourceFactory`` covering every detection key.

    Args:
        config: Typed application configuration (``detection`` section read).
        state_store_provider: Awaitable factory for the confirmed runtime's
            durable state store; defaults to the shared Postgres pool.
        websocket_factory: ``OnchainWebSocket``-compatible constructor for the
            gen-1 runtimes' subscriptions; ``app.main`` binds the health
            reporter here. Defaults to the plain client.

    Returns:
        ``DetectionSourceFactory`` with the provisional-pending,
        logs-only confirmed, and receipt-confirmed (mempool-reserved)
        builders registered.
    """

    from pmcopy.clients.onchain_ws import OnchainWebSocket
    from pmcopy.config.addresses import (
        POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
        POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    )
    from pmcopy.orchestrators.detection_source_factory import (
        DetectionSourceFactory,
        LogsOnlyConfirmedSourceBuilder,
        ProvisionalPendingSourceBuilder,
    )

    oc_cfg = getattr(getattr(config, "detection", None), "onchain", None)
    from pmcopy.adapters.polygon.block_timestamp import RpcBlockTimestampReaderFactory
    from pmcopy.utils.fetchData import _get_http_client

    rpc_url = getattr(getattr(config, "polymarket", None), "rpc_url", "")
    block_time_factory = (
        RpcBlockTimestampReaderFactory(rpc_url=rpc_url, client_provider=_get_http_client).build
        if rpc_url
        else None
    )
    return DetectionSourceFactory(
        [
            ProvisionalPendingSourceBuilder(
                pending_config=_build_pending_tx_config(oc_cfg),
                condition_id_resolver=_build_onchain_condition_id_resolver(),
                websocket_factory=websocket_factory or OnchainWebSocket,
            ),
            LogsOnlyConfirmedSourceBuilder(
                exchange_addresses=(
                    POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
                    POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
                ),
                condition_id_resolver=_build_onchain_condition_id_resolver(),
                websocket_factory=websocket_factory or OnchainWebSocket,
                block_timestamp_reader_factory=block_time_factory,
            ),
            _confirmed_source_builder(
                state_store_provider or _default_source_state_store_provider,
                block_timestamp_reader_factory=block_time_factory,
            ),
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
    from pmcopy.services.risk.risk_monitor import _reap_expired_reservations_for_context

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
    """Return wallets whose downstream contexts include any failed user_id."""
    return RuntimeStreamCoordinator.wallets_blocked_by_reconcile_failures(
        started_wallets,
        contexts_by_leader,
        failed_user_ids,
    )


def _cold_start_monitored_after_reconcile(
    monitored_leaders: list[str],
    contexts_by_leader: dict[str, list["TradingContext"]],
    reconcile_failures: list[str],
) -> list[str]:
    """Filter the cold-start leader set to exclude failed-reconcile accounts."""
    return RuntimeStreamCoordinator.cold_start_monitored_after_reconcile(
        monitored_leaders,
        contexts_by_leader,
        reconcile_failures,
    )


async def _refresh_contexts_for_new_leaders(
    *,
    new_wallets: list[str],
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    contexts_by_leader: dict[str, list],
    leaders_repo: LeadersRepo | AutoTraderRuntime,
    config,
    readiness,
) -> list[str]:
    return await RuntimeStreamCoordinator(logger=logger).refresh_contexts_for_new_leaders(
        new_wallets=new_wallets,
        runtime_users=runtime_users,
        user_states=user_states,
        contexts_by_leader=contexts_by_leader,
        leaders_repo=leaders_repo,
        config=config,
        readiness=readiness,
        context_builder=build_trading_context,
    )


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
    latency: PipelineLatency | None = None,
    evidence_runtime: Any | None = None,
    runtimes_out: dict[str, MarketDataRuntime] | None = None,
) -> dict[str, MarketDataRuntime]:
    """Start WSS-backed market data runtimes, keyed by user_id."""
    return await RuntimeStreamCoordinator(logger=logger).start_market_data_runtimes(
        runtime_users=runtime_users,
        user_states=user_states,
        live_positions_by_user=live_positions_by_user,
        contexts_by_leader=contexts_by_leader,
        readiness=readiness,
        config=config,
        quote_sink=quote_sink,
        telemetry=telemetry,
        latency=latency,
        evidence_runtime=evidence_runtime,
        market_runtime_cls=MarketDataRuntime,
        runtimes_out=runtimes_out,
    )


def _attach_market_data_runtime_to_contexts(
    *,
    contexts_by_leader: dict[str, list["TradingContext"]],
    risk_contexts_by_user: dict[str, "TradingContext"],
    runtimes_by_user: dict[str, MarketDataRuntime],
) -> None:
    """Bind each user's MarketDataRuntime onto their TradingContext."""
    RuntimeStreamCoordinator.attach_market_data_runtime_to_contexts(
        contexts_by_leader=contexts_by_leader,
        risk_contexts_by_user=risk_contexts_by_user,
        runtimes_by_user=runtimes_by_user,
    )


def _build_reconcile_wake_closure(context):
    """Return an async wake closure that converges ReconciliationService."""
    return RuntimeStreamCoordinator(logger=logger).build_reconcile_wake_closure(context)


def _build_risk_cache_evict_closure(context):
    """Return a sync callable that evicts a token from ``RiskPriceCache``."""
    return RuntimeStreamCoordinator.build_risk_cache_evict_closure(context)


def _build_market_token_subscribe_closure(runtime: MarketDataRuntime):
    """Return a user-fill hook that subscribes newly seen tokens to MarketWS."""
    return RuntimeStreamCoordinator.build_market_token_subscribe_closure(runtime)


async def _start_user_data_runtimes(
    *,
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    readiness,
    risk_contexts_by_user: dict[str, "TradingContext"] | None = None,
    market_data_runtimes: dict[str, MarketDataRuntime] | None = None,
    telemetry: WssTelemetryEmitter | None = None,
    runtimes_out: list[UserDataRuntime] | None = None,
) -> list[UserDataRuntime]:
    """Start authenticated user WSS runtimes."""
    return await RuntimeStreamCoordinator(logger=logger).start_user_data_runtimes(
        runtime_users=runtime_users,
        user_states=user_states,
        readiness=readiness,
        risk_contexts_by_user=risk_contexts_by_user,
        market_data_runtimes=market_data_runtimes,
        telemetry=telemetry,
        api_creds_resolver=_user_ws_api_creds,
        user_runtime_cls=UserDataRuntime,
        runtimes_out=runtimes_out,
    )


async def _subscribe_context_tokens(
    *,
    market_data_runtimes: dict[str, MarketDataRuntime],
    contexts_by_leader: dict[str, list],
) -> None:
    """Subscribe tokens visible from runtime contexts."""
    await RuntimeStreamCoordinator(logger=logger).subscribe_context_tokens(
        market_data_runtimes=market_data_runtimes,
        contexts_by_leader=contexts_by_leader,
    )


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
        The started telemetry runtime used by detection and readiness probes.
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
    return TelemetryEmitters.build(
        telemetry_runtime.service,
        latency=PipelineLatency(
            clock=SystemReceiptClock(), metrics=telemetry_runtime.service.metrics_registry
        ),
    )


def _detection_connection_probe(runtime: object) -> Callable[[], bool | None]:
    """Return an O(1) transport-health probe for one detection runtime.

    Detection sources start after the telemetry emitters exist, so the emitter
    is given this probe only after the runtime started. A runtime that exposes
    no connection state reports ``None`` (unknown) rather than a fabricated
    ``False``.

    Args:
        runtime: Started detection source runtime.

    Returns:
        A callable returning the runtime's connection state, or ``None``.
    """

    def probe() -> bool | None:
        state = getattr(runtime, "connected", None)
        return state if isinstance(state, bool) else None

    return probe


async def _shutdown_runtime(
    *,
    monitor_tasks: list[asyncio.Task],
    supervised: list[_Supervised],
    market_data_runtimes: dict[str, MarketDataRuntime],
    user_data_runtimes: list[UserDataRuntime],
    metrics_runner,
    onchain_runtime=None,
    pending_copy_tracker=None,
    v1_runtime=None,
    evidence_drain=None,
    account_lease_manager: AccountLeaseManager | None = None,
) -> None:
    coordinator = RuntimeShutdownCoordinator(logger=logger)
    await coordinator.cancel_tasks_and_stop_streams(
        monitor_tasks=monitor_tasks,
        supervised=supervised,
        market_data_runtimes=market_data_runtimes,
        user_data_runtimes=user_data_runtimes,
        onchain_runtime=onchain_runtime,
        pending_copy_tracker=pending_copy_tracker,
        v1_runtime=v1_runtime,
        evidence_drain=evidence_drain,
    )

    # Task 21 — release every held account lease BEFORE the pool closes.
    if account_lease_manager is not None:
        try:
            await account_lease_manager.close()
        except Exception as exc:
            logger.warning(
                "account_lease_release_failed",
                extra={"error": redact_error_detail(exc)},
            )

    try:
        from pmcopy.utils.fetchData import close_session

        await close_session()
    except Exception as exc:
        logger.warning(
            "failed_to_close_http_session",
            extra={"error": redact_error_detail(exc)},
        )

    if metrics_runner:
        await metrics_runner.cleanup()

    try:
        from pmcopy.storage.postgres.connection import shutdown_pool

        await shutdown_pool()
    except Exception as exc:
        logger.warning(
            "db_pool_shutdown_failed",
            extra={"error": redact_error_detail(exc)},
        )


async def _run_application(
    client_registry: list[Any],
    *,
    startup_policy: TraderStartupPolicy | None = None,
    evidence_runtime: Any | None = None,
    copier: str | None = None,
) -> int:
    """Run one fully composed application under a leader startup policy."""
    startup_policy = startup_policy or TraderStartupPolicy()
    configure_logging(level="INFO")

    try:
        startup_config = load_config()
        startup_policy.validate(startup_config)
        startup_config = startup_policy.effective_config(startup_config)
        _validate_detection_confidence_floor(startup_config)
    except EnvValidationError as exc:
        print_error(str(exc))
        return 1

    # Populate the logging secret-value registry from the validated config
    # (wallet private key, DB DSN, Falcon/relayer tokens).
    # This is the single registration seam: it never logs what it collects,
    # and logging stays safe by default via key/pattern redaction even when
    # it registers nothing (e.g. tests or pre-config logging).
    register_config_secrets(startup_config)

    configure_logging(
        level=startup_config.logging.level,
        log_dir=startup_config.logging.log_dir,
        jsonl_file=startup_config.logging.app_log_jsonl_file,
        max_bytes=startup_config.logging.log_max_bytes,
        backup_count=startup_config.logging.log_backup_count,
    )

    from pmcopy.orchestrators.provider_runtime import ProviderRuntime
    from pmcopy.domain.provider_budget import ProviderConfigurationError

    try:
        provider_runtime = ProviderRuntime.from_config(startup_config)
    except ProviderConfigurationError:
        print_error(
            "Approved provider budgets and endpoint configuration must be verified before startup."
        )
        return 1
    client_registry.append(provider_runtime)

    from pmcopy.orchestrators.evidence_runtime import EvidenceRuntime
    from pmcopy.domain.evidence import EvidenceError

    # ``evidence.enabled`` is the operator's only opt-out. It defaults to true in
    # ``EvidenceConfig``, so a deployment that never sets it still fails closed on
    # a missing private credential. Turning it off only disables durable capture
    # and remote publication: native-owner detection still composes and
    # dispatches, discarding raw observations instead of pausing. Pending
    # evaluation genuinely needs a durable sink and fails closed in ``compose``.
    evidence_settings = startup_config.evidence
    if evidence_runtime is None and not evidence_settings.enabled:
        if evidence_settings.history_journal_path is not None:
            print_error(
                "Durable evidence is disabled while evidence.history_journal_path is "
                "configured. Enable evidence, or unset history_journal_path."
            )
            return 1
    else:
        try:
            if evidence_runtime is None:
                evidence_runtime = EvidenceRuntime.from_config(startup_config)
            client_registry.append(evidence_runtime)
            await evidence_runtime.start()
        except (EvidenceError, OSError):
            print_error(
                "Durable local evidence storage and private receiver configuration are required."
            )
            return 1

    import sqlite3

    from pmcopy.orchestrators.history_evidence_drain_runtime import HistoryEvidenceDrainRuntime

    if evidence_runtime is not None:
        try:
            history_drain = HistoryEvidenceDrainRuntime.from_config(
                startup_config.evidence, evidence_runtime
            )
            if history_drain is not None:
                client_registry.append(history_drain)
                await history_drain.start()
        except (ValueError, OSError, sqlite3.Error):
            print_error("The configured historical evidence journal could not be opened safely.")
            return 1

    from pmcopy.bot_cli.commands.doctor import run_doctor_checks

    mode = "paper" if startup_config.execution.paper_trade_only else "live"

    if mode == "live":
        doctor_result = await run_doctor_checks(
            "live",
            config=startup_config,
            auto_trader=startup_policy.auto_trader,
            provider_runtime=provider_runtime,
        )
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
    application = await build_app(config=startup_config, provider_runtime=provider_runtime)
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
    heartbeat_state: dict[str, Any] = {}
    metrics_runner: _HealthServerResource | None = None
    if application.config.app.enable_metrics_server:
        metrics_runner = _HealthServerResource(
            await start_health_server(
                application.config.app.metrics_port,
                tracker=application.readiness,
                host=application.config.app.metrics_bind_host,
                telemetry_query=telemetry_runtime.query if telemetry_runtime is not None else None,
                telemetry_hub=telemetry_runtime.hub if telemetry_runtime is not None else None,
                detection_telemetry=lambda: (
                    det.telemetry_snapshot()
                    if (det := heartbeat_state.get("detection")) is not None
                    and hasattr(det, "telemetry_snapshot")
                    else None
                ),
            )
        )
        client_registry.append(metrics_runner)

    configured = startup_policy.configured_leaders(application.config)
    invalid = [wallet for wallet in configured if not is_valid_eth_address(wallet)]

    if invalid:
        for wallet in invalid:
            print_error(
                f'Invalid leader wallet "{wallet}" — must be 42-char hex (0x + 40 hex digits).'
            )
        return 1

    auto_traders = None
    auto_runtime_users = None
    if startup_policy.auto_trader:
        from pmcopy.orchestrators.auto_trader_runtime import AutoTraderRuntime
        from pmcopy.storage.postgres.connection import get_pool as get_auto_trader_pool

        # Task 22 — the --copier selector limits routing AND runtime
        # ownership, so it is applied at BOTH roster load sites before any
        # account runtime or lease exists.
        try:
            auto_runtime_users = select_runtime_copiers(
                load_runtime_users([], application.config), copier
            )
        except ValueError as exc:
            print_error(str(exc))
            return 1
        auto_traders = AutoTraderRuntime.create(
            application=application, pool=await get_auto_trader_pool(), users=auto_runtime_users
        )
        application.auto_traders = auto_traders
        automatic_wallets = await auto_traders.startup(
            legacy_wallets=await application.leaders_repo.list_active()
        )
        registry_state = LeaderRegistryState(active_monitored=automatic_wallets, mode="automatic")
    else:
        registry_state = await application.registry.refresh(
            configured,
            force_discovery=startup_policy.force_discovery,
            target_wallets=await _startup_refresh_targets(
                application.registry,
                force_discovery=startup_policy.force_discovery,
            ),
        )
    active_leaders = registry_state.active_monitored

    invalid_active = [wallet for wallet in active_leaders if not is_valid_eth_address(wallet)]
    if invalid_active:
        logger.error(
            "startup_blocked_no_active_leaders",
            extra={
                "mode": registry_state.mode,
                "reason": "invalid_active_leaders",
                "invalid_wallets": invalid_active,
                "scheduler_enabled": application.config.discovery.scheduler.enabled,
                "falcon_enabled": application.config.discovery.falcon.enabled,
                "force_discovery": startup_policy.force_discovery,
                "max_active_leaders": (application.config.discovery.scheduler.max_active_leaders),
            },
        )
        print_error("No active leaders available. Auto-selected leaders were invalid.")
        return 1

    if not active_leaders and auto_traders is None:
        if _should_wait_for_discovery_roster(
            application.config,
            force_discovery=startup_policy.force_discovery,
        ):
            logger.info(
                startup_policy.startup_roster_event,
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
                force_discovery=startup_policy.force_discovery,
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

    if not active_leaders and auto_traders is None:
        logger.error(
            "startup_blocked_no_active_leaders",
            extra={
                "mode": registry_state.mode,
                "reason": "manual_and_roster_empty",
                "scheduler_enabled": application.config.discovery.scheduler.enabled,
                "falcon_enabled": application.config.discovery.falcon.enabled,
                "force_discovery": startup_policy.force_discovery,
                "max_active_leaders": (application.config.discovery.scheduler.max_active_leaders),
            },
        )
        print_error(
            "No active leaders available. No enabled traders in "
            "config/traders/*.yml and auto trader detection did not find "
            "eligible candidates."
        )
        return 1

    try:
        runtime_users = select_runtime_copiers(
            (
                auto_runtime_users
                if auto_runtime_users is not None
                else load_runtime_users(active_leaders, application.config)
            ),
            copier,
        )
    except ValueError as exc:
        # An unknown --copier id must fail closed here: never start an
        # unexpected subset of accounts (addon §7 partial-startup policy).
        print_error(str(exc))
        return 1
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
    from pmcopy.config.db import connect_db as _connect_db

    await _connect_db()

    from pmcopy.storage.postgres.connection import get_pool

    # Acquire the pool first so it's available to the per-user wiring below
    # even when the (non-critical, post-lease) purge step fails. Previously
    # `_pool` was scoped inside the try/except, so a degraded purge would
    # NameError on the subsequent `build_user_state(..., _pool, ...)`
    # reference.
    _pool = await get_pool()
    from pmcopy.adapters.clock import SystemReceiptClock
    from pmcopy.orchestrators.evidence_drain_runtime import EvidenceDrainRuntime
    from pmcopy.storage.postgres.evidence_drain import PostgresEvidenceDrain
    from pmcopy.storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection

    evidence_projection = PostgresFillEvidenceProjection(
        clock=SystemReceiptClock(),
        execution_mode=application.config.execution.trading_mode,
    )
    # The database-to-spool drain has nothing to append to without a local
    # evidence owner, so it is composed only alongside one. Composing it against
    # a missing sink would leave a worker retrying forever instead of reporting
    # an honest absence.
    evidence_drain = None
    if evidence_runtime is not None:
        evidence_drain = EvidenceDrainRuntime(
            PostgresEvidenceDrain(_pool, evidence_runtime, batch_size=25)
        )
        client_registry.append(evidence_drain)
        # Final-review fix: the worker itself starts only AFTER every
        # account lease is acquired below. The drain reads the shared
        # database outbox; a doomed second process (lease refused) must
        # never drain it. The registry entry stays here so shutdown closes
        # the drain on every path (aclose is a no-op when never started).
    from pmcopy.services.onchain_detection.leader_sell_scope_guard import LeaderSellScopeGuard
    from pmcopy.storage.postgres.leader_sell_scope_repo import LeaderSellSafetyScopeRepo

    leader_sell_scope_guard = LeaderSellScopeGuard(
        store=LeaderSellSafetyScopeRepo(_pool),
    )

    leaders_repo = auto_traders if auto_traders is not None else application.leaders_repo
    registry = application.registry
    refresh_interval = application.config.discovery.scheduler.rediscover_seconds

    # Follows are explicit per-copier whitelists (A05). This call applies
    # the documented legacy default — the single account whose follows did
    # NOT come from explicit configuration (the config-less fallback)
    # auto-follows the active leaders. That default re-applies at every
    # dynamic refresh too, so the lone config-less account keeps
    # auto-following newly discovered leaders. Every other deployment
    # copies exactly what its follows configuration lists.
    ensure_runtime_follows(runtime_users, active_leaders, application.config)

    user_states: dict[str, UserRuntimeState] = {}

    # Task 21 — cross-process account lease: acquire every account's lease
    # BEFORE any venue runtime or user state exists, so a second process
    # configured for the same wallet+database fails closed here instead of
    # becoming a second active writer. ONE lease per account regardless of
    # paper/live mode: both modes write money-shaped rows (reservations,
    # orders, positions) to this database. Renewal is a supervised loop
    # below; graceful shutdown releases via ``_shutdown_runtime`` (and the
    # client registry on exception paths).
    account_lease_manager = AccountLeaseManager(pool=_pool)
    for runtime_user in runtime_users:
        lease_outcome = await account_lease_manager.acquire(
            CopierAccountId.from_runtime_user(runtime_user),
            mode=mode,
        )
        if not lease_outcome.acquired:
            held_by = lease_outcome.held_by
            print_error(
                "refusing to start a second writer for this account: lease for "
                f"{runtime_user.user_id!r} is held by another active process "
                f"(holder host={getattr(held_by, 'holder_host', '?')}, "
                f"expires_at={getattr(held_by, 'expires_at', '?')})."
            )
            await account_lease_manager.release_all()
            return 1
    client_registry.append(account_lease_manager)
    logger.info(
        "startup_stage | stage=account_leases_acquired users=%d mode=%s",
        len(runtime_users),
        mode,
    )
    # Leases held — now the shared-outbox drain worker may start (see the
    # composition note above: composition precedes the lease loop, the
    # worker start deliberately does not).
    if evidence_drain is not None:
        await evidence_drain.start()

    # Follow-up fix: the retention purge is a shared-database WRITE, so it
    # runs only AFTER every account lease is acquired (mirroring the drain
    # ordering above): a doomed second process (lease refused) must never
    # delete rows from the shared database. Skipping the purge on the
    # refused path needs no cleanup — stale rows simply survive until the
    # next lease-holding start.
    from pmcopy.storage.postgres.orders_repo import OrdersRepo as _PgOrdersRepo

    _max_age = float(application.config.app.retention_max_age_seconds)
    try:
        purged = await _PgOrdersRepo(_pool).purge_old_processed_trades(_max_age)
    except RuntimeError:
        # Defensive: should not fire now that connect_db() runs above,
        # but kept so a degraded PG init does not block startup on a
        # non-critical purge.
        purged = 0
    if purged:
        logger.info("Startup cleanup: purged %s stale records", purged)

    venue_circuit_breaker = CircuitBreaker.for_venue_config(
        application.config,
        on_state_change=lambda st: application.readiness.update(circuit_breaker_state=st.value),
    )

    for runtime_user in runtime_users:
        venue_runtime = await build_polymarket_runtime(
            runtime_user=runtime_user,
            config=application.config,
            latency=telemetry_emitters.latency if telemetry_emitters is not None else None,
        )
        # The CCXT runtime is the single venue owner; close it on shutdown.
        client_registry.append(venue_runtime)
        # A06 — wallet binding above may have re-keyed this user onto a
        # resolved Deposit Wallet address; a collision with an already-built
        # state must fail fast instead of silently overwriting it.
        _reject_duplicate_user_state(user_states, runtime_user.user_id)
        user_states[runtime_user.user_id] = build_user_state(
            runtime_user,
            venue_runtime.client,
            _pool,
            application.config,
            clob_gateway=venue_runtime.gateway,
            telemetry=telemetry_emitters,
            copy_admission=auto_traders,
            provider_runtime=provider_runtime,
            evidence_projection=evidence_projection,
            venue_circuit_breaker=venue_circuit_breaker,
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
        *(get_my_balance(wallet, provider_runtime=provider_runtime) for wallet in active_leaders)
    )
    user_balances_raw = await asyncio.gather(
        *(
            get_my_balance(runtime_user.wallet.funder_address, provider_runtime=provider_runtime)
            for runtime_user in runtime_users
        )
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
        # None == fetch failed (fail-closed); the inventory inspection treats it
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
            provider_runtime=provider_runtime,
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
    for context in {
        id(candidate): candidate
        for candidate in (
            *risk_contexts_by_user.values(),
            *(item for values in contexts_by_leader.values() for item in values),
        )
    }.values():
        context.leader_sell_scope_guard = leader_sell_scope_guard
    from pmcopy.orchestrators.trade_pipeline import seed_position_pct_cap_snapshot

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
    from pmcopy.orchestrators.v1_runtime_bootstrap import start_v1_copy_task_runtime

    logger.info("startup_stage | stage=v1_copy_task_restoration_started")
    initial_v1_contexts = _copy_task_contexts(contexts_by_leader, risk_contexts_by_user)
    v1_runtime = await start_v1_copy_task_runtime(
        pool=_pool,
        config=application.config,
        copy_tasks_config=application.config.copy_trading.copy_tasks,
        contexts=initial_v1_contexts,
        start_workers=False,
        buy_scope_guard=leader_sell_scope_guard,
        decision_observer=telemetry_emitters.trade,
        latency=telemetry_emitters.latency,
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
    from pmcopy.orchestration.trading_permission_runtime import TradingPermissionRuntime

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

    # Seed inventory before drift checks and market/WSS startup so all loops
    # start against reconciled holdings, including positive zero-mark shares.
    inventory_startup = InitialInventorySync(application.readiness)
    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        manual_sync = user_state.manual_sync_service if user_state else None
        await inventory_startup.initialize(manual_sync)

    # PR7 — proactive position drift sweep. Must complete before monitors
    # start AND before the user WSS attaches so its first auth-triggered
    # manual_sync sees the prune timestamps.
    from pmcopy.orchestrators.runtime_bootstrap import startup_drift_check

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

    monitor_tasks: list[asyncio.Task] = []
    monitor_wallet_by_task: dict[asyncio.Task, str] = {}
    monitored_set: set[str] = set(monitored_leaders)
    supervised: list[_Supervised] = []
    market_data_runtimes: dict[str, MarketDataRuntime] = {}
    user_data_runtimes: list[UserDataRuntime] = []
    onchain_runtime = None
    pending_copy_tracker = None

    # WP-B: enter the deterministic shutdown guard BEFORE starting background
    # workers, market/user WSS streams, paper evaluation, or supervised loops
    # so any mid-startup exception tears down every started runtime cleanly.
    try:
        if v1_runtime is not None:
            await v1_runtime.start()

        # Start market-data WSS only after portfolio/database convergence.
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
            latency=telemetry_emitters.latency,
            evidence_runtime=evidence_runtime,
            runtimes_out=market_data_runtimes,
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
        # after drift_check populated prune timestamps.
        logger.info("startup_stage | stage=user_data_runtimes_starting")
        user_data_runtimes = await _start_user_data_runtimes(
            runtime_users=runtime_users,
            user_states=user_states,
            readiness=application.readiness,
            risk_contexts_by_user=risk_contexts_by_user,
            market_data_runtimes=market_data_runtimes,
            telemetry=telemetry_emitters.wss,
            runtimes_out=user_data_runtimes,
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
            telemetry_emitters.detection.roster_updated(monitored_leader_count)

            await _subscribe_context_tokens(
                market_data_runtimes=market_data_runtimes,
                contexts_by_leader=contexts_by_leader,
            )

            new_contexts: list["TradingContext"] = []
            for wallet in started_wallets:
                new_contexts.extend(contexts_by_leader.get(wallet, []))
            dynamic_recon_failures: list[str] = []
            if new_contexts:
                for context in new_contexts:
                    context.leader_sell_scope_guard = leader_sell_scope_guard
                await startup_reconcile_unconfirmed(
                    new_contexts,
                    failures_out=dynamic_recon_failures,
                )

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
                for blocked_wallet in blocked_set:
                    contexts_by_leader.pop(blocked_wallet, None)
                started_wallets = [wallet for wallet in started_wallets if wallet not in blocked_set]

            _attach_market_data_runtime_to_contexts(
                contexts_by_leader=contexts_by_leader,
                risk_contexts_by_user=risk_contexts_by_user,
                runtimes_by_user=market_data_runtimes,
            )

            unblocked_contexts = [
                context
                for wallet in started_wallets
                for context in contexts_by_leader.get(wallet, ())
            ]
            if v1_runtime is not None and unblocked_contexts:
                v1_runtime.add_contexts(unblocked_contexts)

            if started_wallets:
                for wallet in started_wallets:
                    if wallet not in monitored_set:
                        monitored_leaders.append(wallet)
                        monitored_set.add(wallet)
                if onchain_runtime is not None:
                    onchain_runtime.update_watched_wallets(monitored_set)
                logger.info(
                    "alchemy_detection_roster_updated | added=%d total=%d",
                    len(started_wallets),
                    len(monitored_set),
                )
            application.readiness.update(
                monitored_leader_count=len(onchain_runtime.active_wallets())
                if onchain_runtime is not None
                else len(monitored_set)
            )
            return started_wallets

        async def _on_retired_leaders(wallets: list[str]) -> None:
            """Retire only monitors already proven free of outstanding exit ownership."""
            retired = set(wallets)
            tasks = [task for task, wallet in monitor_wallet_by_task.items() if wallet in retired]
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            for task in tasks:
                monitor_wallet_by_task.pop(task, None)
                if task in monitor_tasks:
                    monitor_tasks.remove(task)
            for wallet in retired:
                contexts_by_leader.pop(wallet, None)
                if v1_runtime is not None:
                    await v1_runtime.retire_leader(wallet)
            monitored_leaders[:] = [wallet for wallet in monitored_leaders if wallet not in retired]
            monitored_set.difference_update(retired)
            _refresh_onchain_watched_wallets(onchain_runtime, monitored_set)
            application.readiness.update(monitored_leader_count=len(monitored_set))
            telemetry_emitters.detection.roster_updated(len(monitored_set))

        recovery_services = {
            user_id: getattr(state, "ambiguous_recovery", None)
            for user_id, state in user_states.items()
        }
        recovery_services = {
            user_id: service for user_id, service in recovery_services.items() if service is not None
        }
        if recovery_services:
            from pmcopy.orchestrators.runtime_bootstrap import ambiguous_recovery_loop

            _recovery_interval = float(
                application.config.reconciliation.ambiguous_recovery.interval_seconds
            )
            supervised.append(
                _Supervised(
                    name="ambiguous-recovery",
                    factory=lambda: ambiguous_recovery_loop(
                        recovery_services, interval_seconds=_recovery_interval
                    ),
                )
            )
            logger.info(
                "ambiguous_recovery_wired | users=%d interval_seconds=%s",
                len(recovery_services),
                _recovery_interval,
            )

        paper_evaluation = None
        if auto_traders is not None:
            from pmcopy.orchestrators.auto_trader_paper_runtime import AutoTraderPaperRuntime

            paper_evaluation = AutoTraderPaperRuntime.create(
                lifecycle=auto_traders,
                pool=_pool,
                states=user_states,
                config=application.config,
                contexts=contexts_by_leader,
                seed_funds=user_balances,
                clock=AutoTraderRuntime.now,
            )
            await paper_evaluation.prepare()
            supervised.append(_Supervised(name="auto-trader-paper", factory=paper_evaluation.run))

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

        if auto_traders is not None:
            supervised.append(
                _Supervised(
                    name="auto-trader-evaluation",
                    factory=lambda: auto_traders.run(
                        evaluation_seconds=application.config.discovery.scheduler.rescore_seconds,
                        falcon_seconds=application.config.discovery.scheduler.rediscover_seconds,
                        on_new_leaders=_on_new_leaders,
                        monitored=monitored_set,
                        on_retired_leaders=_on_retired_leaders,
                        evaluate_results=paper_evaluation.evaluate_results
                        if paper_evaluation is not None
                        else None,
                    ),
                )
            )
        elif startup_policy.discovery_loop_enabled(application.config) and not configured:
            dynamic_monitoring_enabled = bool(
                application.config.discovery.scheduler.start_from_roster
            )
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
                        force_discovery=startup_policy.force_discovery,
                    ),
                )
            )
        elif configured:
            logger.info(
                "discovery_skipped_manual_leaders",
                extra={"configured_leader_count": len(configured)},
            )

        if application.config.risk.enable_risk_mgmt:
            from pmcopy.services.risk.risk_monitor import risk_monitor_loop_for_contexts

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

        if not application.config.risk.enable_risk_mgmt:
            _reaper_interval = max(
                30.0, float(application.config.reconciliation.reconcile_interval_seconds)
            )

            async def _reservation_reaper_loop() -> None:
                while True:
                    await _reap_reservations_once(_flatten_contexts(contexts_by_leader))
                    await asyncio.sleep(_reaper_interval)

            supervised.append(
                _Supervised(name="reservation-reaper", factory=_reservation_reaper_loop)
            )

        supervised.append(
            _Supervised(
                name="account-lease-renewal",
                factory=lambda: account_lease_renewal_loop(account_lease_manager),
            )
        )

        from pmcopy.orchestrators.liveness_heartbeat import (
            DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
            bot_heartbeat_loop,
        )

        def _heartbeat_snapshot() -> dict[str, object]:
            """Read live supervisor state; never mutates and never blocks on I/O."""
            detection = heartbeat_state.get("detection")
            ready, _missing = application.readiness.is_ready(strict=False)
            readiness = application.readiness.snapshot()
            return {
                "detection_connected": (
                    bool(detection.connected) if detection is not None else None
                ),
                "active_leaders": (
                    len(detection.active_wallets()) if detection is not None else None
                ),
                "monitored_leaders": len(monitored_leaders),
                "ready": ready,
                "wss_user_ok": readiness.get("wss_user_ok"),
                "wss_market_ok": readiness.get("wss_market_ok"),
                "reconciler_ok": readiness.get("reconciler_ok"),
                "db_ok": readiness.get("db_ok"),
                "api_ok": readiness.get("api_ok"),
            }

        supervised.append(
            _Supervised(
                name="bot-heartbeat",
                factory=lambda: bot_heartbeat_loop(
                    _heartbeat_snapshot,
                    logger=logger,
                    interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                ),
            )
        )

        supervisor = TaskSupervisor(
            supervised=supervised,
            monitor_tasks=monitor_tasks,
            monitor_wallet_by_task=monitor_wallet_by_task,
            readiness=application.readiness,
            refresh_interval=float(refresh_interval),
            metrics_registry=metrics,
            logger=logger,
        )
        supervisor.spawn_all()

        from pmcopy.orchestrators.alchemy_detection_runtime import (
            AlchemyDetectionRuntime,
        )
        from pmcopy.orchestrators.onchain_signal_dispatch import OnchainSignalDispatchAdapter
        from pmcopy.orchestrators.trade_pipeline import execute_trades as _execute_trade_pipeline

        def _alchemy_source_changed() -> None:
            active_owners = onchain_runtime.active_wallets() if onchain_runtime is not None else ()
            application.readiness.update(monitored_leader_count=len(active_owners))
            telemetry_emitters.detection.roster_updated(len(active_owners))

        _alchemy_source_changed()
        try:
            _onchain_signal_sink = OnchainSignalDispatchAdapter(
                contexts_by_leader=contexts_by_leader,
                v1_runtime=v1_runtime,
                observer=paper_evaluation.offer if paper_evaluation is not None else None,
                execute_trades=_execute_trade_pipeline,
                telemetry=telemetry_emitters.detection,
                sell_fraction_resolver=_build_leader_sell_fraction_resolver(
                    application.config,
                    buy_scope_guard=leader_sell_scope_guard,
                    provider_runtime=provider_runtime,
                ),
            )
            onchain_runtime = AlchemyDetectionRuntime.compose(
                config=application.config,
                wallets=monitored_leaders,
                sink=_onchain_signal_sink,
                status_changed=_alchemy_source_changed,
            )
            await onchain_runtime.start()
            heartbeat_state["detection"] = onchain_runtime
            telemetry_emitters.detection.attach_connection_probe(
                _detection_connection_probe(onchain_runtime)
            )
        except Exception as exc:
            logger.error(
                "alchemy_detection_paused | reason=composition_unavailable error=%s",
                redact_error_detail(exc),
            )
            if onchain_runtime is not None:
                await onchain_runtime.stop()
            onchain_runtime = None
            _alchemy_source_changed()

        return await supervisor.run_until_stopped()

    finally:
        await _shutdown_runtime(
            monitor_tasks=monitor_tasks,
            supervised=supervised,
            market_data_runtimes=market_data_runtimes,
            user_data_runtimes=user_data_runtimes,
            metrics_runner=metrics_runner,
            onchain_runtime=onchain_runtime,
            pending_copy_tracker=pending_copy_tracker,
            v1_runtime=v1_runtime,
            evidence_drain=evidence_drain,
            account_lease_manager=account_lease_manager,
        )


async def _close_polymarket_clients(clients: list[Any]) -> None:
    """Close each distinct SDK owner without masking the shutdown reason."""
    await close_polymarket_clients(clients, logger=logger)


async def main(*, auto_trader: bool = False, copier: str | None = None) -> int:
    """Run the application while owning every created Polymarket client.

    Args:
        auto_trader: Use durable automatic membership and degradation-driven rotation.
        copier: Optional copier id selector (``--copier``); limits routing and
            runtime ownership to that single configured copier.

    Returns:
        Process exit code.
    """
    clients: list[Any] = []
    try:
        return await _run_application(
            clients,
            startup_policy=TraderStartupPolicy(auto_trader=auto_trader),
            copier=copier,
        )
    finally:
        await _close_polymarket_clients(clients)
        # Startup can return before ``_shutdown_runtime`` exists (doctor,
        # validation, or compliance failures). Always close the process-wide
        # HTTP pool here too; ``close_session`` is intentionally idempotent.
        try:
            from pmcopy.utils.fetchData import close_session

            await close_session()
        except Exception as exc:
            logger.warning(
                "shared_http_client_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )
        try:
            from pmcopy.storage.postgres.connection import shutdown_pool

            await shutdown_pool()
        except Exception as exc:
            logger.warning(
                "db_pool_shutdown_failed",
                extra={"error": redact_error_detail(exc)},
            )


if __name__ == "__main__":
    from pmcopy.utils.application_runner import ApplicationRunner
    from pmcopy.utils.event_loop import new_event_loop

    import sys

    exit_code = 0
    try:
        # main() returns 1 on a fatal/fail-closed startup or a supervisor
        # exhaustion; orderly SIGTERM/SIGINT shutdown cancels the task and
        # exits 0.
        exit_code = ApplicationRunner(loop_factory=new_event_loop).run(main()) or 0
    except Exception as exc:
        import traceback

        print_error(f"Fatal startup error: {exc}")
        traceback.print_exc()
        exit_code = 1
    sys.exit(exit_code)
