"""Application dependency builders, stream starters, and graceful shutdown helpers."""

from __future__ import annotations

import asyncio
import functools
import sys
from collections.abc import Awaitable, Callable, Iterable, Mapping
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pmcopy.application.trading_context import TradingContext
    from pmcopy.interfaces.source_time import BlockTimestampReader
    from pmcopy.interfaces.source_transactions import SourceTransactionStateStore
    from pmcopy.orchestrators.detection.detection_source_factory import (
        ConfirmedSourceBuilder,
        DetectionSourceRuntime,
    )
    from pmcopy.services.onchain_detection.leader_sell_fraction_resolver import (
        LeaderSellFractionResolver,
    )
    from pmcopy.services.onchain_detection.leader_sell_scope_guard import (
        LeaderSellBuyScopeGuard,
    )

from pmcopy.adapters.time.clock import SystemReceiptClock
from pmcopy.config.app_config import AppConfig, EnvValidationError
from pmcopy.domain.detection import DetectionMode
from pmcopy.orchestrators.bootstrap.runtime_bootstrap import (
    UserRuntimeState,
    build_trading_context,
)
from pmcopy.orchestrators.bootstrap.telemetry_bootstrap import TelemetryRuntime
from pmcopy.orchestrators.discovery.auto_trader_runtime import AutoTraderRuntime
from pmcopy.orchestrators.discovery.leader_registry import LeaderRegistry
from pmcopy.orchestrators.discovery.runtime_discovery import (
    discovery_refresh_loop as _discovery_refresh_loop_impl,
    wait_for_active_leaders as _wait_for_active_leaders_impl,
)
from pmcopy.orchestrators.lifecycle.app_lifecycle import (
    RuntimeShutdownCoordinator,
    RuntimeStreamCoordinator,
    SupervisedTaskSpec as _Supervised,
    user_ws_api_creds as _user_ws_api_creds,
)
from pmcopy.orchestrators.lifecycle.trader_startup_policy import TraderStartupPolicy
from pmcopy.orchestrators.streaming.market_data_runtime import MarketDataRuntime
from pmcopy.orchestrators.streaming.user_data_runtime import UserDataRuntime
from pmcopy.services.accounts.account_lease import AccountLeaseManager
from pmcopy.services.accounts.multi_user_config import RuntimeUser
from pmcopy.services.observability.metrics import MetricsRegistry, metrics
from pmcopy.services.telemetry import TelemetryEmitters, WssTelemetryEmitter
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.storage.postgres.leaders_repo import LeadersRepo
from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("Main")


def _app_facade_attr(name: str, default: Any) -> Any:
    facade = sys.modules.get("pmcopy.app")
    return getattr(facade, name, default) if facade is not None else default


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

    from pmcopy.clients.ws.onchain_ws import OnchainWebSocket

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
    from pmcopy.utils.fetch_data import _get_http_client

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
    from pmcopy.orchestrators.detection.detection_source_factory import ConfirmedSourceBuilder

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

    from pmcopy.clients.ws.onchain_ws import OnchainWebSocket
    from pmcopy.config.addresses import (
        POLYMARKET_CTF_EXCHANGE_V2_ADDRESS,
        POLYMARKET_NEG_RISK_EXCHANGE_V2_ADDRESS,
    )
    from pmcopy.orchestrators.detection.detection_source_factory import (
        DetectionSourceFactory,
        LogsOnlyConfirmedSourceBuilder,
        ProvisionalPendingSourceBuilder,
    )

    oc_cfg = getattr(getattr(config, "detection", None), "onchain", None)
    from pmcopy.adapters.polygon.block_timestamp import RpcBlockTimestampReaderFactory
    from pmcopy.utils.fetch_data import _get_http_client

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
        market_runtime_cls=_app_facade_attr("MarketDataRuntime", MarketDataRuntime),
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
        api_creds_resolver=_app_facade_attr("_user_ws_api_creds", _user_ws_api_creds),
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
        from pmcopy.utils.fetch_data import close_session

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


