"""Compose per-user trading dependencies behind venue-neutral ports."""

from __future__ import annotations

from functools import partial
from services.telemetry.latency import PipelineLatency

import asyncio
import inspect
from dataclasses import dataclass, field
from datetime import timedelta
from urllib.parse import urlencode
from decimal import Decimal, InvalidOperation
from collections.abc import Mapping, Sized
from typing import TYPE_CHECKING, Any

from application.trading_context import TradingContext
from interfaces.clob_gateway import ClobGateway
from cache.market_state_store import MarketStateStore
from services.accountingService import AccountingService
from services.compliance import GeoblockGate
from services.executionService import ExecutionService
from services.execution.coordinator import CopyAdmission
from services.execution.reservation_expiry import (
    OrderRowLookup,
    ReservationExpiryDecision,
    classify_no_id_reservation_expiry,
)
from services.feeService import FeeService
from services.market_data_service import MarketDataService, MarketDataPolicy
from services.multiUserConfig import RuntimeUser, UserFollowConfig
from services.pnlTracker import PnLTracker
from services.leader_performance import LeaderPerformanceReader
from storage.postgres.leader_performance_repository import PostgresLeaderPerformanceReader
from services.positionManager import PositionManager
from services.positionCoordinator import PositionCoordinator
from services.risk.risk_engine import RiskEngine, init_risk_engine
from services.healthServer import ReadinessTracker
from services.entryPermissionService import EntryPermissionService
from services.leaderScoringService import LeaderScoringService
from services.risk.risk_monitor import (
    reconcile_unconfirmed_risk_orders,
    rehydrate_position_manager,
)
from services.trading_permission import TradingPermissionService
from services.telemetry import TelemetryEmitters
from storage.postgres.fills_repo import FillsRepo
from storage.postgres.copy_task_repository import PostgresCopyTaskRepository
from services.copy_tasks.entry_permission import TaskEntryPermissionService
from storage.postgres.task_fill_projection import PostgresTaskFillProjection
from storage.postgres.risk_lot_repository import PostgresRiskLotRepository
from storage.postgres.orders_repo import OrdersRepo
from storage.postgres.positions_repo import PositionsRepo
from storage.postgres.risk_blocks_repo import RiskBlocksRepo
from storage.postgres.risk_triggers_repo import RiskTriggersRepo
from storage.postgres.trading_control_repo import TradingControlRepo
from utils.fetchData import _get_http_client, fetch_data
from integrations.polymarket.data_api_positions import DataApiPositionSizeReader
from orchestrators.account_inventory_startup import InventoryRuntimeRefresh
from collections.abc import Awaitable, Callable
from utils.logger import get_logger

if TYPE_CHECKING:
    from storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection
    from services.manual_sync_service import ManualSyncService
    from services.execution.ambiguous_recovery import AmbiguousSubmissionRecoveryService
    from config.app_config import AppConfig
    from integrations.polymarket.ccxt_prediction_client import CcxtPredictionClient
    from integrations.polymarket.venue_runtime import PolymarketVenueRuntime

logger = get_logger("RuntimeBootstrap")


async def _release_expired_reservations_for_context(context: TradingContext) -> int:
    extra = getattr(context, "extra", {}) or {}
    reservation_service = extra.get("reservation_service")
    if reservation_service is None:
        coordinator = getattr(context, "execution_coordinator", None)
        reservation_service = getattr(coordinator, "reservation_service", None)
    release_expired = getattr(reservation_service, "release_expired_active", None)
    if getattr(reservation_service, "enabled", True) is False:
        return 0
    if not callable(release_expired):
        return 0
    release_result = release_expired()
    released_ids = await release_result if inspect.isawaitable(release_result) else release_result
    return len(released_ids) if isinstance(released_ids, Sized) else 0


async def startup_reconcile_unconfirmed(
    contexts: list[TradingContext],
    *,
    failures_out: list[str] | None = None,
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

    Returns a dict mapping user_id → number of orders reconciled. Users
    whose per-user reconcile raised an exception are NOT in the result.
    Callers must inspect ``failures_out`` (when supplied) to detect
    partial failure and refuse to advertise the bot as ready.

    Parameters
    ----------
    contexts:
        Per-leader TradingContext list. Per-user uniqueness is enforced
        internally so duplicate contexts for the same user run once.
    failures_out:
        Optional list the function appends each failed ``user_id`` to.
        Pre-fix, exceptions were swallowed at WARNING with no observable
        side effect and the bot proceeded to advertise readiness on a
        stale position snapshot. Pass an empty list and refuse to set
        ``reconciler_ok`` when it ends non-empty.
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
            trade_n = await reconcile_submitted_unconfirmed(context, startup_recovery=True)
            risk_n = await reconcile_unconfirmed_risk_orders(context, startup_recovery=True)
            reservation_n = await _release_expired_reservations_for_context(context)
            total_n = trade_n + risk_n + reservation_n
            if total_n > 0:
                # Position state changed — re-rehydrate so the risk monitor
                # and trade loops observe the converged snapshot.
                await rehydrate_position_manager(context)
                reconciled_by_user[context.user_id] = total_n
                logger.info(
                    "startup_reconcile_complete | user=%s reconciled=%d "
                    "(trade=%d risk=%d reservations=%d)",
                    context.user_id,
                    total_n,
                    trade_n,
                    risk_n,
                    reservation_n,
                )
        except Exception as exc:
            logger.warning(
                "startup_reconcile_error | user=%s error=%s",
                context.user_id,
                repr(exc),
            )
            if failures_out is not None:
                failures_out.append(context.user_id)

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
    """Hold the composed runtime state for one follower account."""

    user: RuntimeUser
    client: "CcxtPredictionClient"
    orders_repo: OrdersRepo
    fills_repo: FillsRepo
    positions_repo: PositionsRepo
    risk_blocks_repo: RiskBlocksRepo
    pnl_tracker: PnLTracker
    risk_engine: RiskEngine
    accounting_service: AccountingService
    position_manager: PositionManager
    execution_service: ExecutionService
    # CLOB V2 gateway — single instance per user runtime, reused by the
    # reconciler / execution coordinator when those wires land (see issue #66
    # / #71). Optional default keeps existing test fixtures that build state
    # without a venue boundary working.
    clob_gateway: ClobGateway | None = None
    risk_triggers_repo: RiskTriggersRepo | None = None
    # Freshness-aware market data — issue #hardening PR-1.
    market_data_service: MarketDataService | None = None
    market_store: MarketStateStore | None = None
    # PR6 — production execution funnel.
    execution_coordinator: "object | None" = None
    # PR7 — manual / off-bot position drift sweep.
    position_drift_service: "object | None" = None
    # Manual sync service — funder activity + position seeder (TS port).
    manual_sync_service: ManualSyncService | None = None
    inventory_refresh: InventoryRuntimeRefresh | None = None
    # Cached durable control state; refreshed off the order hot path.
    trading_permission: TradingPermissionService | None = None
    # Part C — operator telemetry emitters shared by every collaborator this
    # follower owns. Defaults to the disabled null-object bundle so existing
    # fixtures keep constructing state without telemetry.
    telemetry: TelemetryEmitters = field(default_factory=TelemetryEmitters.disabled)
    leader_performance: LeaderPerformanceReader | None = None
    # Bounded, read-only recovery for no-order-id lost-POST rows. Optional so
    # paper / wallet-less test contexts keep constructing state without it.
    ambiguous_recovery: "AmbiguousSubmissionRecoveryService | None" = None
    # A04 — the ONE account-owned coordination state (per-scope lock map +
    # intent registry) shared by every per-leader TradingContext, the risk
    # context, and the ExecutionCoordinator for this follower account.
    # Constructed in ``build_user_state``; per-binding contexts must never
    # mint their own instance (see ``build_trading_context``).
    position_coordinator: PositionCoordinator = field(default_factory=PositionCoordinator)
    # Guard for the once-per-account intent-recovery seeding in
    # ``build_trading_context``: intents from every leader binding land in the
    # ONE account-owned registry, seeded exactly once per process.
    intents_recovered: bool = False


def configured_leaders(config: AppConfig) -> list[str]:
    """Return the configured leader wallet set."""
    leaders = list(config.copy_trading.enabled_wallets())
    if not leaders:
        if config.discovery.scheduler.start_from_roster:
            logger.info(
                "No manual leader wallets configured. Startup will use the "
                "hysteresis-qualified leader roster."
            )
        elif config.discovery.scheduler.enabled:
            logger.info(
                "No manual leader wallets configured. Discovery will refresh "
                "the watchlist, but roster startup is disabled."
            )
        else:
            logger.warning(
                "No leader wallets configured. Both config/traders/*.yml and "
                "the per-user follow list are empty. Add a trader file or run "
                "python bot.py rotate-traders after discovery has candidates."
            )
    return leaders


def default_follow(user_id: str, leader_id: str, config: AppConfig) -> UserFollowConfig:
    """Build the default follower-to-leader binding."""
    return UserFollowConfig(
        user_id=user_id,
        leader_id=leader_id,
        enabled=True,
        allocation_pct=config.sizing.percentage / 100.0,
        max_trade_usdc=config.sizing.buy_max_amount,
        max_market_exposure_usdc=config.risk.max_market_exposure_usdc,
    )


def ensure_runtime_follows(
    runtime_users: list[RuntimeUser], leaders: list[str], config: AppConfig
) -> None:
    """Apply follow whitelist semantics with the legacy single-account default.

    Per-copier ``follows`` are explicit whitelists and the source of truth: a
    leader with no follow entry is NOT followed. There is no implicit union
    of the global leader set into every copier.

    LEGACY MIGRATION (documented in ``docs/02-configuration.md``): when
    exactly ONE user account exists AND its follows did NOT come from
    explicit operator configuration (``RuntimeUser.follows_explicit`` is
    False — the config-less single-account fallback), the historical default
    applies at EVERY call, not just startup: that lone account auto-follows
    every leader it is shown, including newly discovered/promoted wallets at
    refresh time, exactly like the pre-whitelist behavior. Entries are added
    only when missing, so repeated refresh calls stay idempotent and never
    overwrite an existing binding. In every other case — multiple accounts,
    or any user loaded from a multi-user configuration (explicit follow
    entries, even all disabled, or an explicitly empty follows list) —
    absence of a follow entry means NOT following.
    """
    if len(runtime_users) != 1:
        return
    runtime_user = runtime_users[0]
    # Duck-typed tolerance for foreign/test states that predate the flag:
    # absence of the attribute carries no evidence of explicit follows
    # configuration, so the legacy default stays available (same posture as
    # the ``build_trading_context`` state fallback).
    if getattr(runtime_user, "follows_explicit", False):
        return
    for leader in leaders:
        if leader not in runtime_user.follows:
            runtime_user.follows[leader] = default_follow(runtime_user.user_id, leader, config)


async def fetch_live_positions(wallet: str) -> list[dict[str, object]] | None:
    """Read a bounded complete inventory without equating zero marks to disposal."""
    return await DataApiPositionSizeReader(_get_http_client).positions(wallet)


def build_user_state(
    runtime_user: RuntimeUser,
    client: "CcxtPredictionClient",
    pool,
    config: AppConfig,
    clob_gateway: "ClobGateway | None" = None,
    telemetry: TelemetryEmitters | None = None,
    copy_admission: CopyAdmission | None = None,
    provider_runtime: Any = None,
    evidence_projection: PostgresFillEvidenceProjection | None = None,
) -> UserRuntimeState:
    """Build one follower runtime from injected configuration and ports.

    Args:
        runtime_user: Follower runtime identity and wallet settings.
        client: CCXT prediction client owned by the venue runtime.
        pool: Database pool backing every repository.
        config: Validated application configuration.
        clob_gateway: The single CCXT venue boundary for this follower.
        telemetry: Operator telemetry emitters; a disabled bundle is used when
            the telemetry runtime is not composed.

    Returns:
        The composed runtime state for one follower account.
    """
    init_risk_engine(config)
    emitters = telemetry if telemetry is not None else TelemetryEmitters.disabled()

    # Same pattern for the balance-lookup module (RPC_URL + collateral
    # contract addresses) so utils/getMyBalance.py drops its ENV reads.
    from utils.getMyBalance import configure_from_appconfig as _configure_balance_module

    _configure_balance_module(config)

    order_repo = OrdersRepo(pool)
    fills_repo = FillsRepo(
        pool,
        task_projection=PostgresTaskFillProjection(PostgresRiskLotRepository(pool)),
        evidence_projection=evidence_projection,
    )
    positions_repo = PositionsRepo(pool)
    risk_blocks_repo = RiskBlocksRepo(pool)
    risk_triggers_repo = RiskTriggersRepo(pool)
    pnl_tracker = PnLTracker(log_path=f"log/pnl_{runtime_user.user_id}.jsonl")
    risk_engine = RiskEngine()
    accounting_service = AccountingService()
    position_manager = PositionManager(config=config)
    # Single CLOB venue boundary shared by execution, reconciliation, and
    # market data. The CCXT adapter is the only transport; without an
    # injected gateway the caller has not built a venue runtime.
    if clob_gateway is None:
        raise RuntimeError(
            "build_user_state requires a CCXT venue gateway; "
            "build the venue runtime first via build_user_venue_runtime"
        )

    # Freshness-aware market data layer (issue #hardening PR-1).
    # The store is fed by WSS deltas; the MDS falls back to REST via the
    # gateway when cache is stale/missing. ExecutionService uses MDS so that
    # live execution never runs on stale data.
    market_store = MarketStateStore()
    md_cfg = getattr(config, "market_data", None)
    primary_source = str(getattr(md_cfg, "primary_source", "rest_snapshot")).lower()
    market_data_svc = MarketDataService(
        market_store,
        clob_gateway,
        policy=MarketDataPolicy(
            # In REST primary mode, copy-trade execution gets its quote/book
            # from a fresh REST `/book` snapshot fetched right before sizing.
            # The legacy WSS-fresh gate is intentionally relaxed; the snapshot
            # path enforces its own millisecond budgets and book-quality gates.
            primary_source=primary_source,
            require_wss_fresh=primary_source == "wss",
            allow_rest_fallback=False,
            allow_rest_snapshot_for_execution=primary_source == "rest_snapshot",
            allow_rest_bootstrap_for_execution=bool(
                getattr(md_cfg, "enable_rest_bootstrap", False)
            ),
            rest_bootstrap_max_age_ms=int(getattr(md_cfg, "rest_bootstrap_max_age_ms", 500)),
            rest_snapshot_max_age_ms=int(getattr(md_cfg, "rest_snapshot_max_age_ms", 500)),
            rest_snapshot_request_timeout_ms=int(
                getattr(md_cfg, "rest_snapshot_request_timeout_ms", 750)
            ),
            rest_snapshot_total_budget_ms=int(
                getattr(md_cfg, "rest_snapshot_total_budget_ms", 1000)
            ),
            rest_snapshot_max_spread_bps=int(getattr(md_cfg, "rest_snapshot_max_spread_bps", 300)),
            max_quote_age_seconds=float(getattr(md_cfg, "executable_quote_max_age_seconds", 2.0)),
            max_book_age_seconds=float(getattr(md_cfg, "executable_book_max_age_seconds", 2.0)),
            max_tick_age_seconds=float(getattr(md_cfg, "executable_tick_max_age_seconds", 300.0)),
            wss_connection_max_silence_seconds=float(
                getattr(md_cfg, "wss_connection_max_silence_seconds", 20.0)
            ),
        ),
    )

    trading_permission = TradingPermissionService(
        TradingControlRepo(pool),
        live_mode=not config.execution.paper_trade_only,
    )
    execution_service = ExecutionService(
        # ``clob_client`` is retained for backward-compatible construction but
        # is never read by ExecutionService or FeeService; the venue boundary
        # is ``clob_gateway``. Pass the gateway so any legacy duck-typed read
        # still resolves against the CCXT adapter.
        clob_client=clob_gateway,
        fee_service=FeeService(clob_gateway),
        order_repo=order_repo,
        config=config,
        clob_gateway=clob_gateway,
        market_data_service=market_data_svc,
        # P1-j: trade-path geoblock enforcement with expiry revalidation. Opt-in
        # via ENFORCE_GEOBLOCK (default on); fail-closed; BUY entries only.
        geoblock_gate=GeoblockGate(config.polymarket.enforce_geoblock),
        trading_permission=trading_permission,
        telemetry=emitters.execution,
        latency=emitters.latency,
        task_entry_permission=TaskEntryPermissionService(
            PostgresCopyTaskRepository(pool),
            follower_accounts=(runtime_user.user_id, runtime_user.wallet.funder_address or ""),
            protective_management_enabled=config.risk.enable_risk_mgmt,
        ),
    )

    # A04 — exactly ONE PositionCoordinator (per-scope lock map + intent
    # registry) per follower account. Owned by the account runtime here,
    # injected into the shared ExecutionCoordinator at construction and into
    # every per-leader TradingContext in ``build_trading_context`` so copy
    # entries, protective exits, and reconciliation serialize on one
    # mutual-exclusion domain. Never rebuilt or re-pointed per binding.
    position_coordinator = PositionCoordinator()

    # PR6 — production execution funnel. ReservationService + ExecutionCoordinator
    # are wired per user so trade pipeline and risk monitor share reservation
    # and collision scope.
    execution_coordinator = _build_execution_coordinator(
        pool=pool,
        positions_repo=positions_repo,
        order_repo=order_repo,
        fills_repo=fills_repo,
        execution_service=execution_service,
        funder_address=runtime_user.wallet.funder_address,
        config=config,
        provider_runtime=provider_runtime,
        latency=emitters.latency,
        copy_admission=copy_admission,
        position_coordinator=position_coordinator,
    )

    reservation_service = getattr(execution_coordinator, "reservation_service", None)
    reservations_repo = getattr(reservation_service, "reservations_repo", None)

    # PR7 — proactive position drift sweep. Built BEFORE manual_sync so the
    # handshake (manual_sync.was_recently_pruned → skip upsert) is wired
    # from boot. Without this ordering manual_sync would resurrect rows
    # drift authoritatively pruned, looping risk-monitor /price probes.
    position_drift_service = _build_position_drift_service(
        pool=pool,
        positions_repo=positions_repo,
        reservations_repo=reservations_repo,
        config=config,
    )

    # Manual sync service — detects off-bot trades on the funder wallet and
    # seeds positions table from venue on startup. Optional so paper-only
    # contexts that lack a funder address skip the service cleanly.
    inventory_refresh = InventoryRuntimeRefresh(
        user_id=runtime_user.user_id,
        wallet=runtime_user.wallet.funder_address,
        positions=positions_repo,
        manager=position_manager,
        drift=position_drift_service,
    )
    manual_sync_service = _build_manual_sync_service(
        funder_address=runtime_user.wallet.funder_address,
        user_id=runtime_user.user_id,
        positions_repo=positions_repo,
        market_store=market_store,
        fills_repo=fills_repo,
        orders_repo=order_repo,
        position_drift_service=position_drift_service,
        reservation_service=reservation_service,
        on_positions_synced=inventory_refresh.refresh,
    )

    ambiguous_recovery = _build_ambiguous_recovery_service(
        user_id=runtime_user.user_id,
        funder_address=runtime_user.wallet.funder_address,
        gateway=clob_gateway,
        orders_repo=order_repo,
        positions_repo=positions_repo,
        pool=pool,
        config=config,
        provider_runtime=provider_runtime,
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
        clob_gateway=clob_gateway,
        risk_triggers_repo=risk_triggers_repo,
        market_data_service=market_data_svc,
        market_store=market_store,
        execution_coordinator=execution_coordinator,
        position_drift_service=position_drift_service,
        manual_sync_service=manual_sync_service,
        inventory_refresh=inventory_refresh,
        trading_permission=trading_permission,
        telemetry=emitters,
        leader_performance=PostgresLeaderPerformanceReader(pool),
        ambiguous_recovery=ambiguous_recovery,
        position_coordinator=position_coordinator,
    )


async def build_user_venue_runtime(
    runtime_user: RuntimeUser,
    config: AppConfig,
    *,
    exchange_factory: object | None = None,
    ccxt_version: str | None = None,
) -> "PolymarketVenueRuntime":
    """Build the single CCXT-backed venue runtime for one follower wallet.

    Constructs the CCXT exchange directly — no SDK object, no fallback, no
    selector — and fails closed when the transport is unavailable or live
    mode lacks live-execution compatibility.

    Args:
        runtime_user: Follower runtime identity and wallet settings.
        config: Validated application configuration.
        exchange_factory: Optional deterministic CCXT factory test seam.
        ccxt_version: Optional installed-version override for tests.

    Returns:
        The venue runtime owning the CCXT client and its gateway.

    Raises:
        TypeError: If ``exchange_factory`` is not callable.
        CcxtPredictionUnavailable: When CCXT is missing or fails the gate.
    """

    from integrations.polymarket.venue_runtime import build_polymarket_runtime

    if exchange_factory is not None and not callable(exchange_factory):
        raise TypeError("exchange_factory must be callable")
    return await build_polymarket_runtime(
        runtime_user=runtime_user,
        config=config,
        exchange_factory=exchange_factory,  # type: ignore[arg-type]
        ccxt_version=ccxt_version,
    )


def _build_manual_sync_service(
    *,
    funder_address: str,
    user_id: str,
    positions_repo: PositionsRepo,
    market_store: MarketStateStore | None,
    fills_repo: FillsRepo | None,
    orders_repo: OrdersRepo | None = None,
    position_drift_service=None,
    reservation_service=None,
    on_positions_synced: Callable[[], Awaitable[None]] | None = None,
):
    """Build the per-user ManualSyncService. Returns None when prerequisites are missing."""
    if not funder_address or market_store is None:
        return None
    try:
        from services.manual_sync_service import ManualSyncService
    except Exception as exc:
        logger.warning("manual_sync_service_unwired error=%r", exc)
        return None
    return ManualSyncService(
        funder_address=funder_address,
        user_id=user_id,
        positions_fetcher=fetch_live_positions,
        positions_repo=positions_repo,
        market_store=market_store,
        fills_repo=fills_repo,
        orders_repo=orders_repo,
        position_drift_service=position_drift_service,
        reservation_service=reservation_service,
        on_positions_synced=on_positions_synced,
    )


def _build_ambiguous_recovery_service(
    *,
    user_id: str,
    funder_address: str | None,
    gateway: ClobGateway | None,
    orders_repo: OrdersRepo,
    positions_repo: PositionsRepo,
    pool,
    config: AppConfig,
    provider_runtime: Any = None,
) -> "AmbiguousSubmissionRecoveryService | None":
    """Build per-user no-ID ambiguous recovery with read-only venue evidence.

    Returns None when the wallet or venue gateway is missing, when recovery is
    disabled, or when any collaborator cannot be constructed. A missing service
    only means the lost-POST rows stay duplicate-blocking (the historical
    behaviour); it never weakens a block.
    """
    if not funder_address or gateway is None:
        return None
    recovery_cfg = getattr(getattr(config, "reconciliation", None), "ambiguous_recovery", None)
    if recovery_cfg is None or not bool(getattr(recovery_cfg, "enabled", True)):
        return None
    try:
        from integrations.polymarket.ccxt_ambiguous_evidence import (
            CcxtAmbiguousEvidenceSource,
        )
        from integrations.polymarket.data_api_positions import DataApiPositionSizeReader
        from services.execution.ambiguous_recovery import AmbiguousSubmissionRecoveryService
        from storage.postgres.reservations_repo import ReservationsRepo
        from utils.getMyBalance import get_my_balance
    except Exception as exc:
        logger.warning("ambiguous_recovery_unwired error=%r", exc)
        return None

    reservations_repo = ReservationsRepo(pool)

    async def _local_shares(uid: str, condition_id: str, token_id: str) -> Decimal | None:
        """Persisted (confirmed-only) shares; None means unknown evidence."""
        try:
            record = await positions_repo.get_position(uid, condition_id, token_id=token_id)
        except Exception as exc:
            logger.warning(
                "ambiguous_recovery_local_position_unavailable | user=%s error_type=%s",
                uid,
                type(exc).__name__,
            )
            return None
        if record is None:
            return Decimal("0")
        try:
            return Decimal(str(getattr(record, "size_tokens", 0) or 0))
        except (InvalidOperation, TypeError, ValueError):
            return None

    async def _reserved_quote(uid: str) -> Decimal | None:
        """Active BUY quote reservation floor; None means unknown evidence."""
        try:
            total = await reservations_repo.sum_active_quote_for_user(uid)
        except Exception as exc:
            logger.warning(
                "ambiguous_recovery_reserved_quote_unavailable | user=%s error_type=%s",
                uid,
                type(exc).__name__,
            )
            return None
        try:
            return Decimal(str(total or 0))
        except (InvalidOperation, TypeError, ValueError):
            return None

    async def _balance(address: str) -> Decimal | None:
        return await get_my_balance(address, provider_runtime=provider_runtime)

    wallet_by_user = {user_id: funder_address}

    evidence_source = CcxtAmbiguousEvidenceSource(
        private_history=gateway,
        shares_reader=DataApiPositionSizeReader(_get_http_client),
        balance_reader=_balance,
        local_shares_reader=_local_shares,
        reserved_quote_reader=_reserved_quote,
        wallet_for_user=wallet_by_user.get,
        read_timeout_seconds=float(getattr(recovery_cfg, "read_timeout_seconds", 10.0) or 10.0),
        match_window=timedelta(
            seconds=float(getattr(recovery_cfg, "match_window_seconds", 900.0) or 0.0)
        ),
    )
    return AmbiguousSubmissionRecoveryService(
        repository=orders_repo,
        evidence_source=evidence_source,
        # Audit A01: the service is account-scoped end to end.  Its batch,
        # scans, attaches, and absence proofs can only ever touch this
        # user's rows; any other account's submission fails closed.
        account_user_id=user_id,
        account_wallet=funder_address,
        grace_period=timedelta(seconds=float(getattr(recovery_cfg, "grace_seconds", 120.0) or 0.0)),
        required_absence_scans=int(getattr(recovery_cfg, "required_absence_scans", 3) or 3),
        batch_size=int(getattr(recovery_cfg, "batch_size", 100) or 100),
        evidence_history_limit=int(getattr(recovery_cfg, "evidence_history_limit", 8) or 8),
    )


async def ambiguous_recovery_loop(
    services: Mapping[str, Any],
    *,
    interval_seconds: float,
) -> None:
    """Run the bounded no-ID recovery sweep for every wired follower.

    A failing scan fails closed: the exception is logged and the row keeps
    blocking, while the loop itself survives so the next tick can retry.
    """
    interval = max(1.0, float(interval_seconds))
    while True:
        for user_id, service in services.items():
            recover_once = getattr(service, "recover_once", None)
            if not callable(recover_once):
                continue
            try:
                results = await recover_once()
            except Exception as exc:
                logger.warning(
                    "ambiguous_recovery_scan_failed | user=%s error_type=%s",
                    user_id,
                    type(exc).__name__,
                )
                continue
            _log_ambiguous_recovery_results(user_id, results)
        await asyncio.sleep(interval)


def _log_ambiguous_recovery_results(user_id: str, results: Any) -> None:
    """Emit one operator-visible line per resolved or progressing row."""
    try:
        entries = tuple(results or ())
    except TypeError:
        return
    resolved = []
    for result in entries:
        disposition = str(getattr(getattr(result, "disposition", None), "value", ""))
        if disposition in {"recovered_order_id", "confirmed_absent"}:
            resolved.append((getattr(result, "idempotency_key", "?"), disposition))
    if not resolved:
        return
    logger.info(
        "ambiguous_recovery_resolved | user=%s count=%d resolutions=%s",
        user_id,
        len(resolved),
        ";".join(f"{key}:{disposition}" for key, disposition in resolved),
    )


def _build_position_drift_service(
    *,
    pool,
    positions_repo: PositionsRepo,
    reservations_repo=None,
    config: AppConfig,
):
    """Build the per-user PositionDriftService (PR7).

    Returns None when the service cannot be constructed; runtime then runs
    without a drift sweep (paper / dry-run modes). In live mode the bootstrap
    must ensure this is wired.
    """
    try:
        from services.position_drift_service import PositionDriftService
    except Exception as exc:
        logger.warning("position_drift_service_unwired error=%r", exc)
        return None

    async def _fetcher(wallet: str):
        # Fail closed: a None snapshot means the fetch failed. Raise so the
        # drift service treats it as unresolved (it is designed to fail closed
        # on a raising fetcher) rather than as "venue holds nothing".
        result = await fetch_live_positions(wallet)
        if result is None:
            raise RuntimeError("live positions snapshot fetch failed")
        return result

    return PositionDriftService(
        positions_repo=positions_repo,
        reservations_repo=reservations_repo,
        live_positions_fetcher=_fetcher,
        config=config.reconciliation.position_drift,
    )


def _build_execution_coordinator(
    *,
    pool,
    positions_repo: PositionsRepo,
    order_repo,
    fills_repo: FillsRepo,
    execution_service: ExecutionService,
    funder_address: str,
    config: "AppConfig | None" = None,
    latency: PipelineLatency | None = None,
    copy_admission: CopyAdmission | None = None,
    provider_runtime: Any = None,
    position_coordinator: PositionCoordinator | None = None,
):
    """Build the production ExecutionCoordinator for a user.

    Wraps ReservationService around ExecutionService so every production
    submit path applies reservation + collision semantics. The account-owned
    ``position_coordinator`` (A04) is injected at construction so the
    execution funnel serializes on the same lock map as every per-binding
    TradingContext; there is no post-construction rewiring. Returns None when
    the funder address is missing so test contexts that build a partial state
    keep working.
    """
    if not funder_address:
        return None
    try:
        from services.execution.coordinator import ExecutionCoordinator
        from services.execution.reservation_service import ReservationService
        from storage.postgres.reservations_repo import ReservationsRepo
        from utils.getMyBalance import get_my_balance
    except Exception as exc:
        logger.warning("execution_coordinator_unwired error=%r", exc)
        return None
    reservations_repo = ReservationsRepo(pool)
    # Reservations are a typed-config gate (RESERVATIONS_ENABLED), not a
    # hard-coded mode. Configless contexts (partial test state) keep the
    # historical disabled behavior so existing fixtures stay green.
    reservations_enabled = config.execution.reservations_enabled if config is not None else False
    _reservation_ttl = config.execution.reservation_ttl_seconds if config is not None else 120
    reservation_service = ReservationService(
        reservations_repo=reservations_repo,
        positions_repo=positions_repo,
        get_balance_func=partial(get_my_balance, provider_runtime=provider_runtime)
        if provider_runtime is not None
        else get_my_balance,
        funder_address=funder_address,
        ttl_seconds=_reservation_ttl,
        enabled=reservations_enabled,
        task_market_fraction=Decimal(str(config.sizing.max_position_pct))
        if config is not None
        else None,
        task_max_open_markets=config.sizing.position_threshold if config is not None else 0,
    )
    if not reservations_enabled:
        logger.warning(
            "reservations DISABLED (RESERVATIONS_ENABLED=false): pre-submit "
            "BUY-quote/SELL-share double-spend reservations are OFF. "
            "Duplicate-order protection now relies solely on the idempotency "
            "submission lock + per-scope position coordinator. funder=%s",
            funder_address,
        )
    coord = ExecutionCoordinator(
        reservation_service=reservation_service,
        orders_repo=order_repo,
        fills_repo=fills_repo,
        positions_repo=positions_repo,
        execution_service=execution_service,
        position_coordinator=position_coordinator,
        latency=latency,
        copy_admission=copy_admission,
    )
    from services.execution.leader_sell_target_service import (
        LeaderSellTargetService,
        PersistedFollowerPositionReader,
    )
    from storage.postgres.leader_sell_target_repo import LeaderSellTargetRepo

    coord.leader_sell_target_service = LeaderSellTargetService(
        repository=LeaderSellTargetRepo(pool),
        position_reader=PersistedFollowerPositionReader(positions_repo),
        coordinator=coord,
    )
    return coord


def _bind_reservation_runtime(context: TradingContext, coord: object | None) -> None:
    """Expose the coordinator's reservation service to reconciliation paths.

    The coordinator owns the service instance. Reconciliation code discovers it
    through ``context.extra`` so terminal fills/cancels can release active
    reservations.
    """
    if coord is None:
        return

    reservation_service = getattr(coord, "reservation_service", None)
    if reservation_service is None:
        return

    reservations_repo = getattr(coord, "reservations_repo", None) or getattr(
        reservation_service, "reservations_repo", None
    )
    context.extra["reservation_service"] = reservation_service
    if reservations_repo is not None:
        context.extra["reservations_repo"] = reservations_repo
    target_service = getattr(coord, "leader_sell_target_service", None)
    if target_service is not None:
        context.extra["leader_sell_target_service"] = target_service

    def _reservation_attr(reservation: object, name: str) -> Any:
        return getattr(reservation, name, None)

    def _reservation_side(reservation: object, request: dict[str, Any] | None = None) -> str:
        request_side = str((request or {}).get("side") or "").upper()
        if request_side:
            return request_side
        raw_side = _reservation_attr(reservation, "side")
        return str(getattr(raw_side, "value", raw_side) or "").upper()

    def _synthetic_trade_record(
        *,
        reservation: object,
        order_row: dict[str, Any] | None,
        order_id: str,
    ) -> tuple[str, dict[str, Any]] | None:
        request = order_row.get("request") if isinstance(order_row, dict) else None
        request = request if isinstance(request, dict) else {}
        side = _reservation_side(reservation, request)
        if side not in {"BUY", "SELL"}:
            return None

        condition_id = request.get("condition_id") or _reservation_attr(reservation, "condition_id")
        token_id = (
            request.get("token_id")
            or request.get("asset")
            or _reservation_attr(reservation, "token_id")
        )
        user_id = (
            ((order_row or {}).get("user_id") if isinstance(order_row, dict) else None)
            or request.get("user_id")
            or _reservation_attr(reservation, "user_id")
            or context.user_id
        )
        leader_id = (
            ((order_row or {}).get("leader_id") if isinstance(order_row, dict) else None)
            or request.get("leader_id")
            or _reservation_attr(reservation, "leader_wallet")
        )
        trade_key = (
            request.get("signal_id")
            or ((order_row or {}).get("idempotency_key") if isinstance(order_row, dict) else None)
            or _reservation_attr(reservation, "intent_key")
            or order_id
        )
        if not condition_id or not token_id or not user_id or not trade_key:
            return None

        record = {
            "trade_key": str(trade_key),
            "user_id": str(user_id),
            "leader_id": leader_id,
            "tx_hash": str(request.get("signal_id") or trade_key),
            "asset": str(token_id),
            "condition_id": str(condition_id),
            "order_id": str(order_id),
            "reservation_id": str(_reservation_attr(reservation, "id") or ""),
            "request": request,
            "status": (order_row or {}).get("status") if isinstance(order_row, dict) else None,
        }
        return side, record

    async def _lookup_order_by_reservation(reservation_id: str) -> OrderRowLookup:
        order_lookup = getattr(context.orders_repo, "get_by_reservation_id", None)
        if not callable(order_lookup):
            return OrderRowLookup.unavailable(
                RuntimeError("orders repository cannot look up submissions by reservation id")
            )
        try:
            lookup_result = order_lookup(reservation_id)
            order_row = await lookup_result if inspect.isawaitable(lookup_result) else lookup_result
        except Exception as exc:
            return OrderRowLookup.unavailable(exc)
        return OrderRowLookup.from_lookup(order_row)

    async def _lookup_order_by_order_id(order_id: str | None) -> OrderRowLookup:
        if not order_id:
            return OrderRowLookup.absent()
        order_lookup = getattr(context.orders_repo, "get_by_order_id", None)
        if not callable(order_lookup):
            return OrderRowLookup.unavailable(
                RuntimeError("orders repository cannot look up submissions by order id")
            )
        try:
            lookup_result = order_lookup(order_id)
            order_row = await lookup_result if inspect.isawaitable(lookup_result) else lookup_result
        except Exception as exc:
            return OrderRowLookup.unavailable(exc)
        return OrderRowLookup.from_lookup(order_row)

    async def _reservation_is_released(reservation_id: str) -> bool:
        if reservations_repo is None:
            return False
        try:
            active = await reservations_repo.get_by_id(reservation_id)
        except Exception:
            return False
        return active is None or str(getattr(active, "status", "")).lower() != "active"

    async def _reconcile_linked_reservation_order(
        *,
        reservation: object,
        order_id: str,
        order_row: dict[str, Any] | None,
    ) -> bool:
        synthetic = _synthetic_trade_record(
            reservation=reservation,
            order_row=order_row,
            order_id=order_id,
        )
        if synthetic is None:
            return False
        side, record = synthetic
        try:
            from orchestrators.trade_pipeline import get_reconciliation_service

            reconciler = get_reconciliation_service(context)
            applied = await reconciler.reconcile_trade_record(
                record,
                side,
                confirm_fill=reconciler.confirm_fill,
                pnl_tracker=context.pnl_tracker,
                get_position=context.positions_repo.get_position,
                logger=logger,
                balance_regime=getattr(context, "balance_regime", None),
            )
            if applied:
                await rehydrate_position_manager(context)
            return applied
        except Exception as exc:
            logger.warning(
                "reservation_expiry_linked_order_reconcile_failed | user=%s "
                "reservation_id=%s order_id=%s error=%r",
                context.user_id,
                _reservation_attr(reservation, "id"),
                order_id,
                exc,
            )
            return False

    async def _reconcile_expired_reservation(
        reservation_id: str,
    ) -> ReservationExpiryDecision:
        """Classify one expired reservation before any capacity release.

        The decision tree is an explicit allow/deny: only authoritative
        terminal evidence for this account, or POSITIVE proof that no
        submission was ever persisted (pre-submit orphan — the write-ahead
        submission row is saved before any venue POST), may release reserved
        capacity. An unresolved no-ID submission (``submitted_unconfirmed``
        without a venue identity) stays reserved under the ambiguous-recovery
        machinery's ownership; TTL alone never releases it. A failed
        persistence lookup is an outage, not absence — the reservation is
        kept and the sweep retries later.
        """
        try:
            from orchestrators.trade_pipeline import reconcile_submitted_unconfirmed

            trade_n = await reconcile_submitted_unconfirmed(context, max_age_seconds=0)
            risk_n = await reconcile_unconfirmed_risk_orders(context)
            if trade_n or risk_n:
                await rehydrate_position_manager(context)
        except Exception as exc:
            logger.warning(
                "reservation_expiry_reconcile_failed | user=%s reservation_id=%s error=%r",
                context.user_id,
                reservation_id,
                exc,
            )
            return ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE

        if reservations_repo is None:
            logger.warning(
                "reservation_expiry_reservations_repo_unavailable | user=%s reservation_id=%s",
                context.user_id,
                reservation_id,
            )
            return ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE

        try:
            active = await reservations_repo.get_by_id(reservation_id)
        except Exception as exc:
            logger.warning(
                "reservation_expiry_lookup_failed | user=%s reservation_id=%s error=%r",
                context.user_id,
                reservation_id,
                exc,
            )
            return ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE

        if active is None or str(getattr(active, "status", "")).lower() != "active":
            return ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE

        linked_order = getattr(active, "order_id", None) or getattr(active, "venue_order_id", None)
        submission_lookup = await _lookup_order_by_reservation(reservation_id)
        candidate_order_id = str(
            linked_order or (submission_lookup.order_row or {}).get("order_id") or ""
        ).strip()
        if candidate_order_id:
            order_row = submission_lookup.order_row
            if order_row is None:
                # Best-effort by-order-id fetch; the synthetic record falls
                # back to the reservation's own attributes when unavailable.
                by_id_lookup = await _lookup_order_by_order_id(candidate_order_id)
                order_row = by_id_lookup.order_row
            await _reconcile_linked_reservation_order(
                reservation=active,
                order_id=candidate_order_id,
                order_row=order_row,
            )
            if await _reservation_is_released(reservation_id):
                return ReservationExpiryDecision.RELEASE_TERMINAL_EVIDENCE
            return ReservationExpiryDecision.KEEP_PENDING_RECOVERY

        decision = classify_no_id_reservation_expiry(submission_lookup)
        if decision is ReservationExpiryDecision.PERSISTENCE_UNAVAILABLE:
            logger.error(
                "reservation_expiry_persistence_unavailable | user=%s reservation_id=%s "
                "lookup_error=%r",
                context.user_id,
                reservation_id,
                submission_lookup.error,
            )
        elif decision is ReservationExpiryDecision.KEEP_PENDING_RECOVERY:
            logger.info(
                "reservation_expiry_keep_pending_recovery | user=%s reservation_id=%s "
                "row_status=%s idempotency_key=%s",
                context.user_id,
                reservation_id,
                (submission_lookup.order_row or {}).get("status"),
                (submission_lookup.order_row or {}).get("idempotency_key"),
            )
        else:
            logger.info(
                "reservation_expiry_pre_submit_orphan | user=%s reservation_id=%s "
                "— proven no persisted submission, releasing",
                context.user_id,
                reservation_id,
            )
        return decision

    reservation_service.reconciler_callback = _reconcile_expired_reservation


def _user_state_telemetry(user_state: object) -> TelemetryEmitters:
    """Return the follower's emitter bundle, or the disabled null object."""
    emitters = getattr(user_state, "telemetry", None)
    if isinstance(emitters, TelemetryEmitters):
        return emitters
    return TelemetryEmitters.disabled()


async def build_trading_context(
    user_state: UserRuntimeState,
    follow: UserFollowConfig,
    config: AppConfig,
    leaders_repo=None,
    readiness: ReadinessTracker | None = None,
) -> TradingContext:
    """Build the venue-neutral trading context for one follower binding."""
    if leaders_repo is not None and hasattr(leaders_repo, "for_account"):
        leaders_repo = leaders_repo.for_account(user_state.user.user_id)
    rank_multiplier = Decimal("1")
    roster_score: float | None = None
    if leaders_repo is not None and hasattr(leaders_repo, "get_roster"):
        roster_entry = await leaders_repo.get_roster(follow.leader_id)
        if roster_entry is not None:
            from services.order_sizing import longitudinal_sizing_multiplier

            longitudinal = config.discovery.longitudinal
            roster_score = float(roster_entry.score)
            rank_multiplier = longitudinal_sizing_multiplier(
                roster_entry.longitudinal_score
                if roster_entry.longitudinal_score is not None
                else roster_entry.score,
                minimum=longitudinal.sizing_multiplier_min,
                maximum=longitudinal.sizing_multiplier_max,
                exponent=longitudinal.sizing_multiplier_exponent,
            )
    leader_scorer = LeaderScoringService(
        config=config,
        roster_score=roster_score,
    )
    context = TradingContext(
        user_id=user_state.user.user_id,
        wallet_address=user_state.user.wallet.funder_address,
        # Risk monitor reads ``context.client.get_price(token_id, side)``; the
        # CCXT gateway exposes exactly that surface, so the venue boundary is
        # the single read authority. The raw CCXT client does not.
        client=user_state.clob_gateway,
        positions_repo=user_state.positions_repo,
        fills_repo=user_state.fills_repo,
        orders_repo=user_state.orders_repo,
        risk_blocks_repo=user_state.risk_blocks_repo,
        pnl_tracker=user_state.pnl_tracker,
        leader_performance=user_state.leader_performance,
        risk_engine=user_state.risk_engine,
        accounting_service=user_state.accounting_service,
        position_manager=user_state.position_manager,
        config=config,
        # A04 — the ONE account-owned PositionCoordinator from
        # ``build_user_state``. Leader bindings are immutable policy
        # references; they must never mint or re-point coordination state.
        # The fallback only serves foreign/test states constructed without
        # ``build_user_state`` (the TradingContext default factory exists for
        # direct test construction; production always passes this explicitly).
        position_coordinator=(
            user_state.position_coordinator
            if getattr(user_state, "position_coordinator", None) is not None
            else PositionCoordinator()
        ),
        entry_permission_service=EntryPermissionService(user_state.risk_blocks_repo),
        execution_service=user_state.execution_service,
        leaders_repo=leaders_repo,
        leader_scorer=leader_scorer,
        allocation_pct=follow.allocation_pct,
        buy_min_amount=config.sizing.buy_min_amount,
        buy_max_amount=min(config.sizing.buy_max_amount, follow.max_trade_usdc),
        max_market_exposure_usdc=follow.max_market_exposure_usdc,
        rank_size_multiplier=rank_multiplier,
        readiness=readiness or ReadinessTracker(),
        market_data_service=getattr(user_state, "market_data_service", None),
        market_quotes=getattr(user_state, "market_store", None),
        holdings_reader=DataApiPositionSizeReader(_get_http_client),
        telemetry=_user_state_telemetry(user_state),
        extra={
            "slippage_tolerance_bps": int(config.execution.slippage_tolerance * 100),
            "clob_gateway": user_state.clob_gateway,
            "leader_wallet": follow.leader_id,
            "accounted_fill_reader": user_state.fills_repo,
        },
    )
    if user_state.risk_triggers_repo is not None:
        context.risk_triggers_repo = user_state.risk_triggers_repo
    # PR6 — bind the account-owned execution coordinator. A04: the
    # coordinator is BORN with the account-owned position_coordinator (see
    # ``build_user_state`` / ``_build_execution_coordinator``); per-binding
    # rewiring of ``coord.position_coordinator`` is forbidden — it let the
    # last-built leader binding win the shared coordinator while earlier
    # bindings kept private lock maps and intent registries.
    coord = getattr(user_state, "execution_coordinator", None)
    if coord is not None:
        context.execution_coordinator = coord
        _bind_reservation_runtime(context, coord)
    # Phase 4 / A04 — seed the ACCOUNT-owned IntentRegistry from durable
    # reservation rows on restart. Must run after _bind_reservation_runtime so
    # context.extra["reservations_repo"] is populated. The registry is shared
    # by every leader binding of this account, so recovery runs ONCE per
    # account (guarded by ``user_state.intents_recovered``) and is scoped to
    # this account's rows: intents from all leaders land in the one registry,
    # and another account's ownership never does. A missing reservations_repo
    # (test contexts, no-reservation deployments) is silently skipped — the
    # registry starts empty which is safe.
    _reservations_repo = context.extra.get("reservations_repo")
    if (
        _reservations_repo is not None
        and not getattr(user_state, "intents_recovered", False)
    ):
        try:
            from services.execution.intent_recovery import recover_intents_from_db

            _recovered = await recover_intents_from_db(
                registry=context.position_coordinator.intent_registry,
                reservations_repo=_reservations_repo,
                account_user_id=context.user_id,
            )
            user_state.intents_recovered = True
            if _recovered:
                logger.info(
                    "runtime_bootstrap.intent_recovery user_id=%s recovered=%d",
                    context.user_id,
                    _recovered,
                )
        except Exception as _exc:
            context.readiness.update(reconciler_ok=False)
            logger.warning(
                "runtime_bootstrap.intent_recovery_failed user_id=%s error_type=%s",
                context.user_id,
                type(_exc).__name__,
            )
            raise RuntimeError("reservation ownership recovery unavailable") from None
    inventory_refresh = getattr(user_state, "inventory_refresh", None)
    if inventory_refresh is not None:
        inventory_refresh.bind_readiness(context.readiness)
    # PR7 — bind position drift service so the coordinator can check
    # ``is_scope_paused`` before submission.
    drift_svc = getattr(user_state, "position_drift_service", None)
    if drift_svc is not None:
        context.position_drift_service = drift_svc
        if coord is not None:
            try:
                coord.position_drift_service = drift_svc
            except Exception:
                pass
    await rehydrate_position_manager(context)
    return context


async def startup_drift_check(
    *,
    runtime_users,
    user_states: dict,
    config: AppConfig,
    readiness,
) -> dict[str, bool]:
    """Run position drift sweep for every user before monitors start.

    Updates ``readiness.drift_ok`` to the overall result and returns a per
    user dict for observability. Each unresolved scope pauses through the
    PositionDriftService's internal state — the coordinator gate honours it.
    """
    all_clean = True
    per_user: dict[str, bool] = {}

    if not config.reconciliation.position_drift.check_on_startup:
        readiness.update(drift_ok=True)
        return per_user

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        if user_state is None:
            per_user[runtime_user.user_id] = False
            all_clean = False
            continue
        drift_svc = getattr(user_state, "position_drift_service", None)
        if drift_svc is None:
            per_user[runtime_user.user_id] = False
            all_clean = False
            continue
        try:
            report = await drift_svc.scan_user(
                user_id=runtime_user.user_id,
                wallet=runtime_user.wallet.funder_address,
            )
        except Exception as exc:
            logger.warning(
                "startup_drift_check_failed | user=%s error=%r",
                runtime_user.user_id,
                exc,
            )
            per_user[runtime_user.user_id] = False
            all_clean = False
            continue
        per_user[runtime_user.user_id] = bool(report.clean)
        if not report.clean:
            all_clean = False
            logger.warning(
                "startup_drift_check_unresolved | user=%s paused_scopes=%d",
                runtime_user.user_id,
                len(report.paused_scopes),
            )

    for user_id, clean in per_user.items():
        readiness.update_account_drift(user_id, clean)
    if not per_user:
        readiness.update(drift_ok=all_clean)
    return per_user


async def hydrate_resolved_market_state(
    *,
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
) -> dict[str, int]:
    """Boot-time gate: REST-query Polymarket's ``/markets/{condition_id}``
    for every open position and mark resolved markets in the per-user
    MarketStateStore.

    Why:
        The WSS ``market_resolved`` event only fires on live resolution.
        A market that resolved while the bot was offline never re-emits the
        event on reconnect, so the position would otherwise poll ``/price``
        forever (the venue happily returns ``price=0`` indefinitely on a
        resolved binary outcome).

    The result feeds ``services/riskMonitor._is_token_resolved`` so neither
    the periodic risk refresh nor the per-position stop-loss probe issues
    HTTP requests for terminal positions.

    Returns a dict mapping user_id → count of (token, condition) pairs
    flagged resolved. Tokens already marked resolved by an earlier path
    (WSS push, prior boot) are still incremented — the operation is
    idempotent so the count reflects the boot survey, not a delta.
    """
    summary: dict[str, int] = {}

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        if user_state is None:
            continue
        market_store = getattr(user_state, "market_store", None)
        positions_repo = getattr(user_state, "positions_repo", None)
        if market_store is None or positions_repo is None:
            continue

        try:
            positions = list(await positions_repo.list_open_positions(runtime_user.user_id) or [])
        except Exception as exc:
            logger.warning(
                "hydrate_resolved_market_state_list_failed | user=%s error=%r",
                runtime_user.user_id,
                exc,
            )
            continue

        tokens_by_condition: dict[str, list[str]] = {}
        for pos in positions:
            cond = str(getattr(pos, "condition_id", "") or "")
            token = str(getattr(pos, "token_id", "") or "")
            if not cond or not token:
                continue
            tokens_by_condition.setdefault(cond, []).append(token)

        resolved_count = 0
        # Gamma market metadata via the official public REST API (allowed by
        # the CCXT-only execution boundary). condition_ids are unique so this
        # is bounded by len(positions) — cheap and one-shot at boot.
        for cond, tokens in tokens_by_condition.items():
            lookup = await _lookup_gamma_market(cond)
            if lookup.unavailable:
                # Transient outage or unexpected payload shape: missing
                # evidence must never mark a position resolved — doing so
                # would disable price probes, stop-loss, and protective exits
                # for the lifetime of the process. Leave monitoring intact;
                # the WSS market_resolved event or the next boot retries.
                continue
            if lookup.market is None:
                # Gamma answered and does not index the condition: every token
                # under it is retired. Mark resolved so the risk monitor stops
                # probing them.
                for token in tokens:
                    try:
                        market_store.mark_resolved(token, cond)
                        resolved_count += 1
                    except Exception:
                        logger.debug("mark_resolved_failed", exc_info=True)
                continue
            market = lookup.market
            # Terminal signals per Polymarket OpenAPI:
            #   - ``closed: true``  → resolved binary outcome decided.
            #   - ``active: false`` → market suspended / not tradeable.
            #   - ``archived: true`` → historical market (documented on
            #     /simplified-markets responses; defensively check here too).
            closed = bool(market.get("closed"))
            inactive = market.get("active") is False
            archived = bool(market.get("archived"))
            if not (closed or inactive or archived):
                continue
            for token in tokens:
                try:
                    market_store.mark_resolved(token, cond)
                    resolved_count += 1
                except Exception:
                    logger.debug("mark_resolved_failed", exc_info=True)

        if resolved_count:
            logger.info(
                "hydrate_resolved_market_state | user=%s resolved=%d",
                runtime_user.user_id,
                resolved_count,
            )
        summary[runtime_user.user_id] = resolved_count

    return summary


_GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"


@dataclass(frozen=True)
class _GammaLookup:
    """Result of one Gamma market-metadata lookup.

    Exactly one of three states:
    * ``market`` set — an authoritative market document;
    * ``market`` None — Gamma answered and does not index the condition
      (authoritative terminal signal);
    * ``unavailable`` True — the lookup itself failed (transport error or
      unexpected payload shape). Never evidence of anything.
    """

    market: dict[str, Any] | None = None
    unavailable: bool = False


async def _lookup_gamma_market(condition_id: str) -> _GammaLookup:
    """Look up one Gamma market document by its 0x condition id.

    Gamma's ``/markets/{id}`` route accepts only the numeric Gamma market id
    (a condition id there returns 422), so this uses the ``condition_ids``
    filter, which resolves condition ids directly.
    """

    cond = str(condition_id or "").strip()
    if not cond:
        return _GammaLookup(unavailable=True)
    query = urlencode((("condition_ids", cond), ("limit", "1")))
    try:
        payload = await fetch_data(f"{_GAMMA_MARKETS_URL}?{query}", silent=True)
    except Exception as exc:
        logger.debug(
            "hydrate_resolved_gamma_unavailable | cond=%s error=%r",
            cond,
            exc,
        )
        return _GammaLookup(unavailable=True)
    if not isinstance(payload, list):
        logger.debug(
            "hydrate_resolved_gamma_unexpected_payload | cond=%s type=%s",
            cond,
            type(payload).__name__,
        )
        return _GammaLookup(unavailable=True)
    if not payload:
        return _GammaLookup(market=None)
    market = payload[0]
    if not isinstance(market, dict):
        logger.debug(
            "hydrate_resolved_gamma_malformed_row | cond=%s type=%s",
            cond,
            type(market).__name__,
        )
        return _GammaLookup(unavailable=True)
    return _GammaLookup(market=market)


async def build_context_maps(
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    active_leaders: list[str],
    config: AppConfig,
    *,
    leaders_repo=None,
    readiness: ReadinessTracker | None = None,
) -> tuple[dict[str, list[TradingContext]], dict[str, TradingContext]]:
    """Index trading contexts by account and leader identity."""
    contexts_by_leader: dict[str, list[TradingContext]] = {leader: [] for leader in active_leaders}
    risk_contexts_by_user: dict[str, TradingContext] = {}

    for runtime_user in runtime_users:
        user_state = user_states[runtime_user.user_id]
        for leader in active_leaders:
            follow = runtime_user.follows.get(leader)
            if not follow or not follow.enabled:
                continue
            context = await build_trading_context(
                user_state,
                follow,
                config,
                leaders_repo=leaders_repo,
                readiness=readiness,
            )
            contexts_by_leader[leader].append(context)
            if context.user_id not in risk_contexts_by_user:
                risk_contexts_by_user[context.user_id] = context

    # P1-i: a user holding open copied positions must still be risk-monitored
    # even when rotation/discovery leaves them with no enabled follow on any
    # active leader (empty active_leaders, or a follow set that no longer
    # intersects the active leaders). Otherwise their open positions go
    # unmonitored and protective exits (stop-loss / trailing) never run. The
    # risk-only context is added to ``risk_contexts_by_user`` ONLY — never to
    # ``contexts_by_leader`` — so it drives protective exits without enabling
    # any new copy entries.
    for runtime_user in runtime_users:
        if runtime_user.user_id in risk_contexts_by_user:
            continue
        risk_user_state = user_states.get(runtime_user.user_id)
        if risk_user_state is None:
            continue
        try:
            open_positions = (
                await risk_user_state.positions_repo.list_open_positions(runtime_user.user_id) or []
            )
        except Exception:
            logger.warning(
                "risk_only_context_open_check_failed | user=%s",
                runtime_user.user_id,
                exc_info=True,
            )
            continue
        if not open_positions:
            continue
        follow = next(iter(runtime_user.follows.values()), None)
        if follow is None:
            follow = UserFollowConfig(
                user_id=runtime_user.user_id,
                leader_id="",
                enabled=False,
                allocation_pct=0,
                max_trade_usdc=0,
                max_market_exposure_usdc=config.risk.max_market_exposure_usdc,
            )
        context = await build_trading_context(
            risk_user_state,
            follow,
            config,
            leaders_repo=leaders_repo,
            readiness=readiness,
        )
        risk_contexts_by_user[runtime_user.user_id] = context
        logger.info(
            "risk_only_context_built | user=%s open_positions=%d (no active-leader follow)",
            runtime_user.user_id,
            len(open_positions),
        )

    return contexts_by_leader, risk_contexts_by_user
