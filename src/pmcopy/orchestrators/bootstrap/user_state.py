"""Per-account UserRuntimeState composition, follow normalization, and venue builder."""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pmcopy.application.runtime_state import (
    AccountExecutionRuntimeState,
    AccountRiskRuntimeState,
)
from pmcopy.cache.market_state_store import MarketStateStore
from pmcopy.config.app_config import AppConfig
from pmcopy.domain.account_identity import CopierAccountId
from pmcopy.interfaces.clob_gateway import ClobGateway
from pmcopy.orchestrators.bootstrap.reservation_wiring import (
    _build_ambiguous_recovery_service,
    _build_execution_coordinator,
    _build_manual_sync_service,
    _build_position_drift_service,
    _build_reservations_repo,
    fetch_live_positions,  # noqa: F401
)
from pmcopy.orchestrators.lifecycle.account_inventory_startup import InventoryRuntimeRefresh
from pmcopy.services.accounting.accounting_service import AccountingService
from pmcopy.services.accounting.fee_service import FeeService
from pmcopy.services.accounting.pnl_tracker import PnLTracker
from pmcopy.services.accounts.balance_regime import BalanceRegime
from pmcopy.services.accounts.multi_user_config import RuntimeUser, UserFollowConfig
from pmcopy.services.compliance import GeoblockGate
from pmcopy.services.copy_tasks.entry_permission import TaskEntryPermissionService
from pmcopy.services.execution.circuit_breaker import CircuitBreaker
from pmcopy.services.execution.coordinator import CopyAdmission
from pmcopy.services.execution.execution_service import ExecutionService
from pmcopy.services.market_data.market_data_service import MarketDataPolicy, MarketDataService
from pmcopy.services.positions.position_coordinator import PositionCoordinator
from pmcopy.services.positions.position_manager import PositionManager
from pmcopy.services.risk.risk_engine import RiskEngine, init_risk_engine
from pmcopy.services.telemetry import TelemetryEmitters
from pmcopy.services.trading.leader_performance import LeaderPerformanceReader
from pmcopy.services.trading.trading_permission import TradingPermissionService
from pmcopy.storage.postgres.copy_task_repository import PostgresCopyTaskRepository
from pmcopy.storage.postgres.fills_repo import FillsRepo
from pmcopy.storage.postgres.leader_performance_repository import PostgresLeaderPerformanceReader
from pmcopy.storage.postgres.orders_repo import OrdersRepo
from pmcopy.storage.postgres.positions_repo import PositionsRepo
from pmcopy.storage.postgres.risk_blocks_repo import RiskBlocksRepo
from pmcopy.storage.postgres.risk_lot_repository import PostgresRiskLotRepository
from pmcopy.storage.postgres.risk_triggers_repo import RiskTriggersRepo
from pmcopy.storage.postgres.task_fill_projection import PostgresTaskFillProjection
from pmcopy.storage.postgres.trading_control_repo import TradingControlRepo
from pmcopy.utils.logger import get_logger

if TYPE_CHECKING:
    from pmcopy.integrations.polymarket.ccxt_prediction_client import CcxtPredictionClient
    from pmcopy.integrations.polymarket.venue_runtime import PolymarketVenueRuntime
    from pmcopy.services.execution.ambiguous_recovery import AmbiguousSubmissionRecoveryService
    from pmcopy.services.positions.manual_sync_service import ManualSyncService
    from pmcopy.storage.postgres.fill_evidence_projection import PostgresFillEvidenceProjection

logger = get_logger("RuntimeBootstrap")


def _bootstrap_facade_attr(name: str, default: Any) -> Any:
    facade = sys.modules.get("pmcopy.orchestrators.bootstrap.runtime_bootstrap")
    return getattr(facade, name, default) if facade is not None else default


@dataclass
class UserRuntimeState:
    """Hold the composed runtime state for one follower account.

    Task 21 — this state IS the account-owned runtime: exactly one instance
    per copier account, constructed only by ``build_user_state``, explicitly
    owning every account-scoped concern. Ownership map (nothing below may be
    constructed per-leader or shared across accounts):

    * ``account`` — the typed account identity (``CopierAccountId``, the
      persisted ``user_id``; stable across display/config renames).
    * ``position_coordinator`` — the ONE per-scope lock map + intent
      registry (A04); born-wired into ``execution_coordinator`` and injected
      into every per-leader ``TradingContext`` and the risk context.
    * ``execution_coordinator`` — reservation service, collision scope, and
      leader-sell-target service for the whole account.
    * ``ambiguous_recovery`` — account-scoped no-ID lost-POST recovery
      (A01: scans only this account's rows).
    * ``risk_engine`` / ``position_manager`` / ``accounting_service`` /
      ``pnl_tracker`` — account risk and position state.
    * ``trading_permission`` — the account's cached operator control gate.
    * ``clob_gateway`` / ``client`` — the private venue boundary (one CCXT
      exchange per follower wallet; the ONLY authenticated transport).
    * ``manual_sync_service`` / ``inventory_refresh`` / ``position_drift_service``
      — account inventory convergence and drift pause scopes.
    * ``market_store`` / ``market_data_service`` — the account's market state.
    * ``intents_recovered`` — the once-per-account intent-recovery guard.

    Per-LEADER (not account) objects — ``LeaderScoringService``,
    ``EntryPermissionService``, follow config, roster multipliers — are
    policy references and stay per-binding. Cross-PROCESS mutual exclusion
    for the account is enforced by the DB-backed lease acquired at startup
    (``services.account_lease``); this state presumes it is held.
    """

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
    # Task 21 — the typed account identity for this runtime. Set by
    # ``build_user_state`` from the persisted ``user_id``; optional only so
    # pre-existing test fixtures that construct partial states stay valid
    # (``account_id`` derives it lazily for those).
    account: "CopierAccountId | None" = None
    # WP-C — account-owned daily spend regime, execution/reconciliation state,
    # and risk-monitor state shared across every per-leader TradingContext and
    # the protective risk context of this follower account.
    balance_regime: BalanceRegime | None = None
    account_execution_state: AccountExecutionRuntimeState = field(
        default_factory=AccountExecutionRuntimeState
    )
    risk_state: AccountRiskRuntimeState = field(default_factory=AccountRiskRuntimeState)

    @property
    def account_id(self) -> "CopierAccountId":
        """The typed identity of the account owning this runtime."""
        if self.account is not None:
            return self.account
        # Duck-typed fallback for foreign/test states constructed without
        # ``build_user_state``; production always carries the explicit field.
        return CopierAccountId(self.user.user_id)


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
    """Build the default follower-to-leader binding.

    A06 — follow money is exact ``Decimal`` end-to-end. The typed AppConfig
    surface is float-typed; ``Decimal(str(...))`` of a float is its shortest
    exact decimal text, so this is the lossless trusted-config boundary
    (operator-supplied payload money must arrive as decimal text and is
    strictly validated in ``services.multiUserConfig``).
    """
    return UserFollowConfig(
        user_id=user_id,
        leader_id=leader_id,
        enabled=True,
        allocation_pct=Decimal(str(config.sizing.percentage)) / Decimal("100"),
        max_trade_usdc=Decimal(str(config.sizing.buy_max_amount)),
        max_market_exposure_usdc=Decimal(str(config.risk.max_market_exposure_usdc)),
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
    venue_circuit_breaker: CircuitBreaker | None = None,
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
    # contract addresses) so utils/wallet_balance.py drops its ENV reads.
    from pmcopy.utils.wallet_balance import configure_from_appconfig as _configure_balance_module

    _configure_balance_module(config)

    order_repo = OrdersRepo(pool)
    fills_repo = _bootstrap_facade_attr("FillsRepo", FillsRepo)(
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
    position_manager = _bootstrap_facade_attr("PositionManager", PositionManager)(config=config)
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
    execution_service = _bootstrap_facade_attr("ExecutionService", ExecutionService)(
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
        circuit_breaker=venue_circuit_breaker,
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
    # Task 24 fix round 2 — the COPIER-level open-exposure cap: the minimum
    # across this account's EXPLICITLY configured caps (schema v2
    # ``limits.max_open_exposure_quote`` puts one value on every binding).
    # Absent explicit caps this stays None: legacy accounts take no new
    # reservation-transaction work.
    _explicit_caps = [
        Decimal(str(f.max_market_exposure_usdc))
        for f in (getattr(runtime_user, "follows", None) or {}).values()
        if getattr(f, "market_exposure_cap_explicit", False)
    ]
    open_exposure_cap = min(_explicit_caps) if _explicit_caps else None

    confirmed_exposure_reader = None
    if open_exposure_cap is not None:
        from pmcopy.services.risk.account_snapshot import AccountRiskSnapshotBuilder

        async def _confirmed_open_exposure(uid: str) -> Decimal | None:
            """Confirmed open exposure on the SAME cost-basis accounting the
            admission snapshot uses (``AccountRiskSnapshotBuilder`` over the
            account's held positions) — no second valuation."""
            try:
                positions = await positions_repo.list_held_positions(uid)
                snapshot = AccountRiskSnapshotBuilder(
                    market_store,
                    max_age_seconds=float(config.market_data.executable_quote_max_age_seconds),
                ).build(
                    account=uid,
                    free_cash=Decimal("0"),
                    positions=positions or [],
                    condition_id="",
                    leader_id="",
                )
                return snapshot.total_exposure_usdc
            except Exception as exc:
                logger.warning(
                    "copier_cap_confirmed_exposure_unavailable | user=%s error_type=%s",
                    uid,
                    type(exc).__name__,
                )
                return None

        confirmed_exposure_reader = _confirmed_open_exposure

    # The reservations repo is hoisted BEFORE both the drift service and the
    # execution coordinator so (a) the drift service can be built first and
    # (b) the coordinator's reservation service owns the SAME repo instance
    # the drift service prunes against, exactly as before the reorder. The
    # helper mirrors the coordinator's own None cases (no funder address,
    # import failure), so a coordinator-less runtime keeps a repo-less drift
    # service.
    reservations_repo = _build_reservations_repo(
        pool,
        funder_address=runtime_user.wallet.funder_address,
    )

    # PR7 — proactive position drift sweep. Built BEFORE manual_sync so the
    # handshake (manual_sync.was_recently_pruned → skip upsert) is wired
    # from boot. Without this ordering manual_sync would resurrect rows
    # drift authoritatively pruned, looping risk-monitor /price probes.
    # The service is ACCOUNT-owned (one per user) and is injected into the
    # execution coordinator at construction below, so it must exist first.
    position_drift_service = _build_position_drift_service(
        pool=pool,
        positions_repo=positions_repo,
        reservations_repo=reservations_repo,
        config=config,
    )

    execution_coordinator = _build_execution_coordinator(
        pool=pool,
        positions_repo=positions_repo,
        execution_service=execution_service,
        funder_address=runtime_user.wallet.funder_address,
        config=config,
        provider_runtime=provider_runtime,
        latency=emitters.latency,
        copy_admission=copy_admission,
        position_coordinator=position_coordinator,
        open_exposure_cap=open_exposure_cap,
        confirmed_exposure_reader=confirmed_exposure_reader,
        reservations_repo=reservations_repo,
        position_drift_service=position_drift_service,
    )

    reservation_service = getattr(execution_coordinator, "reservation_service", None)

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

    balance_regime = (
        BalanceRegime.from_config(config.execution) if isinstance(config, AppConfig) else None
    )

    return UserRuntimeState(
        user=runtime_user,
        client=client,
        # Task 21 — the typed account identity this whole runtime owns.
        account=CopierAccountId.from_runtime_user(runtime_user),
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
        balance_regime=balance_regime,
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

    from pmcopy.integrations.polymarket.venue_runtime import build_polymarket_runtime

    if exchange_factory is not None and not callable(exchange_factory):
        raise TypeError("exchange_factory must be callable")
    return await build_polymarket_runtime(
        runtime_user=runtime_user,
        config=config,
        exchange_factory=exchange_factory,  # type: ignore[arg-type]
        ccxt_version=ccxt_version,
    )



def _user_state_telemetry(user_state: object) -> TelemetryEmitters:
    """Return the follower's emitter bundle, or the disabled null object."""
    emitters = getattr(user_state, "telemetry", None)
    if isinstance(emitters, TelemetryEmitters):
        return emitters
    return TelemetryEmitters.disabled()


def _ensure_user_state_account_containers(
    user_state: Any,
) -> tuple[AccountExecutionRuntimeState, AccountRiskRuntimeState]:
    """Return (and lazily attach on synthetic test states) per-account state owners."""
    exec_state = getattr(user_state, "account_execution_state", None)
    if not isinstance(exec_state, AccountExecutionRuntimeState):
        exec_state = AccountExecutionRuntimeState()
        try:
            user_state.account_execution_state = exec_state
        except AttributeError:
            pass
    risk_state = getattr(user_state, "risk_state", None)
    if not isinstance(risk_state, AccountRiskRuntimeState):
        risk_state = AccountRiskRuntimeState()
        try:
            user_state.risk_state = risk_state
        except AttributeError:
            pass
    return exec_state, risk_state


