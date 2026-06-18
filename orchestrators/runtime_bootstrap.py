from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from domain.trading_context import TradingContext
from interfaces.clob_gateway import ClobGateway
from cache.market_state_store import MarketStateStore
from services.accountingService import AccountingService
from services.compliance import GeoblockGate
from services.executionService import ExecutionService
from services.feeService import FeeService
from services.market_data_service import MarketDataService, MarketDataPolicy
from services.multiUserConfig import RuntimeUser, UserFollowConfig
from services.pnlTracker import PnLTracker
from services.positionManager import PositionManager
from services.risk.risk_engine import RiskEngine, init_risk_engine
from services.healthServer import ReadinessTracker
from services.entryPermissionService import EntryPermissionService
from services.leaderScoringService import LeaderScoringService
from services.risk.risk_monitor import (
    reconcile_unconfirmed_risk_orders,
    rehydrate_position_manager,
)
from storage.postgres.fills_repo import FillsRepo
from storage.postgres.orders_repo import OrdersRepo
from storage.postgres.positions_repo import PositionsRepo
from storage.postgres.risk_blocks_repo import RiskBlocksRepo
from storage.postgres.risk_triggers_repo import RiskTriggersRepo
from utils.fetchData import fetch_data
from utils.logger import get_logger

if TYPE_CHECKING:
    from config.app_config import AppConfig

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
    released_ids = await release_expired()
    try:
        return len(released_ids or [])
    except TypeError:
        return 0


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
            trade_n = await reconcile_submitted_unconfirmed(context)
            risk_n = await reconcile_unconfirmed_risk_orders(context)
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
    user: RuntimeUser
    client: object
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
    manual_sync_service: "object | None" = None


def configured_leaders(config: AppConfig) -> list[str]:
    leaders = [str(wallet).strip() for wallet in config.leader.wallets if str(wallet).strip()]
    if not leaders:
        if config.discovery.allow_auto_trader_detection:
            logger.info(
                "No leader wallets configured. Startup auto trader detection "
                "is enabled and will select live-eligible DB/discovery "
                "candidates without writing LEADER_WALLETS."
            )
        elif config.discovery.enable_trader_discovery:
            logger.info(
                "No leader wallets configured. Discovery is enabled, so the bot "
                "will score DB candidates only. Live copying requires "
                "python bot.py rotate-traders or manual LEADER_WALLETS."
            )
        else:
            logger.warning(
                "No leader wallets configured. Both LEADER_WALLETS and USER_ADDRESS "
                "are empty. Add manual leader wallets or run "
                "python bot.py rotate-traders after discovery has candidates."
            )
    return leaders


def default_follow(user_id: str, leader_id: str, config: AppConfig) -> UserFollowConfig:
    return UserFollowConfig(
        user_id=user_id,
        leader_id=leader_id,
        enabled=True,
        allocation_pct=config.sizing.percentage / 100.0,
        max_trade_usdc=config.sizing.buy_max_amount,
        max_market_exposure_usdc=config.risk.max_market_exposure_usdc,
    )


def ensure_runtime_follows(runtime_users: list[RuntimeUser], leaders: list[str], config: AppConfig) -> None:
    for runtime_user in runtime_users:
        for leader in leaders:
            if leader not in runtime_user.follows:
                runtime_user.follows[leader] = default_follow(runtime_user.user_id, leader, config)


async def fetch_live_positions(wallet: str) -> list[dict] | None:
    """Fetch the wallet's live positions from the data API.

    Returns ``None`` when the snapshot could not be fetched (network error,
    non-list response). Callers MUST distinguish ``None`` (fetch failed →
    fail closed, do not prune/delete local state) from ``[]`` (the wallet
    genuinely holds no open positions).
    """
    try:
        positions = await fetch_data(
            f"https://data-api.polymarket.com/positions?user={wallet}",
            silent=True,
        )
    except Exception:
        return None

    if not isinstance(positions, list):
        return None

    live_positions: list[dict] = []
    for position in positions:
        try:
            size = float(position.get("size") or 0)
            value = float(position.get("currentValue") or 0)
        except (TypeError, ValueError):
            continue
        if abs(size) <= 1e-12 or value <= 0:
            continue
        live_positions.append(position)
    return live_positions


def build_user_state(
    runtime_user: RuntimeUser,
    client: object,
    pool,
    config: AppConfig,
) -> UserRuntimeState:
    init_risk_engine(config)

    # Push typed config into the CLOB-client module so its CLOB_HTTP_URL +
    # meta-cache TTLs follow AppConfig rather than the raw env (legacy-kill
    # slice #77). Safe to call repeatedly per-user.
    from utils.createClobClient import configure_from_appconfig as _configure_clob_module
    _configure_clob_module(config)

    # Same pattern for the balance-lookup module (RPC_URL + collateral
    # contract addresses) so utils/getMyBalance.py drops its ENV reads.
    from utils.getMyBalance import configure_from_appconfig as _configure_balance_module
    _configure_balance_module(config)

    order_repo = OrdersRepo(pool)
    fills_repo = FillsRepo(pool)
    positions_repo = PositionsRepo(pool)
    risk_blocks_repo = RiskBlocksRepo(pool)
    risk_triggers_repo = RiskTriggersRepo(pool)
    pnl_tracker = PnLTracker(log_path=f"log/pnl_{runtime_user.user_id}.jsonl")
    risk_engine = RiskEngine()
    accounting_service = AccountingService()
    position_manager = PositionManager(config=config)
    # Single CLOB venue boundary — same instance used by ExecutionService and,
    # when issue #71 lands, the reconciler / execution coordinator. SDK-shaped
    # types stay behind the gateway. See issue #66.
    from integrations.polymarket.clob_v2_gateway import ClobV2Gateway
    clob_gateway = ClobV2Gateway(client, config)

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
            rest_bootstrap_max_age_ms=int(
                getattr(md_cfg, "rest_bootstrap_max_age_ms", 500)
            ),
            rest_snapshot_max_age_ms=int(
                getattr(md_cfg, "rest_snapshot_max_age_ms", 500)
            ),
            rest_snapshot_request_timeout_ms=int(
                getattr(md_cfg, "rest_snapshot_request_timeout_ms", 750)
            ),
            rest_snapshot_total_budget_ms=int(
                getattr(md_cfg, "rest_snapshot_total_budget_ms", 1000)
            ),
            rest_snapshot_max_spread_bps=int(
                getattr(md_cfg, "rest_snapshot_max_spread_bps", 300)
            ),
            max_quote_age_seconds=float(
                getattr(md_cfg, "executable_quote_max_age_seconds", 2.0)
            ),
            max_book_age_seconds=float(
                getattr(md_cfg, "executable_book_max_age_seconds", 2.0)
            ),
            max_tick_age_seconds=float(
                getattr(md_cfg, "executable_tick_max_age_seconds", 300.0)
            ),
            wss_connection_max_silence_seconds=float(
                getattr(md_cfg, "wss_connection_max_silence_seconds", 20.0)
            ),
        ),
    )

    execution_service = ExecutionService(
        clob_client=client,
        fee_service=FeeService(client),
        order_repo=order_repo,
        config=config,
        clob_gateway=clob_gateway,
        market_data_service=market_data_svc,
        # P1-j: trade-path geoblock enforcement with expiry revalidation. Opt-in
        # via ENFORCE_GEOBLOCK (default on); fail-closed; BUY entries only.
        geoblock_gate=GeoblockGate(config.network.enforce_geoblock),
    )

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
    manual_sync_service = _build_manual_sync_service(
        funder_address=runtime_user.wallet.funder_address,
        user_id=runtime_user.user_id,
        positions_repo=positions_repo,
        market_store=market_store,
        fills_repo=fills_repo,
        orders_repo=order_repo,
        position_drift_service=position_drift_service,
        reservation_service=reservation_service,
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
        positions_repo=positions_repo,
        market_store=market_store,
        fills_repo=fills_repo,
        orders_repo=orders_repo,
        position_drift_service=position_drift_service,
        reservation_service=reservation_service,
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
        config=config.position_drift,
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
):
    """Build the production ExecutionCoordinator for a user.

    Wraps ReservationService around ExecutionService so every production
    submit path applies reservation + collision semantics. Returns None when
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
    reservations_enabled = (
        config.execution.reservations_enabled if config is not None else False
    )
    _reservation_ttl = (
        config.execution.reservation_ttl_seconds if config is not None else 120
    )
    reservation_service = ReservationService(
        reservations_repo=reservations_repo,
        positions_repo=positions_repo,
        get_balance_func=get_my_balance,
        funder_address=funder_address,
        ttl_seconds=_reservation_ttl,
        enabled=reservations_enabled,
        # sym 5 — break the reservation-release deadlock. A reservation linked
        # to a FAK order the reconciler can never confirm terminal would
        # otherwise pin its scope forever (the reason RESERVATIONS_ENABLED was
        # turned off). Force-release once it has been expired by a full extra
        # TTL window: a fill-and-kill order cannot rest, so by then it is
        # definitively not live and the held budget is stranded.
        force_release_grace_seconds=float(_reservation_ttl),
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

    reservations_repo = (
        getattr(coord, "reservations_repo", None)
        or getattr(reservation_service, "reservations_repo", None)
    )
    context.extra["reservation_service"] = reservation_service
    if reservations_repo is not None:
        context.extra["reservations_repo"] = reservations_repo

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

        condition_id = (
            request.get("condition_id")
            or _reservation_attr(reservation, "condition_id")
        )
        token_id = (
            request.get("token_id")
            or request.get("asset")
            or _reservation_attr(reservation, "token_id")
        )
        user_id = (
            (order_row or {}).get("user_id") if isinstance(order_row, dict) else None
        ) or request.get("user_id") or _reservation_attr(reservation, "user_id") or context.user_id
        leader_id = (
            (order_row or {}).get("leader_id") if isinstance(order_row, dict) else None
        ) or request.get("leader_id") or _reservation_attr(reservation, "leader_wallet")
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

    async def _lookup_order_by_reservation(reservation_id: str) -> dict[str, Any] | None:
        order_lookup = getattr(context.orders_repo, "get_by_reservation_id", None)
        if not callable(order_lookup):
            return None
        try:
            order_row = await order_lookup(reservation_id)
        except Exception:
            return None
        return order_row if isinstance(order_row, dict) else None

    async def _lookup_order_by_order_id(order_id: str | None) -> dict[str, Any] | None:
        if not order_id:
            return None
        order_lookup = getattr(context.orders_repo, "get_by_order_id", None)
        if not callable(order_lookup):
            return None
        try:
            order_row = await order_lookup(order_id)
        except Exception:
            return None
        return order_row if isinstance(order_row, dict) else None

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

    async def _reconcile_expired_reservation(reservation_id: str) -> bool:
        """Run canonical reconciliation before an expired reservation releases."""
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
            return False

        if reservations_repo is None:
            return False

        try:
            active = await reservations_repo.get_by_id(reservation_id)
        except Exception as exc:
            logger.warning(
                "reservation_expiry_lookup_failed | user=%s reservation_id=%s error=%r",
                context.user_id,
                reservation_id,
                exc,
            )
            return False

        if active is None or str(getattr(active, "status", "")).lower() != "active":
            return True

        linked_order = getattr(active, "order_id", None) or getattr(active, "venue_order_id", None)
        order_row = await _lookup_order_by_reservation(reservation_id)
        candidate_order_id = str(
            linked_order or (order_row or {}).get("order_id") or ""
        ).strip()
        if candidate_order_id:
            if order_row is None:
                order_row = await _lookup_order_by_order_id(candidate_order_id)
            await _reconcile_linked_reservation_order(
                reservation=active,
                order_id=candidate_order_id,
                order_row=order_row,
            )
            return await _reservation_is_released(reservation_id)

        try:
            await reservation_service.release_terminal(
                reservation_id,
                reason="expired_orphan_no_order",
            )
        except Exception as exc:
            logger.warning(
                "reservation_expiry_orphan_release_failed | user=%s reservation_id=%s error=%r",
                context.user_id,
                reservation_id,
                exc,
            )
            return False
        return True

    reservation_service.reconciler_callback = _reconcile_expired_reservation


async def build_trading_context(
    user_state: UserRuntimeState,
    follow: UserFollowConfig,
    config: AppConfig,
    leaders_repo=None,
    readiness: ReadinessTracker | None = None,
    trust_store=None,
) -> TradingContext:
    leader_scorer = LeaderScoringService(config=config, trust_store=trust_store)
    context = TradingContext(
        user_id=user_state.user.user_id,
        wallet_address=user_state.user.wallet.funder_address,
        client=user_state.client,
        positions_repo=user_state.positions_repo,
        fills_repo=user_state.fills_repo,
        orders_repo=user_state.orders_repo,
        risk_blocks_repo=user_state.risk_blocks_repo,
        pnl_tracker=user_state.pnl_tracker,
        risk_engine=user_state.risk_engine,
        accounting_service=user_state.accounting_service,
        position_manager=user_state.position_manager,
        config=config,
        entry_permission_service=EntryPermissionService(user_state.risk_blocks_repo),
        execution_service=user_state.execution_service,
        leaders_repo=leaders_repo,
        leader_scorer=leader_scorer,
        allocation_pct=follow.allocation_pct,
        buy_min_amount=config.sizing.buy_min_amount,
        buy_max_amount=min(config.sizing.buy_max_amount, follow.max_trade_usdc),
        max_market_exposure_usdc=follow.max_market_exposure_usdc,
        readiness=readiness or ReadinessTracker(),
        market_data_service=getattr(user_state, "market_data_service", None),
        extra={
            "slippage_tolerance_bps": int(config.execution.slippage_tolerance * 100),
            "clob_gateway": user_state.clob_gateway,
        },
    )
    if user_state.risk_triggers_repo is not None:
        context.risk_triggers_repo = user_state.risk_triggers_repo
    # PR6 — bind execution coordinator and give it the per-context scope lock.
    coord = getattr(user_state, "execution_coordinator", None)
    if coord is not None:
        try:
            coord.position_coordinator = context.position_coordinator
        except Exception as exc:
            # If coord is frozen/slotted the silent bypass would leave live
            # submits without the per-context position coordinator.
            # Surface the wiring failure loudly so ops can fix it.
            logger.error(
                "runtime_bootstrap.coordinator_wiring_failed user_id=%s error=%r",
                getattr(context, "user_id", "?"), exc,
            )
            raise RuntimeError(
                "runtime_bootstrap: failed to attach position_coordinator to "
                f"execution_coordinator for user {getattr(context, 'user_id', '?')}"
            ) from exc
        assert (
            getattr(coord, "position_coordinator", None) is context.position_coordinator
        ), "runtime_bootstrap: position_coordinator attach silently dropped"
        context.execution_coordinator = coord
        _bind_reservation_runtime(context, coord)
    # Phase 4 — seed IntentRegistry from durable reservation rows on restart.
    # Must run after _bind_reservation_runtime so context.extra["reservations_repo"]
    # is populated.  A missing reservations_repo (test contexts, no-reservation
    # deployments) is silently skipped — the registry starts empty which is safe.
    _reservations_repo = context.extra.get("reservations_repo")
    if _reservations_repo is not None:
        try:
            from services.execution.intent_recovery import recover_intents_from_db
            _recovered = await recover_intents_from_db(
                registry=context.position_coordinator.intent_registry,
                reservations_repo=_reservations_repo,
            )
            if _recovered:
                logger.info(
                    "runtime_bootstrap.intent_recovery user_id=%s recovered=%d",
                    context.user_id, _recovered,
                )
        except Exception as _exc:
            logger.warning(
                "runtime_bootstrap.intent_recovery_failed user_id=%s error=%r",
                context.user_id, _exc,
            )
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

    if not config.position_drift.check_on_startup:
        readiness.update(drift_ok=True)
        return per_user

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        if user_state is None:
            continue
        drift_svc = getattr(user_state, "position_drift_service", None)
        if drift_svc is None:
            # No drift service wired (e.g. partial test state). Skip
            # gracefully; in production this is unreachable.
            per_user[runtime_user.user_id] = True
            continue
        try:
            report = await drift_svc.scan_user(
                user_id=runtime_user.user_id,
                wallet=runtime_user.wallet.funder_address,
            )
        except Exception as exc:
            logger.warning(
                "startup_drift_check_failed | user=%s error=%r",
                runtime_user.user_id, exc,
            )
            per_user[runtime_user.user_id] = False
            all_clean = False
            continue
        per_user[runtime_user.user_id] = bool(report.clean)
        if not report.clean:
            all_clean = False
            logger.warning(
                "startup_drift_check_unresolved | user=%s paused_scopes=%d",
                runtime_user.user_id, len(report.paused_scopes),
            )

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
    loop = asyncio.get_running_loop()

    for runtime_user in runtime_users:
        user_state = user_states.get(runtime_user.user_id)
        if user_state is None:
            continue
        market_store = getattr(user_state, "market_store", None)
        client = getattr(user_state, "client", None)
        positions_repo = getattr(user_state, "positions_repo", None)
        if market_store is None or client is None or positions_repo is None:
            continue

        try:
            positions = list(
                await positions_repo.list_open_positions(runtime_user.user_id) or []
            )
        except Exception as exc:
            logger.warning(
                "hydrate_resolved_market_state_list_failed | user=%s error=%r",
                runtime_user.user_id, exc,
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
        # Sync SDK call must run in a thread; condition_ids are unique so
        # this is bounded by len(positions) — cheap and one-shot at boot.
        for cond, tokens in tokens_by_condition.items():
            try:
                market = await loop.run_in_executor(None, client.get_market, cond)
            except Exception as exc:
                # SDK raises (PolyApiException or similar) when the condition
                # itself is unknown to Polymarket — typically a 404 on
                # ``/markets/{cond}``. Treat that as terminal: if the venue
                # no longer indexes the condition, every token under it is
                # retired. We mark resolved here so the risk monitor stops
                # probing them.
                err_text = repr(exc).lower()
                if "404" in err_text or "not_found" in err_text or "not found" in err_text:
                    for token in tokens:
                        try:
                            market_store.mark_resolved(token, cond)
                            resolved_count += 1
                        except Exception:
                            logger.debug("mark_resolved_failed", exc_info=True)
                else:
                    logger.debug(
                        "hydrate_resolved_get_market_failed | cond=%s error=%r",
                        cond, exc,
                    )
                continue
            if not isinstance(market, dict):
                continue
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
                runtime_user.user_id, resolved_count,
            )
        summary[runtime_user.user_id] = resolved_count

    return summary


async def build_context_maps(
    runtime_users: list[RuntimeUser],
    user_states: dict[str, UserRuntimeState],
    active_leaders: list[str],
    config: AppConfig,
    *,
    leaders_repo=None,
    readiness: ReadinessTracker | None = None,
    trust_store=None,
) -> tuple[dict[str, list[TradingContext]], dict[str, TradingContext]]:
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
                trust_store=trust_store,
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
        user_state = user_states.get(runtime_user.user_id)
        if user_state is None:
            continue
        try:
            open_positions = (
                await user_state.positions_repo.list_open_positions(
                    runtime_user.user_id
                )
                or []
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
            logger.warning(
                "risk_only_context_skipped_no_follow | user=%s open_positions=%d",
                runtime_user.user_id,
                len(open_positions),
            )
            continue
        context = await build_trading_context(
            user_state,
            follow,
            config,
            leaders_repo=leaders_repo,
            readiness=readiness,
            trust_store=trust_store,
        )
        risk_contexts_by_user[runtime_user.user_id] = context
        logger.info(
            "risk_only_context_built | user=%s open_positions=%d "
            "(no active-leader follow)",
            runtime_user.user_id,
            len(open_positions),
        )

    return contexts_by_leader, risk_contexts_by_user
