"""Manual sync, ambiguous recovery, position drift, coordinator, and reservation wiring."""

from __future__ import annotations

import asyncio
import inspect
import sys
from collections.abc import Awaitable, Callable, Mapping, Sized
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from functools import partial
from typing import TYPE_CHECKING, Any, cast

from pmcopy.application.runtime_state import (
    resolve_account_execution_state,
    resolve_reservation_service,
)
from pmcopy.application.trading_context import TradingContext
from pmcopy.cache.market_state_store import MarketStateStore
from pmcopy.config.app_config import AppConfig
from pmcopy.integrations.polymarket.data_api_positions import DataApiPositionSizeReader
from pmcopy.interfaces.clob_gateway import ClobGateway
from pmcopy.services.execution.coordinator import CopyAdmission
from pmcopy.services.execution.execution_service import ExecutionService
from pmcopy.services.execution.reservation_expiry import (
    OrderRowLookup,
    ReservationExpiryDecision,
    classify_no_id_reservation_expiry,
)
from pmcopy.services.positions.position_coordinator import PositionCoordinator
from pmcopy.services.risk.risk_monitor import (
    reconcile_unconfirmed_risk_orders,
    rehydrate_position_manager,
)
from pmcopy.services.telemetry.latency import PipelineLatency
from pmcopy.storage.postgres.fills_repo import FillsRepo
from pmcopy.storage.postgres.orders_repo import OrdersRepo
from pmcopy.storage.postgres.positions_repo import PositionsRepo
from pmcopy.utils.fetch_data import _get_http_client
from pmcopy.utils.logger import get_logger

if TYPE_CHECKING:
    from pmcopy.services.execution.ambiguous_recovery import AmbiguousSubmissionRecoveryService

logger = get_logger("RuntimeBootstrap")


def _bootstrap_facade_attr(name: str, default: Any) -> Any:
    facade = sys.modules.get("pmcopy.orchestrators.bootstrap.runtime_bootstrap")
    return getattr(facade, name, default) if facade is not None else default


async def fetch_live_positions(wallet: str) -> list[dict[str, object]] | None:
    """Read a bounded complete inventory without equating zero marks to disposal."""
    client_getter = _bootstrap_facade_attr("_get_http_client", _get_http_client)
    return await DataApiPositionSizeReader(client_getter).positions(wallet)


async def _release_expired_reservations_for_context(context: TradingContext) -> int:
    reservation_service = resolve_reservation_service(context)
    release_expired = getattr(reservation_service, "release_expired_active", None)
    if getattr(reservation_service, "enabled", True) is False:
        return 0
    if not callable(release_expired):
        return 0
    release_result = release_expired()
    released_ids = await release_result if inspect.isawaitable(release_result) else release_result
    return len(released_ids) if isinstance(released_ids, Sized) else 0



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
        from pmcopy.services.positions.manual_sync_service import ManualSyncService
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
        from pmcopy.integrations.polymarket.ccxt_ambiguous_evidence import (
            CcxtAmbiguousEvidenceSource,
        )
        from pmcopy.integrations.polymarket.data_api_positions import DataApiPositionSizeReader
        from pmcopy.services.execution.ambiguous_recovery import AmbiguousSubmissionRecoveryService
        from pmcopy.storage.postgres.reservations_repo import ReservationsRepo
        from pmcopy.utils.wallet_balance import get_my_balance
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
                async with asyncio.timeout(60.0):
                    results = await cast(Callable[[], Awaitable[Any]], recover_once)()
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
        from pmcopy.services.positions.position_drift_service import PositionDriftService
    except Exception as exc:
        logger.warning("position_drift_service_unwired error=%r", exc)
        return None

    async def _fetcher(wallet: str):
        # Fail closed: a None snapshot means the fetch failed. Raise so the
        # drift service treats it as unresolved (it is designed to fail closed
        # on a raising fetcher) rather than as "venue holds nothing".
        result = await _bootstrap_facade_attr("fetch_live_positions", fetch_live_positions)(wallet)
        if result is None:
            raise RuntimeError("live positions snapshot fetch failed")
        return result

    return PositionDriftService(
        positions_repo=positions_repo,
        reservations_repo=reservations_repo,
        live_positions_fetcher=_fetcher,
        config=config.reconciliation.position_drift,
    )


def _build_reservations_repo(pool, *, funder_address: str):
    """Hoist the per-account reservations repo for drift + coordinator.

    The account-owned drift service must exist BEFORE the execution
    coordinator (the coordinator is BORN with it), yet its reservations repo
    is the one the coordinator's reservation service will own. Building the
    repo here keeps the historical None-repo cases: no funder address or an
    import failure of the reservations storage layer yields None exactly as
    the old coordinator-derived lookup did.
    """
    if not funder_address:
        return None
    try:
        from pmcopy.storage.postgres.reservations_repo import ReservationsRepo
    except Exception as exc:
        logger.warning("reservations_repo_unwired error=%r", exc)
        return None
    return ReservationsRepo(pool)


def _build_execution_coordinator(
    *,
    pool,
    positions_repo: PositionsRepo,
    execution_service: ExecutionService,
    funder_address: str,
    config: "AppConfig | None" = None,
    latency: PipelineLatency | None = None,
    copy_admission: CopyAdmission | None = None,
    provider_runtime: Any = None,
    position_coordinator: PositionCoordinator | None = None,
    open_exposure_cap: Decimal | None = None,
    confirmed_exposure_reader=None,
    reservations_repo=None,
    position_drift_service=None,
):
    """Build the production ExecutionCoordinator for a user.

    Wraps ReservationService around ExecutionService so every production
    submit path applies reservation + collision semantics. The account-owned
    ``position_coordinator`` (A04) and the account-owned
    ``position_drift_service`` (PR7) are injected at construction so the
    execution funnel serializes on the same lock map as every per-binding
    TradingContext and honours the drift gate from birth; there is no
    post-construction rewiring. Returns None when the funder address is
    missing so test contexts that build a partial state keep working.
    """
    if not funder_address:
        return None
    try:
        from pmcopy.services.execution.coordinator import ExecutionCoordinator
        from pmcopy.services.execution.reservation_service import ReservationService
        from pmcopy.storage.postgres.reservations_repo import ReservationsRepo
        from pmcopy.utils.wallet_balance import get_my_balance
    except Exception as exc:
        logger.warning("execution_coordinator_unwired error=%r", exc)
        return None
    if reservations_repo is None:
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
        # Task 24 fix round 2 — the copier open-exposure cap is enforced
        # authoritatively inside the checked BUY reservation transaction.
        open_exposure_cap=open_exposure_cap,
        confirmed_exposure_reader=confirmed_exposure_reader,
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
        positions_repo=positions_repo,
        execution_service=execution_service,
        position_coordinator=position_coordinator,
        latency=latency,
        copy_admission=copy_admission,
        position_drift_service=position_drift_service,
    )
    from pmcopy.services.execution.leader_sell_target_service import (
        LeaderSellTargetService,
        PersistedFollowerPositionReader,
    )
    from pmcopy.storage.postgres.leader_sell_target_repo import LeaderSellTargetRepo

    coord.leader_sell_target_service = LeaderSellTargetService(
        repository=LeaderSellTargetRepo(pool),
        position_reader=PersistedFollowerPositionReader(positions_repo),
        coordinator=coord,
    )
    return coord


def _bind_reservation_runtime(context: TradingContext, coord: object | None) -> None:
    """Expose the coordinator's reservation service via typed ``TradingContext`` fields."""
    if coord is None:
        return

    reservation_service = getattr(coord, "reservation_service", None)
    if reservation_service is None:
        return

    reservations_repo = getattr(coord, "reservations_repo", None) or getattr(
        reservation_service, "reservations_repo", None
    )
    context.reservation_service = reservation_service
    if reservations_repo is not None:
        context.reservations_repo = reservations_repo
    target_service = getattr(coord, "leader_sell_target_service", None)
    if target_service is not None:
        context.leader_sell_target_service = target_service

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
            from pmcopy.orchestrators.pipeline.trade_pipeline import get_reconciliation_service
            from pmcopy.services.positions.position_action_coordinator import PositionKey

            scope = PositionKey(
                user_id=str(record["user_id"]),
                condition_id=str(record["condition_id"]),
                token_id=str(record["asset"]),
            )
            pos_coord = getattr(context, "position_coordinator", None)
            if pos_coord is not None and pos_coord.is_locked(scope):
                return False

            reconciler = get_reconciliation_service(context)
            req_payload = record.get("request")
            signal_id = (
                str(req_payload.get("signal_id") or "") if isinstance(req_payload, dict) else ""
            )
            is_risk_exit = side == "SELL" and (
                signal_id.startswith("risk:")
                or str(record.get("trade_key") or "").startswith("risk:")
            )

            async def _run_reconcile() -> bool:
                if is_risk_exit:
                    return await reconciler.reconcile_risk_record(
                        record,
                        user_id=str(record["user_id"]),
                        condition_id=str(record["condition_id"]),
                        token_id=str(record["asset"]),
                        confirm_fill=reconciler.confirm_fill,
                        pnl_tracker=context.pnl_tracker,
                        get_position=context.positions_repo.get_position,
                        logger=logger,
                    )
                return await reconciler.reconcile_trade_record(
                    record,
                    side,
                    confirm_fill=reconciler.confirm_fill,
                    pnl_tracker=context.pnl_tracker,
                    get_position=context.positions_repo.get_position,
                    logger=logger,
                    balance_regime=getattr(context, "balance_regime", None),
                )

            if pos_coord is not None:
                if pos_coord.is_locked(scope):
                    return False
                async with pos_coord.acquire(scope):
                    applied = await _run_reconcile()
            else:
                applied = await _run_reconcile()
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
            from pmcopy.orchestrators.pipeline.trade_pipeline import reconcile_submitted_unconfirmed

            trade_n = await reconcile_submitted_unconfirmed(
                context,
                max_age_seconds=0,
                skip_locked_scopes=True,
            )
            risk_n = await reconcile_unconfirmed_risk_orders(
                context,
                skip_locked_scopes=True,
            )
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

    exec_state = resolve_account_execution_state(context)
    if (
        not exec_state.reconciler_callback_bound
        or getattr(reservation_service, "reconciler_callback", None) is None
    ):
        reservation_service.reconciler_callback = _reconcile_expired_reservation
        exec_state.reconciler_callback_bound = True


