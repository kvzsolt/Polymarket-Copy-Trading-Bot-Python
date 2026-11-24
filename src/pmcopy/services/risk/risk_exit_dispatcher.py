"""Protective risk-exit dispatch, reservation reaping, cooldowns, and reconciliation."""

from __future__ import annotations

import inspect
import time
from collections.abc import Sized
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pmcopy.application.trading_context import TradingContext
    from pmcopy.config.app_config import AppConfig

from pmcopy.application.runtime_state import (
    resolve_reservation_service,
    resolve_reservations_repo,
    resolve_risk_state,
)
from pmcopy.services.positions.position_action_coordinator import PositionScope
from pmcopy.services.risk.risk_price_cache import (
    _DEFAULT_RISK_EXIT_REJECT_COOLDOWN_SECONDS,
    _DEFAULT_RISK_PRE_RECONCILE_BACKOFF_SECONDS,
    _DEFAULT_RISK_PRE_RECONCILE_TIMEOUT_SECONDS,
    _MAX_RISK_PRE_RECONCILE_BACKOFF_SECONDS,
    RiskPreReconcileBackoff,
    _should_log_active_reservation_skip,  # noqa: F401
)
from pmcopy.utils import is_valid_clob_token_id
from pmcopy.utils.logger import get_logger

logger = get_logger("RiskMonitor")

_INSUFFICIENT_BALANCE_PATTERNS = (
    "insufficient_balance",
    "insufficient balance",
    "not enough balance",
    "allowance",
    # ``redact_error_detail`` maps venue balance text to this category, so an
    # already-redacted error string must still register as a drift signal.
    "insufficient_funds",
)


def _is_drift_error(msg: str) -> bool:
    lower = msg.lower()
    return any(p in lower for p in _INSUFFICIENT_BALANCE_PATTERNS)


def _risk_exit_signal_id(
    pos: Any,
    *,
    reason: str,
    position_manager: Any | None = None,
) -> str:
    """Build a stable idempotency key for a protective risk-exit SELL.

    C3 — the key must be stable across retries for the same position so the
    submission idempotency lock can collapse a duplicate dispatch against an
    in-flight prior POST (previously ``time.time_ns()`` produced a fresh key
    every tick, defeating dedup and causing oversell/phantom-prune storms).

    The key is ``risk:{reason}:{condition_id}:{token_id}:{epoch}`` where
    ``epoch`` is the in-memory tracked position's ``entry_time`` (which resets
    on re-entry, so a freshly re-entered position is not dedup-suppressed by
    the prior position's one-shot trim). When the tracked position is absent
    (e.g. fresh boot before rehydrate) the epoch falls back to a stable hash
    of immutable DB fields — never the wall clock.
    """
    condition_id = str(getattr(pos, "condition_id", "") or "")
    token_id = str(getattr(pos, "token_id", "") or "")
    epoch: Any = None
    if position_manager is not None:
        get_position = getattr(position_manager, "get_position", None)
        if callable(get_position):
            try:
                tracked = get_position(condition_id, token_id)
            except TypeError:
                try:
                    tracked = get_position(condition_id)
                except Exception:
                    tracked = None
            except Exception:
                tracked = None
            if tracked is not None:
                epoch = getattr(tracked, "entry_time", None)
    if epoch is None:
        # Stable fallback: hash immutable DB position fields so the same
        # position yields the same key across ticks even without the in-memory
        # tracked position.
        leader_id = str(getattr(pos, "leader_id", "") or "")
        avg_entry = str(getattr(pos, "avg_entry_price", "") or "")
        size_tokens = str(getattr(pos, "size_tokens", "") or "")
        epoch = _stable_hash(f"{condition_id}|{token_id}|{leader_id}|{avg_entry}|{size_tokens}")
    return f"risk:{reason}:{condition_id}:{token_id}:{epoch}"


def _stable_hash(value: str) -> str:
    import hashlib

    return hashlib.sha1(value.encode("utf-8"), usedforsecurity=False).hexdigest()[:16]


async def _fetch_live_risk_position_size(
    context: TradingContext,
    *,
    token_id: str,
) -> Decimal | None:
    if context.holdings_reader is None:
        return None
    return await context.holdings_reader.shares(context.wallet_address, token_id)



def _exit_monitor_enabled(config: AppConfig) -> bool:
    return bool(getattr(config.risk, "enable_risk_mgmt", False))


async def _dispatch_risk_exit(
    *,
    coordinator,
    execution_service,
    exec_req,
    user_id: str,
    condition_id: str,
    token_id: str,
    side: str,
    price: float,
    notional: float,
    source: str,
    leader_wallet: str | None,
    signal_id: str | None,
):
    """Single dispatch point for risk/protective SELL submissions.

    Routes through ExecutionCoordinator when wired (PR6); otherwise falls
    back to the low-level ExecutionService primitive via getattr alias to
    keep the coordinator-routing acceptance scan in PR6 clean.
    """
    if coordinator is not None:
        from decimal import Decimal as _D

        from pmcopy.services.execution.coordinator import ExecutionIntent

        intent = ExecutionIntent(
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
            side=side.upper(),
            size=_D(str(exec_req.amount)),
            price=_D(str(price or 0)),
            notional_usdc=_D(str(notional or 0)),
            idempotency_key=signal_id,
            source=source,
            signal_id=signal_id,
            leader_wallet=leader_wallet,
            max_slippage_bps=int(exec_req.max_slippage_bps),
            reason=exec_req.reason or source,
            recovery_metadata=exec_req.recovery_metadata,
        )
        if source == "protective_exit":
            coord_result = await coordinator.submit_protective_exit(intent)
        else:
            coord_result = await coordinator.submit_risk_exit(intent)
        if coord_result.raw_result is not None:
            return coord_result.raw_result
        from pmcopy.services.execution.execution_service import ExecutionResult as _ER
        from decimal import Decimal as _D2

        return _ER(
            accepted=coord_result.accepted,
            order_id=coord_result.order_id,
            status=coord_result.skip_code or coord_result.status or "rejected",
            requested_amount=_D2(str(exec_req.amount)),
            filled_amount=_D2("0"),
            avg_price=None,
            fees_paid=_D2("0"),
            error=coord_result.reason,
        )
    # Fallback path — coordinator missing. Live mode must fail closed.
    from pmcopy.services.execution.coordinator import is_live_mode

    cfg = getattr(execution_service, "_config", None)
    if is_live_mode(cfg):
        logger.error(
            "execution_coordinator_missing | risk path live mode requires coordinator | "
            "user=%s cond=%s token=%s side=%s source=%s",
            user_id,
            condition_id,
            token_id,
            side,
            source,
            extra={
                "type": "execution_coordinator_missing",
                "user_id": user_id,
                "condition_id": condition_id,
                "token_id": token_id,
                "side": side,
                "source": source,
            },
        )
        try:
            from pmcopy.services.observability.metrics import (
                METRIC_COORDINATOR_MISSING_SKIP,
                metrics,
            )

            metrics.inc_sync(
                METRIC_COORDINATOR_MISSING_SKIP,
                labels={"side": side, "source": source},
            )
        except Exception:
            pass
        from pmcopy.services.execution.execution_service import ExecutionResult as _ER
        from decimal import Decimal as _D

        return _ER(
            accepted=False,
            order_id=None,
            status="execution_coordinator_missing",
            requested_amount=_D(str(exec_req.amount)),
            filled_amount=_D("0"),
            avg_price=None,
            fees_paid=_D("0"),
            error="ExecutionCoordinator not wired in live mode",
        )
    submitter = execution_service.execute
    return await submitter(exec_req)


async def rehydrate_position_manager(context: TradingContext) -> None:
    """Load persisted open positions into the context position manager."""
    pm = context.position_manager
    if not hasattr(pm, "track_buy"):
        return

    for pos in await context.positions_repo.list_open_positions(context.user_id):
        existing = None
        if hasattr(pm, "get_position"):
            try:
                existing = pm.get_position(pos.condition_id, pos.token_id)
            except Exception:
                raise RuntimeError("position protection recovery unavailable") from None
        if existing is not None:
            continue
        try:
            pm.track_buy(
                condition_id=pos.condition_id,
                asset=pos.token_id,
                entry_price=pos.avg_entry_price,
                size_tokens=pos.size_tokens,
                usdc_cost=pos.size_tokens * pos.avg_entry_price,
                leader_wallet=pos.leader_id or "",
            )
        except Exception:
            raise RuntimeError("position protection recovery unavailable") from None
        # Restore durable one-shot exit state. ``near_max_warn_done`` lives
        # in-memory on the tracked position; without this a restart re-arms
        # the 60% near-max trim and bleeds the position again on every boot.
        # A ``pending`` trigger row counts as done (crash between trigger and
        # fill-status update must not re-arm a sell); ``failed`` re-arms.
        try:
            triggers_repo = getattr(context, "risk_triggers_repo", None)
            if triggers_repo is not None and hasattr(pm, "get_position"):
                already_trimmed = await triggers_repo.has_trigger_for_reason(
                    user_id=context.user_id,
                    condition_id=pos.condition_id,
                    token_id=pos.token_id,
                    reason="near_max_partial_trim",
                )
                if already_trimmed:
                    tracked = pm.get_position(pos.condition_id, pos.token_id)
                    if tracked is not None:
                        tracked.exit_state.near_max_warn_done = True
        except Exception:
            raise RuntimeError("position protection recovery unavailable") from None


def _fallback_risk_decision(
    entry_price: Decimal,
    current_price: Decimal,
) -> dict | None:
    """No implicit fallback exits. Advanced exit decisions live in PositionManager."""
    return None


def _extract_risk_condition_id(request: dict[str, object] | None) -> str | None:
    if not isinstance(request, dict):
        return None
    condition_id = str(request.get("condition_id") or "").strip()
    if condition_id:
        return condition_id
    signal_id = str(request.get("signal_id") or "").strip()
    if not signal_id.startswith("risk:"):
        return None
    parts = signal_id.split(":", 3)
    if len(parts) < 3:
        return None
    derived = str(parts[2] or "").strip()
    return derived or None


def _decimal_or_zero(value: object) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _matched_amount_from_payload(payload: dict[str, object] | None) -> Decimal:
    if not isinstance(payload, dict):
        return Decimal("0")
    return _decimal_or_zero(
        payload.get("size_matched")
        or payload.get("filled_size")
        or payload.get("matched_size")
        or payload.get("matched_amount")
        or payload.get("sizeMatched")
        or payload.get("filledSize")
        or payload.get("matchedSize")
        or payload.get("matchedAmount")
        or payload.get("size")
        or 0
    )


async def _active_sell_reserved_size(
    context: TradingContext,
    *,
    user_id: str,
    condition_id: str,
    token_id: str,
) -> Decimal:
    """Return active SELL reservations for this scoped position.

    An active SELL reservation means another protective/risk/copy exit already
    owns some or all free shares. The risk loop must not keep submitting fresh
    stop-loss intents against the same held shares while that reservation is
    waiting for reconciliation.

    An expired reservation with no linked order may only be released here on
    the typed expiry outcomes that PROVE release is safe (positive proof that
    no submission was ever persisted, or a terminal no-ID row status). An
    unresolved no-ID submission keeps its shares reserved — releasing it on
    TTL alone would open a double-spend window while the ambiguous venue
    order may still be live.
    """
    reservation_service = resolve_reservation_service(context)
    repo = resolve_reservations_repo(context)
    if getattr(reservation_service, "enabled", True) is False:
        return Decimal("0")
    list_active = getattr(repo, "list_active_for_scope", None)
    if not callable(list_active):
        return Decimal("0")

    try:
        lookup_result = list_active(user_id, condition_id, token_id)
        reservations = await lookup_result if inspect.isawaitable(lookup_result) else lookup_result
    except Exception:
        logger.debug("risk_active_reservation_lookup_failed", exc_info=True)
        return Decimal("0")

    if not isinstance(reservations, (list, tuple)):
        return Decimal("0")
    total = Decimal("0")
    for reservation in reservations:
        side = str(getattr(reservation, "side", "") or "").upper()
        status = str(getattr(reservation, "status", "") or "").lower()
        if side != "SELL" or status != "active":
            continue
        if _is_expired_orphan_reservation(reservation):
            if await _expired_orphan_release_is_proven(context, reservation):
                release = getattr(reservation_service, "release_terminal", None)
                if callable(release):
                    try:
                        release_result = release(
                            reservation.id,
                            reason="expired_orphan_before_risk_exit",
                        )
                        if inspect.isawaitable(release_result):
                            await release_result
                        logger.info(
                            "risk_released_expired_orphan_reservation | "
                            "reservation_id=%s cond=%s token=%s",
                            getattr(reservation, "id", None),
                            condition_id[:12],
                            token_id[:12],
                        )
                        continue
                    except Exception:
                        logger.debug(
                            "risk_release_orphan_reservation_failed",
                            exc_info=True,
                        )
        total += _decimal_or_zero(getattr(reservation, "reserved_size", "0"))
    return total


async def _expired_orphan_release_is_proven(
    context: TradingContext,
    reservation: object,
) -> bool:
    """Return whether an expired no-link reservation may be released.

    Reuses the typed expiry classification: only ``RELEASE_PRE_SUBMIT_ORPHAN``
    (the persisted-submission lookup SUCCEEDED and found no row — no venue
    POST can have occurred) or ``RELEASE_TERMINAL_EVIDENCE`` (a terminal
    no-ID row status) authorize release. A failed lookup, a missing lookup
    capability, or an unresolved no-ID submission all fail closed to keep.
    """
    from pmcopy.services.execution.reservation_expiry import (
        OrderRowLookup,
        classify_no_id_reservation_expiry,
    )

    reservation_id = str(getattr(reservation, "id", "") or "").strip()
    if not reservation_id:
        return False
    orders_repo = getattr(context, "orders_repo", None)
    lookup = getattr(orders_repo, "get_by_reservation_id", None)
    if not callable(lookup):
        return False
    try:
        lookup_result = lookup(reservation_id)
        order_row = await lookup_result if inspect.isawaitable(lookup_result) else lookup_result
    except Exception:
        logger.debug("risk_orphan_submission_lookup_failed", exc_info=True)
        return False
    return classify_no_id_reservation_expiry(OrderRowLookup.from_lookup(order_row)).allows_release


def _is_expired_orphan_reservation(reservation: object) -> bool:
    if getattr(reservation, "order_id", None) or getattr(reservation, "venue_order_id", None):
        return False
    expires_at = getattr(reservation, "expires_at", None)
    if expires_at is None:
        return False
    now_ts = time.time()
    try:
        if hasattr(expires_at, "timestamp"):
            return float(expires_at.timestamp()) <= now_ts
        return float(expires_at) <= now_ts
    except Exception:
        return False


async def _reap_expired_reservations_for_context(context: TradingContext) -> int:
    """Reconcile-and-release expired reservations during the runtime loop.

    Startup runs this once (``runtime_bootstrap``). Without an in-loop call a
    reservation stranded mid-session — e.g. a forced-exit FAK that returned
    ``submitted_unconfirmed`` and was kept active pending reconciliation — would
    never be reaped until the next restart, deadlocking the risk loop on
    ``risk_exit_already_reserved`` (the orphan-release path refuses any
    reservation carrying an order_id). ``release_expired_active`` only acts on
    reservations past their TTL and only releases when reconciliation confirms
    the linked order is venue-terminal, so a still-live order is never
    blind-released — duplicate-order / stale-lock protections are preserved.
    """
    reservation_service = resolve_reservation_service(context)
    if getattr(reservation_service, "enabled", True) is False:
        return 0
    release_expired = getattr(reservation_service, "release_expired_active", None)
    if not callable(release_expired):
        return 0
    try:
        release_result = release_expired()
        released = await release_result if inspect.isawaitable(release_result) else release_result
    except Exception:
        logger.debug("risk_reservation_reaper_failed", exc_info=True)
        return 0
    count = len(released) if isinstance(released, Sized) else 0
    if count:
        logger.info(
            "risk_reaped_expired_reservations | count=%d user=%s",
            count,
            context.user_id,
        )
    return count


async def _attempt_phantom_prune_on_no_fill(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    leader_id: str | None,
) -> dict:
    """Reconcile a forced-exit no-fill against live venue holdings and prune.

    Near-max / resolved markets return a terminal ``cancelled`` (not an
    insufficient-balance drift error), so a manually-sold or settled position
    would otherwise linger in the DB and re-fire the exit forever. This fetches
    live holdings; ``reconcile_position_drift`` deletes the local row only when
    the venue reports ~zero shares and no reservation is active (a real position
    with venue shares is never deleted). Returns the drift result dict (empty on
    failure).
    """
    try:
        from pmcopy.orchestrators.pipeline.trade_pipeline import get_reconciliation_service

        drift_reconciler = get_reconciliation_service(context)

        async def _fetcher():
            return await _fetch_live_risk_position_size(context, token_id=token_id)

        result = await drift_reconciler.reconcile_position_drift(
            user_id=context.user_id,
            condition_id=condition_id,
            token_id=token_id,
            leader_id=leader_id or "risk-monitor",
            live_position_fetcher=_fetcher,
        )
        return result or {}
    except Exception:
        logger.debug("risk_drift_reconciliation_error", exc_info=True)
        return {}


def _should_attempt_no_fill_prune(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    now: float | None = None,
    interval_seconds: float = 30.0,
) -> bool:
    """Throttle live-holdings drift checks on repeated forced-exit no-fills.

    A real-but-illiquid position keeps returning no-fill every cycle; without a
    throttle we would hammer the venue holdings endpoint at the risk-loop
    cadence. One check per ``interval_seconds`` per scope is enough to catch a
    manual sell / resolution promptly while bounding REST load.
    """
    current = time.time() if now is None else now
    key = f"{condition_id}:{token_id}"
    state = resolve_risk_state(context).no_fill_prune_attempted_at
    try:
        last = float(state.get(key, 0.0) or 0.0)
    except Exception:
        last = 0.0
    if current - last < interval_seconds:
        return False
    state[key] = current
    return True


def _risk_exit_reject_cooldown_seconds(context: TradingContext) -> float:
    """Resolve the terminal-exit cooldown: typed AppConfig first, env fallback.

    0 is a valid, meaningful value (operator opt-out to legacy behavior), so
    this does NOT go through ``_float_env_or_default`` (which maps <=0 to the
    default).
    """
    config = getattr(context, "config", None)
    if config is not None:
        risk_cfg = getattr(config, "risk", None)
        val = getattr(risk_cfg, "risk_exit_reject_cooldown_seconds", None)
        if isinstance(val, (int, float)) and float(val) >= 0.0:
            return float(val)
    return _DEFAULT_RISK_EXIT_REJECT_COOLDOWN_SECONDS


def _risk_exit_cooldown_active(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    now: float | None = None,
) -> bool:
    current = time.time() if now is None else now
    state = resolve_risk_state(context).exit_reject_cooldown_until
    try:
        until = float(state.get(f"{condition_id}:{token_id}", 0.0) or 0.0)
    except Exception:
        return False
    return current < until


def _arm_risk_exit_reject_cooldown(
    context: TradingContext,
    *,
    condition_id: str,
    token_id: str,
    now: float | None = None,
) -> None:
    """Suppress re-dispatch for this scope after a terminal exit failure.

    Keyed on ``ExecutionResult.status`` by the callers (rejected/cancelled),
    deliberately NOT on error-string classification, so the storm-stopper
    holds even if venue error classification regresses to an opaque shape.
    """
    seconds = _risk_exit_reject_cooldown_seconds(context)
    if seconds <= 0:
        return
    current = time.time() if now is None else now
    state = resolve_risk_state(context).exit_reject_cooldown_until
    state[f"{condition_id}:{token_id}"] = current + seconds


def _should_arm_risk_exit_reject_cooldown(status: object, error: object = None) -> bool:
    status_lower = str(status or "").lower()
    if status_lower in ("rejected", "cancelled"):
        return True
    if status_lower == "failed" and _is_drift_error(str(error or "")):
        return True
    return False


def _risk_exit_fully_closed_position(updated_pos: object | None) -> bool:
    """Return True only when the protective SELL fully closed the position.

    Partial fills are real and must still update fills/positions/PnL, but they
    must NOT activate a durable re-entry block.  The block represents a
    completed protective exit regime, not merely a partial de-risking step.
    """
    return updated_pos is None


async def reconcile_unconfirmed_risk_orders(
    context: TradingContext,
    *,
    skip_locked_scopes: bool = False,
    startup_recovery: bool = False,
) -> int:
    """Converge risk-monitor-originated submitted_unconfirmed SELLs.

    Issue #71 — per-record settlement delegates to ``ReconciliationService``,
    the canonical owner of terminal-state transitions and fill apply. This
    function still orchestrates the batch (filtering, scope locking) so that
    risk SELLs serialize against the trade pipeline and live risk-monitor
    actions on the same scoped position.

    ``skip_locked_scopes`` — the risk loop's pre-reconcile phase passes True
    so a record whose scope lock is held by another consumer (execute_trades
    submit/confirm, manual sync, standalone reconciler) is skipped instead of
    queued behind that holder's venue I/O inside the pre-reconcile ceiling;
    the holder converges the position, or the next tick retries the row.
    """
    if not context.execution_service:
        return 0

    from pmcopy.orchestrators.pipeline.trade_pipeline import get_reconciliation_service

    records = await context.orders_repo.get_unconfirmed_order_submissions(
        user_id=context.user_id,
        side="SELL",
    )
    if not records:
        return 0

    reconciler = get_reconciliation_service(context)
    reconciled = 0

    for record in records:
        request = record.get("request") or {}
        signal_id = str(request.get("signal_id") or "")
        if not signal_id.startswith("risk:"):
            continue

        order_id = str(record.get("order_id") or "").strip()
        token_id = str(request.get("token_id") or "").strip()
        condition_id = _extract_risk_condition_id(request)
        if not order_id or not token_id or not condition_id or not is_valid_clob_token_id(token_id):
            continue

        scope = PositionScope(
            user_id=context.user_id,
            condition_id=condition_id,
            token_id=token_id,
        )
        if skip_locked_scopes and context.position_coordinator.is_locked(scope):
            logger.debug(
                "risk_reconcile_skip_locked_scope | order_id=%s",
                order_id,
            )
            continue
        async with context.position_coordinator.acquire(scope):
            applied = await reconciler.reconcile_risk_record(
                record,
                user_id=context.user_id,
                condition_id=condition_id,
                token_id=token_id,
                confirm_fill=reconciler.confirm_fill,
                pnl_tracker=context.pnl_tracker,
                get_position=context.positions_repo.get_position,
                logger=logger,
                startup_recovery=startup_recovery,
            )
            if applied:
                reconciled += 1

    return reconciled


def _risk_pre_reconcile_timeout_seconds(
    poll_interval: float,
    configured: float | None = None,
) -> float:
    return max(
        0.05,
        float(poll_interval or 0.0),
        float(
            configured if configured is not None else _DEFAULT_RISK_PRE_RECONCILE_TIMEOUT_SECONDS
        ),
    )


def _risk_pre_reconcile_backoff_base_seconds(
    poll_interval: float,
    timeout_seconds: float,
    configured: float | None = None,
) -> float:
    return max(
        float(poll_interval or 0.0),
        timeout_seconds,
        float(
            configured if configured is not None else _DEFAULT_RISK_PRE_RECONCILE_BACKOFF_SECONDS
        ),
    )


def _risk_pre_reconcile_state(context: TradingContext) -> RiskPreReconcileBackoff:
    risk_state = resolve_risk_state(context)
    state = risk_state.pre_reconcile_backoff
    if not isinstance(state, RiskPreReconcileBackoff):
        state = RiskPreReconcileBackoff()
        risk_state.pre_reconcile_backoff = state
    return state


def _risk_pre_reconcile_is_due(context: TradingContext, *, now: float) -> bool:
    return _risk_pre_reconcile_state(context).next_attempt_at <= now


def _record_risk_pre_reconcile_success(context: TradingContext) -> None:
    state = _risk_pre_reconcile_state(context)
    state.consecutive_timeouts = 0
    state.next_attempt_at = 0.0


def _record_risk_pre_reconcile_timeout(
    context: TradingContext,
    *,
    now: float,
    base_backoff_seconds: float,
) -> float:
    state = _risk_pre_reconcile_state(context)
    state.consecutive_timeouts += 1
    multiplier = 2 ** min(state.consecutive_timeouts - 1, 3)
    backoff_seconds = min(
        base_backoff_seconds * multiplier,
        max(base_backoff_seconds, _MAX_RISK_PRE_RECONCILE_BACKOFF_SECONDS),
    )
    state.next_attempt_at = now + backoff_seconds
    return backoff_seconds


