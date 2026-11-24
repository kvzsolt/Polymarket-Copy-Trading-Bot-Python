"""Risk monitor — background task that checks open positions for advanced exit triggers.

Decomposed into ``risk_price_cache`` (price cache, sanity gates, circuit breaker),
``risk_exit_dispatcher`` (exit dispatch, cooldowns, reservation reaping, reconciliation),
and this evaluation/loop facade.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pmcopy.application.trading_context import TradingContext
    from pmcopy.config.app_config import AppConfig
    from pmcopy.services.observability.health_server import ReadinessTracker

from pmcopy.application.runtime_state import (
    resolve_enable_balance_tiered_policy,
    resolve_risk_state,
    resolve_slippage_tolerance_bps,
)
from pmcopy.integrations.polymarket.models import PRICE_TOKEN_NOT_FOUND  # noqa: F401
from pmcopy.services.execution.execution_service import ExecutionRequest  # noqa: F401
from pmcopy.services.observability.metrics import metrics
from pmcopy.services.positions.position_action_coordinator import PositionScope  # noqa: F401
from pmcopy.services.risk.risk_engine import active_policy
from pmcopy.services.risk.risk_exit_dispatcher import (
    _INSUFFICIENT_BALANCE_PATTERNS,  # noqa: F401
    _active_sell_reserved_size,
    _arm_risk_exit_reject_cooldown,
    _attempt_phantom_prune_on_no_fill,
    _decimal_or_zero,  # noqa: F401
    _dispatch_risk_exit,
    _exit_monitor_enabled,
    _expired_orphan_release_is_proven,  # noqa: F401
    _extract_risk_condition_id,  # noqa: F401
    _fallback_risk_decision,
    _fetch_live_risk_position_size,  # noqa: F401
    _is_drift_error,
    _is_expired_orphan_reservation,  # noqa: F401
    _matched_amount_from_payload,  # noqa: F401
    _reap_expired_reservations_for_context,
    _record_risk_pre_reconcile_success,
    _record_risk_pre_reconcile_timeout,
    _risk_exit_cooldown_active,
    _risk_exit_fully_closed_position,
    _risk_exit_reject_cooldown_seconds,  # noqa: F401
    _risk_exit_signal_id,
    _risk_pre_reconcile_backoff_base_seconds,
    _risk_pre_reconcile_is_due,
    _risk_pre_reconcile_timeout_seconds,
    _should_arm_risk_exit_reject_cooldown,
    _should_attempt_no_fill_prune,
    _should_log_active_reservation_skip,
    _stable_hash,  # noqa: F401
    reconcile_unconfirmed_risk_orders,
    rehydrate_position_manager,
)
from pmcopy.services.risk.risk_price_cache import (
    _DEFAULT_CB_COOLDOWN,  # noqa: F401
    _DEFAULT_CB_THRESHOLD,  # noqa: F401
    _DEFAULT_RISK_EXIT_REJECT_COOLDOWN_SECONDS,  # noqa: F401
    _DEFAULT_RISK_PRE_RECONCILE_BACKOFF_SECONDS,  # noqa: F401
    _DEFAULT_RISK_PRE_RECONCILE_TIMEOUT_SECONDS,  # noqa: F401
    _DEFAULT_RISK_PRICE_SEMAPHORE,  # noqa: F401
    _DISABLED_RISK_TELEMETRY,  # noqa: F401
    _DORMANT_TOKEN_TTL_SECONDS,  # noqa: F401
    _MAX_RISK_PRE_RECONCILE_BACKOFF_SECONDS,  # noqa: F401
    _MIN_RISK_CACHE_STALE_SECONDS,  # noqa: F401
    CachedRiskPrice,  # noqa: F401
    RiskPreReconcileBackoff,  # noqa: F401
    RiskPriceCache,  # noqa: F401
    VenueCircuitBreaker,  # noqa: F401
    _cache_market_store_risk_price,  # noqa: F401
    _ensure_context_price_cache,  # noqa: F401
    _ensure_context_venue_circuit_breaker,  # noqa: F401
    _fetch_and_store_price,  # noqa: F401
    _get_context_cached_price,
    _market_wss_live_for_token,  # noqa: F401
    _refresh_context_price_cache,
    _refresh_price_cache_for_tokens,  # noqa: F401
    _risk_cache_stale_after,  # noqa: F401
    _risk_mark_max_abs_distance,  # noqa: F401
    _risk_mark_max_spread_bps,  # noqa: F401
    _risk_mark_passes_sanity,  # noqa: F401
    _risk_telemetry,
    _risk_wss_connection_max_silence,  # noqa: F401
)
from pmcopy.services.telemetry import RiskTelemetryEmitter  # noqa: F401
from pmcopy.storage.postgres.fills_repo import FillRecord  # noqa: F401
from pmcopy.utils import is_valid_clob_token_id  # noqa: F401
from pmcopy.utils.logger import get_logger

logger = get_logger("RiskMonitor")

async def _pre_reconcile_for_context(context: TradingContext) -> int:
    """Reconcile this context's unconfirmed orders. Best-effort; never raises.

    Issue #29 — converge unconfirmed order state BEFORE position evaluation so
    the monitor never acts on stale positions: unconfirmed SELLs that already
    filled on-exchange would otherwise still look open (→ duplicate protective
    SELLs), and unconfirmed BUYs that filled would be invisible (→ unprotected).
    Returns the number of orders reconciled (0 on error).

    Hoisted into its own function so the risk loop can run it OUTSIDE the
    per-cycle protective-exit timeout budget — these are venue order-status
    round-trips and must not starve the stop-loss evaluation every cycle
    (sym 1: "Risk loop TIMEOUT ... context skipped this cycle").
    """
    if not getattr(context, "execution_service", None):
        return 0
    try:
        from pmcopy.orchestrators.pipeline.trade_pipeline import reconcile_submitted_unconfirmed

        # skip_locked_scopes: this phase is best-effort and runs under the
        # pre-reconcile wait_for ceiling. Blocking on a scope lock held by an
        # in-flight execute_trades submit/confirm or manual sync (both do venue
        # round-trips inside the lock) burned the whole ceiling and produced
        # the recurring "Risk pre-reconcile TIMEOUT" ops spam; the lock holder
        # (or the next tick / standalone reconciler) converges skipped rows.
        trade_n = await reconcile_submitted_unconfirmed(context, skip_locked_scopes=True)
        risk_n = await reconcile_unconfirmed_risk_orders(context, skip_locked_scopes=True)
        total_n = trade_n + risk_n
        if total_n:
            logger.info(
                "risk_monitor_pre_reconcile | reconciled=%d unconfirmed orders (trade=%d risk=%d)",
                total_n,
                trade_n,
                risk_n,
            )
        return total_n
    except Exception as exc:
        logger.warning("risk_monitor_pre_reconcile_error | %s", repr(exc))
        return 0


async def run_risk_check_for_context(
    context: TradingContext, *, pre_reconcile: bool = True
) -> None:
    """Run one follower risk evaluation through injected services."""
    if not context.execution_service:
        return

    # Issue #29 — reconcile unconfirmed orders BEFORE rehydration / position
    # evaluation. Direct callers (and tests) keep this inline by default; the
    # risk loop passes ``pre_reconcile=False`` because it runs the reconcile in
    # its own untimed phase so slow venue round-trips never consume the
    # protective-exit timeout budget (sym 1).
    if pre_reconcile:
        await _pre_reconcile_for_context(context)

    if context.task_position_manager is not None:
        try:
            await context.task_position_manager.run_once()
        except Exception as exc:
            logger.warning(
                "risk_monitor_task_protective_error | user=%s error=%r",
                context.user_id,
                exc,
            )
    await rehydrate_position_manager(context)
    risk_state = resolve_risk_state(context)
    slippage_bps = resolve_slippage_tolerance_bps(context, 200)
    refresh_interval = float(risk_state.check_interval_seconds or 0.25)

    enable_tiered = resolve_enable_balance_tiered_policy(context)

    warned_tokens: set[str] = risk_state.warned_invalid_tokens
    for pos in await context.positions_repo.list_open_positions(context.user_id):
        if not is_valid_clob_token_id(pos.token_id):
            if pos.token_id not in warned_tokens:
                warned_tokens.add(pos.token_id)
                logger.warning(
                    "Skipping position with invalid token_id=%r (condition=%s) — "
                    "likely stale placeholder; close or delete manually",
                    pos.token_id,
                    pos.condition_id,
                )
            continue
        # --- Per-scoped-position action coordination (issue #28) ---
        _scope = PositionScope(
            user_id=context.user_id,
            condition_id=pos.condition_id,
            token_id=pos.token_id,
        )
        if context.position_coordinator.is_locked(_scope):
            logger.info(
                "risk_context_scope_busy_skipped | user=%s condition=%s token=%s",
                context.user_id,
                pos.condition_id[:12],
                pos.token_id[:12],
            )
            continue
        async with context.position_coordinator.acquire(_scope):
            try:
                current_price = await _get_context_cached_price(
                    context,
                    pos.token_id,
                    refresh_interval=refresh_interval,
                )
                if current_price is None or current_price <= 0:
                    continue

                action = None
                if hasattr(context.position_manager, "evaluate_risk_action"):
                    try:
                        # Decimal-internal: evaluate_risk_action normalises
                        # any numeric input via ``_to_dec`` so callers no
                        # longer have to round-trip through float at the
                        # boundary (Phase 2 / CLAUDE.md no-float-money).
                        action = context.position_manager.evaluate_risk_action(
                            pos.condition_id,
                            pos.token_id,
                            current_price,
                        )
                    except TypeError:
                        action = context.position_manager.evaluate_risk_action(
                            pos.condition_id,
                            current_price,
                        )
                if not action:
                    action = _fallback_risk_decision(pos.avg_entry_price, current_price)
                if not action:
                    continue

                fraction = max(0.0, min(1.0, float(action.get("size_fraction", 1.0) or 1.0)))
                reason = str(action.get("reason") or "risk_exit")
                trigger_notional = float(
                    (Decimal(str(pos.size_tokens)) * Decimal(str(current_price))).quantize(
                        Decimal("0.00000001")
                    )
                )

                # Capture block metadata at trigger time — but defer persisting the
                # risk block until after the protective SELL actually fills.  Writing
                # the block before execution creates a durable re-entry ban even when
                # the SELL fails, which is the core issue #27 lifecycle bug.
                _block_metadata: dict[str, object] = {
                    "leader_notional_at_trigger": trigger_notional,
                    "copied_position_notional_at_trigger": trigger_notional,
                    "trigger_price": float(current_price),
                    "trigger_size_tokens": float(pos.size_tokens),
                    "trigger_fraction": fraction,
                }

                # Simplified account-cap policy
                if fraction < 1.0 and enable_tiered:
                    policy = active_policy()
                    if not policy.allow_partial_exits:
                        logger.info(
                            "Balance policy [%s] upgrades partial exit → full for %s (was %.0f%%)",
                            policy.name,
                            pos.condition_id[:12],
                            fraction * 100,
                        )
                        fraction = 1.0

                _block_metadata["trigger_fraction"] = fraction

                sell_size = (pos.size_tokens * Decimal(str(fraction))).quantize(
                    Decimal("0.00000001")
                )
                if sell_size <= 0:
                    continue

                active_reserved = await _active_sell_reserved_size(
                    context,
                    user_id=context.user_id,
                    condition_id=pos.condition_id,
                    token_id=pos.token_id,
                )
                if active_reserved > 0:
                    if _should_log_active_reservation_skip(
                        context,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                        reason=reason,
                    ):
                        logger.info(
                            "risk_exit_already_reserved | cond=%s token=%s reason=%s "
                            "reserved=%s requested=%s",
                            pos.condition_id[:12],
                            pos.token_id[:12],
                            reason,
                            active_reserved,
                            sell_size,
                        )
                    continue

                if _risk_exit_cooldown_active(
                    context,
                    condition_id=pos.condition_id,
                    token_id=pos.token_id,
                ):
                    # Recent terminal rejection for this scope — skip before
                    # record_trigger so the DB is not spammed either.
                    logger.debug(
                        "risk_exit_reject_cooldown_active | cond=%s token=%s reason=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                    )
                    continue

                _trigger_id: int | None = None
                if context.risk_triggers_repo is not None:
                    try:
                        _trigger_id = await context.risk_triggers_repo.record_trigger(
                            user_id=context.user_id,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                            reason=reason,
                            source="risk_monitor",
                            fill_status="pending",
                            metadata=_block_metadata,
                        )
                    except Exception:
                        logger.debug("risk_trigger_record_error", exc_info=True)
                _risk_telemetry(context).exit_triggered(pos.condition_id, pos.token_id, reason)

                req = ExecutionRequest(
                    user_id=context.user_id,
                    leader_id=pos.leader_id or "risk-monitor",
                    token_id=pos.token_id,
                    side="SELL",
                    amount=sell_size,
                    max_slippage_bps=slippage_bps,
                    reason=reason,
                    signal_id=_risk_exit_signal_id(
                        pos,
                        reason=reason,
                        position_manager=getattr(context, "position_manager", None),
                    ),
                    condition_id=pos.condition_id,
                    recovery_metadata=_block_metadata,
                )
                # C4 — shield the protective-exit POST from the outer
                # ``wait_for`` ceiling so a loop timeout cannot cancel the
                # venue POST mid-flight (which previously left a live venue
                # order with no local record, then C3 re-fired a duplicate).
                # The shielded task completes its POST + persistence even if
                # the parent ``wait_for`` raises TimeoutError; the stable
                # idempotency key (C3) collapses any duplicate the next tick.
                result = await asyncio.shield(
                    _dispatch_risk_exit(
                        coordinator=getattr(context, "execution_coordinator", None),
                        execution_service=context.execution_service,
                        exec_req=req,
                        user_id=context.user_id,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                        side="SELL",
                        price=float(current_price) if current_price is not None else 0.0,
                        notional=float(
                            Decimal(str(pos.size_tokens)) * Decimal(str(current_price or 0))
                        ),
                        source="risk_exit",
                        leader_wallet=pos.leader_id,
                        signal_id=req.signal_id,
                    )
                )
                if not result.accepted:
                    logger.info(
                        "risk_exit_sell_not_filled | cond=%s token=%s reason=%s status=%s error=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                        result.status,
                        result.error,
                    )
                    # Phantom-position prune. An explicit insufficient-balance
                    # error is the strong signal, but a resolved/near-max market
                    # returns a terminal ``cancelled`` instead, so also probe
                    # live holdings on a throttled cadence. ``reconcile_position_
                    # drift`` only deletes when the venue reports ~zero shares and
                    # no reservation is active — a real position is never pruned.
                    drift_signal = _is_drift_error(str(result.error or ""))
                    if drift_signal or _should_attempt_no_fill_prune(
                        context,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                    ):
                        drift_result = await _attempt_phantom_prune_on_no_fill(
                            context,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                            leader_id=pos.leader_id,
                        )
                        if (
                            drift_result.get("reconciled")
                            and drift_result.get("action") == "deleted"
                        ):
                            logger.info(
                                "risk_drift_reconciled_position_deleted | cond=%s token=%s live_size=%s",
                                pos.condition_id[:12],
                                pos.token_id[:12],
                                str(drift_result.get("live_size", 0)),
                            )
                            if _trigger_id is not None and context.risk_triggers_repo is not None:
                                try:
                                    await context.risk_triggers_repo.update_fill_status(
                                        _trigger_id,
                                        fill_status="rejected",
                                        fill_order_id=result.order_id,
                                    )
                                except Exception:
                                    logger.debug("risk_trigger_update_error", exc_info=True)
                            continue
                    if _trigger_id is not None and context.risk_triggers_repo is not None:
                        try:
                            await context.risk_triggers_repo.update_fill_status(
                                _trigger_id,
                                fill_status="rejected",
                                fill_order_id=result.order_id,
                            )
                        except Exception:
                            logger.debug("risk_trigger_update_error", exc_info=True)
                    # Terminal statuses and failed insufficient-balance/allowance
                    # rejections arm cooldown. Plain transient failures keep the
                    # normal retry cadence.
                    if _should_arm_risk_exit_reject_cooldown(
                        result.status,
                        result.error,
                    ):
                        _arm_risk_exit_reject_cooldown(
                            context,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                        )
                    continue
                if result.filled_amount <= 0 and result.status in ("submitted_unconfirmed",):
                    logger.info(
                        "risk_exit_submitted_unconfirmed | cond=%s token=%s reason=%s order_id=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                        result.order_id,
                    )
                    if _trigger_id is not None and context.risk_triggers_repo is not None:
                        try:
                            await context.risk_triggers_repo.update_fill_status(
                                _trigger_id,
                                fill_status="pending",
                                fill_order_id=result.order_id,
                            )
                        except Exception:
                            logger.debug("risk_trigger_update_error", exc_info=True)
                    continue
                if result.filled_amount <= 0:
                    logger.info(
                        "risk_exit_sell_not_filled | cond=%s token=%s reason=%s status=%s error=%s",
                        pos.condition_id[:12],
                        pos.token_id[:12],
                        reason,
                        result.status,
                        result.error,
                    )
                    _risk_telemetry(context).exit_not_filled(pos.condition_id, pos.token_id, reason)
                    # Terminal accepted-but-zero-fill (e.g. a fully-killed FAK on
                    # a resolved/illiquid market). Probe live holdings — if the
                    # shares are gone, prune the phantom so the loop stops.
                    if _should_attempt_no_fill_prune(
                        context,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                    ):
                        drift_result = await _attempt_phantom_prune_on_no_fill(
                            context,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                            leader_id=pos.leader_id,
                        )
                        if (
                            drift_result.get("reconciled")
                            and drift_result.get("action") == "deleted"
                        ):
                            logger.info(
                                "risk_drift_reconciled_position_deleted | cond=%s token=%s live_size=%s",
                                pos.condition_id[:12],
                                pos.token_id[:12],
                                str(drift_result.get("live_size", 0)),
                            )
                    if _trigger_id is not None and context.risk_triggers_repo is not None:
                        try:
                            await context.risk_triggers_repo.update_fill_status(
                                _trigger_id,
                                fill_status="rejected",
                                fill_order_id=result.order_id,
                            )
                        except Exception:
                            logger.debug("risk_trigger_update_error", exc_info=True)
                    # Accepted-but-zero-fill is terminal for this attempt (fully
                    # killed FAK) — same 1 Hz refire shape as a rejection.
                    _arm_risk_exit_reject_cooldown(
                        context,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                    )
                    continue

                fill_size = Decimal(str(result.filled_amount))
                fill_price = Decimal(str(result.avg_price or current_price))

                # Persist risk block only when the protective SELL fully closes
                # the scoped position. A partial fill is real and should be
                # accounted for, but it must not create a durable re-entry ban.
                _block_metadata["fill_price"] = float(fill_price)
                _block_metadata["fill_size_tokens"] = float(fill_size)
                _block_metadata["fill_order_id"] = result.order_id

                if _trigger_id is not None and context.risk_triggers_repo is not None:
                    try:
                        await context.risk_triggers_repo.update_fill_status(
                            _trigger_id,
                            fill_status="filled",
                            fill_order_id=result.order_id,
                        )
                    except Exception:
                        logger.debug("risk_trigger_update_error", exc_info=True)

                # Concurrent-run dedup guard — PR #108 P1 finding.
                #
                # The ``idx_fills_dedup`` index keys on
                # ``(user_id, order_id, condition_id, side, fill_ts)``.
                # ``fill_ts`` is the only field that varies between two
                # concurrent ``run_risk_check_for_context`` invocations
                # for the same protective SELL (wall-clock T vs T+1), so
                # the unique index does NOT collapse them — both inserts
                # succeed with distinct ``fill_id`` values, both call
                # ``_apply_fill_in_conn`` (the second on size=0 because
                # the first already closed the position), the second
                # delete is a no-op, position state ends correct, but
                # the fills table now holds TWO rows for ONE exchange
                # fill. Downstream PnL replay (e.g. ``reconcile_trade_
                # record``) double-counts.
                #
                # Anchor dedup to the exchange-sourced ``result.order_id``
                # via ``try_claim_processed_trade`` — the same pattern
                # the trade pipeline uses upstream of its fill insert.
                # Only the winning coroutine reaches ``append_fill_and_
                # apply``; the loser bails out before mutating fills /
                # positions / blocks / PnL.
                risk_trade_key = (
                    f"risk_exit:{context.user_id}:{pos.condition_id}:"
                    f"{pos.token_id}:{result.order_id}"
                )
                claimed = await context.orders_repo.try_claim_processed_trade(
                    trade_key=risk_trade_key,
                    user_id=context.user_id,
                    leader_id=pos.leader_id,
                    tx_hash=None,
                    side="SELL",
                    asset=pos.token_id,
                    condition_id=pos.condition_id,
                    status="in_progress",
                )
                if not claimed:
                    logger.info(
                        "risk_exit_concurrent_claim_lost | user=%s condition=%s token=%s order=%s",
                        context.user_id,
                        pos.condition_id,
                        pos.token_id,
                        result.order_id,
                    )
                    continue

                # Freeze ``fill_ts`` once per claim so any retry within
                # this coroutine reuses the same timestamp the unique
                # index keys on, instead of re-sampling ``time.time()``.
                _fill_ts = int(time.time())

                # Atomic fill insert + position update — closes the
                # crash-window the protective-SELL path had between the
                # two independent pool connections. ``append_fill_and_
                # apply`` also short-circuits on duplicate fills (the
                # ON CONFLICT DO NOTHING branch returns fill_id=0 and
                # skips ``apply_fill``), preventing the double-count
                # regression flagged in PR #108.
                try:
                    (
                        fill_id,
                        updated_pos,
                    ) = await context.fills_repo.append_fill_and_apply(
                        FillRecord(
                            user_id=context.user_id,
                            leader_id=pos.leader_id,
                            order_id=result.order_id,
                            condition_id=pos.condition_id,
                            token_id=pos.token_id,
                            side="SELL",
                            size_tokens=fill_size,
                            price=fill_price,
                            fee=(
                                None if result.fees_paid is None else Decimal(str(result.fees_paid))
                            ),
                            fill_ts=_fill_ts,
                        ),
                        context.positions_repo,
                    )
                except Exception:
                    # Release the claim so a retry can re-attempt rather
                    # than tombstoning the order_id behind a stale
                    # in_progress row.
                    try:
                        await context.orders_repo.mark_processed_trade(
                            trade_key=risk_trade_key,
                            user_id=context.user_id,
                            leader_id=pos.leader_id,
                            tx_hash=None,
                            side="SELL",
                            asset=pos.token_id,
                            condition_id=pos.condition_id,
                            status="retryable_failed",
                        )
                    except Exception:
                        logger.debug("risk_exit_claim_release_failed", exc_info=True)
                    raise
                if fill_id == 0:
                    # Duplicate protective SELL — fill was already persisted
                    # on a prior call. Risk block, PnL, and position-manager
                    # state were updated then; replaying them would create a
                    # false-positive durable re-entry block (the duplicate
                    # path returns updated_pos=None which would otherwise
                    # trip ``_risk_exit_fully_closed_position`` → True) and
                    # double-count realised PnL.
                    logger.info(
                        "risk_exit_duplicate_fill_ignored | user=%s condition=%s token=%s order=%s",
                        context.user_id,
                        pos.condition_id,
                        pos.token_id,
                        result.order_id,
                    )
                    await context.orders_repo.mark_processed_trade(
                        trade_key=risk_trade_key,
                        user_id=context.user_id,
                        leader_id=pos.leader_id,
                        tx_hash=None,
                        side="SELL",
                        asset=pos.token_id,
                        condition_id=pos.condition_id,
                        status="duplicate_ignored",
                    )
                    continue
                if _risk_exit_fully_closed_position(updated_pos):
                    await context.risk_blocks_repo.mark_risk_block(
                        user_id=context.user_id,
                        condition_id=pos.condition_id,
                        token_id=pos.token_id,
                        reason=reason,
                        source="risk_monitor",
                        leader_anchor=pos.leader_id,
                        metadata=_block_metadata,
                    )
                else:
                    logger.info(
                        "risk_exit_partial_fill_no_block | user=%s condition=%s token=%s remaining=%s",
                        context.user_id,
                        pos.condition_id,
                        pos.token_id,
                        getattr(updated_pos, "size_tokens", None),
                    )
                if hasattr(context.pnl_tracker, "record_sell"):
                    # Realised PnL in Decimal — only round-trip through
                    # float at the pnl_tracker boundary (the tracker's
                    # public API still types these as float).
                    entry_price_d = Decimal(str(pos.avg_entry_price))
                    fill_price_d = Decimal(str(fill_price))
                    fill_size_d = Decimal(str(fill_size))
                    realized_d = (fill_price_d - entry_price_d) * fill_size_d
                    usdc_value_d = fill_size_d * fill_price_d
                    context.pnl_tracker.record_sell(
                        condition_id=pos.condition_id,
                        price=float(fill_price_d),
                        size_tokens=float(fill_size_d),
                        usdc_value=float(usdc_value_d),
                        entry_price=float(entry_price_d),
                        realized_pnl=float(realized_d),
                        exit_reason=reason,
                        leader_wallet=pos.leader_id or "",
                    )
                metrics.inc(
                    "risk_exits_total",
                    labels={
                        "user_id": context.user_id,
                        "leader_id": str(pos.leader_id or ""),
                        "reason": reason,
                    },
                )
                _risk_telemetry(context).exit_filled(pos.condition_id, pos.token_id, reason)
                if hasattr(context.position_manager, "track_sell"):
                    context.position_manager.track_sell(
                        pos.condition_id,
                        float(fill_price),
                        float(fill_size),
                        asset=pos.token_id,
                        reason=reason,
                    )
                    # NOTE: no manual ``release_scope`` here — the surrounding
                    # ``async with context.position_coordinator.acquire(_scope)``
                    # context manager already releases on exit. (Greptile P2 PR #63.)

                # Settle the dedup claim — any concurrent run that sampled
                # ``time.time()`` one second later and would have inserted
                # a phantom duplicate fill row now bounces off the claim
                # short-circuit instead.
                try:
                    await context.orders_repo.mark_processed_trade(
                        trade_key=risk_trade_key,
                        user_id=context.user_id,
                        leader_id=pos.leader_id,
                        tx_hash=None,
                        side="SELL",
                        asset=pos.token_id,
                        condition_id=pos.condition_id,
                        status="completed",
                    )
                except Exception:
                    logger.debug("risk_exit_claim_complete_failed", exc_info=True)
            except Exception as exc:
                logger.error(
                    "Risk context check failed user=%s condition=%s: %r",
                    context.user_id,
                    pos.condition_id,
                    exc,
                )


async def risk_monitor_loop_for_contexts(
    contexts: list[TradingContext],
    readiness_tracker: ReadinessTracker,
    *,
    config: AppConfig,
    interval: float | None = None,
) -> None:
    """Restart-safe risk monitor using persisted positions as the source of truth."""
    if not contexts:
        logger.warning("Risk monitor started with no contexts")
        return

    enabled = _exit_monitor_enabled(config)
    poll_interval = float(interval or config.risk.risk_check_interval)

    if not enabled:
        logger.info("Global risk management disabled — monitor will not run")
        return

    for context in contexts:
        resolve_risk_state(context).check_interval_seconds = poll_interval

    # Reservation reaping is far cheaper than worthwhile every 0.25s tick;
    # throttle to a coarse cadence. The reaper only releases reservations past
    # their TTL once reconciliation confirms the order is terminal, so a longer
    # cadence than the poll interval is safe and keeps DB load low.
    reap_interval = max(poll_interval * 4, 10.0)
    last_reap = 0.0

    while True:
        readiness_tracker.update(last_risk_check_ts=int(time.time()))

        # Reap stranded reservations BEFORE this cycle's risk checks so a freed
        # reservation lets the same cycle retry the exit or prune the phantom.
        now_ts = time.time()
        if now_ts - last_reap >= reap_interval:
            last_reap = now_ts
            reap_tasks = [_reap_expired_reservations_for_context(context) for context in contexts]
            reap_results = await asyncio.gather(*reap_tasks, return_exceptions=True)
            for context, result in zip(contexts, reap_results, strict=False):
                if isinstance(result, Exception):
                    logger.error(
                        "Risk reservation reaper failed user=%s: %r",
                        context.user_id,
                        result,
                    )

        # Pre-reconcile unconfirmed orders in their OWN phase, ahead of the timed
        # protective-exit check below. These are venue order-status round-trips;
        # keeping them out of the per-context protective-exit budget stops them
        # from starving the stop-loss evaluation while copies settle (sym 1).
        #
        # A timeout here is usually a stuck/slow venue order-status call. Retrying
        # that same call every risk tick creates the live spam pattern observed in
        # ops ("Risk pre-reconcile TIMEOUT ... after 5.0s" every ~6s) and delays
        # protective-exit evaluation by the timeout every cycle. After a timeout,
        # back off this user's pre-reconcile attempts; the standalone reconciler
        # and WSS wake path can still converge state, while risk checks keep
        # running with ``pre_reconcile=False`` instead of parking behind venue IO.
        risk_config = getattr(config, "risk", None)
        prerec_ceiling = _risk_pre_reconcile_timeout_seconds(
            poll_interval,
            getattr(risk_config, "risk_pre_reconcile_timeout_seconds", None),
        )
        prerec_backoff_base = _risk_pre_reconcile_backoff_base_seconds(
            poll_interval,
            prerec_ceiling,
            getattr(risk_config, "risk_pre_reconcile_backoff_seconds", None),
        )
        prerec_now = time.monotonic()
        prerec_contexts = [
            context for context in contexts if _risk_pre_reconcile_is_due(context, now=prerec_now)
        ]
        if prerec_contexts:
            prereconcile_results = await asyncio.gather(
                *(
                    asyncio.wait_for(_pre_reconcile_for_context(context), timeout=prerec_ceiling)
                    for context in prerec_contexts
                ),
                return_exceptions=True,
            )
            for context, result in zip(prerec_contexts, prereconcile_results, strict=False):
                if isinstance(result, asyncio.TimeoutError):
                    backoff_seconds = _record_risk_pre_reconcile_timeout(
                        context,
                        now=time.monotonic(),
                        base_backoff_seconds=prerec_backoff_base,
                    )
                    logger.error(
                        "Risk pre-reconcile TIMEOUT user=%s after %.1fs — "
                        "backing off %.1fs; deferred to standalone reconciler",
                        context.user_id,
                        prerec_ceiling,
                        backoff_seconds,
                    )
                elif isinstance(result, Exception):
                    logger.error(
                        "Risk pre-reconcile failed user=%s: %r",
                        context.user_id,
                        result,
                    )
                else:
                    _record_risk_pre_reconcile_success(context)

        refresh_tasks = [
            _refresh_context_price_cache(context, refresh_interval=poll_interval)
            for context in contexts
        ]
        refresh_results = await asyncio.gather(*refresh_tasks, return_exceptions=True)
        for context, refresh_result in zip(contexts, refresh_results, strict=False):
            if isinstance(refresh_result, Exception):
                logger.error(
                    "Risk price cache refresh failed user=%s: %r",
                    context.user_id,
                    refresh_result,
                )
        # Pre-fix: sequential iteration blocked every context behind a
        # stalled venue call for any single one. Parallel gather mirrors
        # the refresh-tasks path above so one slow context never starves
        # the others. asyncio.wait_for per context puts a hard ceiling
        # on any single per-context check so a hung venue call cannot
        # block the loop indefinitely — the timeout budget is the same
        # poll_interval, which is the maximum any one cycle can take.
        ceiling = max(poll_interval, 5.0)
        check_tasks = [
            asyncio.wait_for(
                run_risk_check_for_context(c, pre_reconcile=False),
                timeout=ceiling,
            )
            for c in contexts
        ]
        check_results = await asyncio.gather(*check_tasks, return_exceptions=True)
        for context, check_result in zip(contexts, check_results, strict=False):
            if isinstance(check_result, asyncio.TimeoutError):
                logger.error(
                    f"Risk loop TIMEOUT user={context.user_id} "
                    f"after {ceiling:.1f}s — context skipped this cycle"
                )
            elif isinstance(check_result, Exception):
                logger.error(
                    "Risk loop error user=%s: %r",
                    context.user_id,
                    check_result,
                )
        await asyncio.sleep(poll_interval)
