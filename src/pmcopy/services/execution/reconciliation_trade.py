"""Trade-order reconciliation (Protocol path + concrete trade-record settlement)."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Awaitable, Callable, cast

from pmcopy.domain.cost_basis import PositionCostBasis
from pmcopy.domain.order_lifecycle import OrderLifecycleStatus
from pmcopy.services.execution.fill_accounting import AccountedOrderFill
from pmcopy.services.execution.reconciliation_models import (
    _FINAL_FILL_STATUSES_TRADE,
    _TERMINAL_NO_FILL_STATUSES,
    _VENUE_NON_TERMINAL,
    _VENUE_TO_LIFECYCLE,
    ConfirmFillCallable,
    ReconciliationResult,
    _ReconciliationMixinBase,
    _matched_amount_from_response,
    _normalize_venue_status,
)
from pmcopy.services.execution.recovery_inventory import StartupInventoryRecoveryUnavailable
from pmcopy.storage.postgres.fills_repo import FillRecord
from pmcopy.utils.logger import get_logger

_log = get_logger("ReconciliationService")


class _ReconciliationTradeMixin(_ReconciliationMixinBase):
    """Terminal settlement for copy-trade orders and Protocol-based reconcile_order."""

    # ------------------------------------------------------------------
    # Protocol-based path (kept for unit tests + V2 gateway)
    # ------------------------------------------------------------------
    async def reconcile_order(self, order_id: str) -> ReconciliationResult:
        """Reconcile one persisted order against authoritative venue evidence."""
        order = await self.orders_repo.get(order_id)

        if not order.venue_order_id:
            await self.orders_repo.mark_reconciliation_required(order.id)
            return ReconciliationResult.unresolved(
                order.id,
                OrderLifecycleStatus.RECONCILIATION_REQUIRED,
                "no_venue_order_id",
            )

        gateway = self.clob_gateway
        if gateway is None:
            return ReconciliationResult.unresolved(
                order.id,
                OrderLifecycleStatus.RECONCILIATION_REQUIRED,
                "clob_gateway_unavailable",
            )
        try:
            venue_order = await gateway.get_order(order.venue_order_id)
            venue_fills = await gateway.get_fills(market=order.market_id)
        except Exception as exc:
            self._audit("reconciliation_unreachable", {"order_id": order.id, "error": str(exc)})
            return ReconciliationResult.unresolved(
                order.id, OrderLifecycleStatus.UNKNOWN_UNCONFIRMED, str(exc)
            )

        if venue_fills:
            # Issue #97: filter fills to the exact venue_order_id before
            # applying. get_fills(market=...) returns ALL fills for the
            # market — applying every one to the target order corrupts
            # position state and can trigger premature reservation release.
            # Invariant: every applied fill MUST carry an order_id that
            # matches order.venue_order_id. Fills missing an order_id are
            # inconclusive and must NOT be attributed to this order.
            matching_fills = [
                f
                for f in venue_fills
                if (
                    f.get("order_id") or f.get("orderID")
                    if isinstance(f, dict)
                    else getattr(f, "order_id", None)
                )
                == order.venue_order_id
            ]
            if matching_fills:
                # PR #108 finding: the previous ``async with self.db.transaction()``
                # was a no-op — production callers never pass ``db=``, and
                # ``asyncpg.Pool`` does not expose ``.transaction()``. Each
                # repo call already commits on its own pool connection, so
                # we issue them in dependency order without a doomed outer
                # wrap. If true cross-repo atomicity is needed here it
                # requires a connection-aware repo path (out of scope).
                for venue_fill in matching_fills:
                    fill = await self.fills_repo.upsert_venue_fill(order, venue_fill)
                    await self.positions_repo.upsert_position(fill)

                await self.orders_repo.mark_filled_or_partially_filled(order.id)
                if self.reservation_service is not None:
                    await self.reservation_service.release_terminal(
                        order.reservation_id,
                        reason="venue_fill_confirmed",
                    )
                self._audit("reconciliation_resolved_filled", {"order_id": order.id})
                return ReconciliationResult.resolved(order.id, OrderLifecycleStatus.FILLED)
            # venue_fills was non-empty but none matched this order — treat
            # as inconclusive (fills belong to a different order in the same
            # market, or lack order_id and cannot be safely attributed).
            self._audit(
                "reconciliation_no_matching_fill",
                {"order_id": order.id, "total_fills": len(venue_fills)},
            )

        if venue_order and venue_order.status.lower() in {"live", "open", "pending"}:
            await self.orders_repo.mark_submitted_unconfirmed(order.id)
            return ReconciliationResult.pending(
                order.id, OrderLifecycleStatus.SUBMITTED_UNCONFIRMED
            )

        if venue_order and venue_order.status.lower() in {
            "cancelled",
            "expired",
            "rejected",
        }:
            # PR #108 finding: same as the matching_fills branch — the
            # outer ``self.db.transaction()`` was a no-op in production.
            await self.orders_repo.mark_terminal(order.id, venue_order.status.lower())
            if self.reservation_service is not None:
                await self.reservation_service.release_terminal(
                    order.reservation_id,
                    reason=venue_order.status.lower(),
                )
            self._audit(
                "reconciliation_resolved_terminal",
                {"order_id": order.id, "status": venue_order.status.lower()},
            )
            try:
                status = OrderLifecycleStatus(venue_order.status.lower())
            except ValueError:
                status = OrderLifecycleStatus.TERMINAL_FAILED
            return ReconciliationResult.resolved(order.id, status)

        await self.orders_repo.mark_reconciliation_required(order.id)
        return ReconciliationResult.unresolved(
            order.id,
            OrderLifecycleStatus.RECONCILIATION_REQUIRED,
            f"unknown_venue_status:{venue_order.status if venue_order else 'None'}",
        )

    # ------------------------------------------------------------------
    # Trade BUY/SELL processed_trade reconciliation
    # ------------------------------------------------------------------
    async def _startup_sell_inventory_recovery(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        confirmed: bool,
        get_position: Callable[..., Any] | None,
    ) -> bool | None:
        """Choose normal application, verified absence recovery, or unresolved.

        True authorizes the locked absence-recovery writer; False retains the
        normal position delta for positive local inventory. None means local
        state, final fill evidence, or current account inventory is unavailable.
        """
        try:
            position_reader = get_position or self.positions_repo.get_position
            position = await position_reader(user_id, condition_id, token_id=token_id)
            if position is not None:
                size = getattr(position, "size_tokens", None)
                if not isinstance(size, Decimal) or not size.is_finite() or size < 0:
                    return None
                if size > 0:
                    return False
            if (
                not confirmed
                or self._accounted_fill_reader is None
                or self._recovery_inventory is None
            ):
                return None
            absent = await self._recovery_inventory.zero_inventory(user_id, condition_id, token_id)
        except Exception as exc:
            _log.warning("startup_sell_recovery_unavailable error_type=%s", type(exc).__name__)
            return None
        return True if absent is True else None

    async def reconcile_trade_record(
        self,
        record: dict[str, Any],
        side: str,
        *,
        confirm_fill: ConfirmFillCallable,
        pnl_tracker: Any | None = None,
        get_position: Callable[..., Any] | None = None,
        logger: Any | None = None,
        balance_regime: Any | None = None,
        startup_recovery: bool = False,
    ) -> bool:
        """Reconcile one BUY/SELL processed_trade order.

        Returns True if the order was reconciled (fill applied or retry
        unlocked); False otherwise. Logs go through ``logger`` if supplied so
        callers retain their existing trace tags.

        ``startup_recovery`` permits confirmed absent-inventory SELL accounting
        only with a fresh complete holdings proof and a locked absence check.

        Raises:
            StartupInventoryRecoveryUnavailable: An absent-position startup SELL
                lacks authoritative inventory, final fill, or accounting evidence.
        """
        order_id = record.get("order_id")
        if not order_id:
            return False

        # Fail closed on malformed rows. Terminal branches dereference these
        # keys with bracket access — a missing key would raise KeyError inside
        # the outer try/except and the caller would silently retry forever.
        required_keys = ("user_id", "trade_key", "asset", "condition_id")
        missing_keys = [k for k in required_keys if not record.get(k)]
        if missing_keys:
            if logger is not None:
                logger.error(
                    "reconcile_trade_record_malformed | order_id=%s missing=%s keys=%s",
                    order_id,
                    missing_keys,
                    list(record.keys()),
                )
            return False

        try:
            fill_info = await confirm_fill(order_id)
        except Exception as exc:
            if logger is not None:
                logger.warning(
                    "reconcile_unconfirmed_poll_error | side=%s order_id=%s error=%s",
                    side,
                    order_id,
                    repr(exc),
                )
            return False

        order_status = _normalize_venue_status(fill_info.get("status", ""))
        current_matched = _matched_amount_from_response(fill_info)
        # poll_deferred means this pass did not ask the venue. It is not
        # evidence of an unfilled order, and treating it as one rewrites a
        # settled or live row to ambiguous_timeout on the next tick.
        if order_status == "poll_deferred":
            return False
        if not order_status and current_matched <= 0:
            if logger is not None:
                logger.warning(
                    "reconcile_unconfirmed_retryable_missing_status | side=%s order_id=%s cond=%s",
                    side,
                    order_id,
                    str(record.get("condition_id", ""))[:12],
                    extra={
                        "type": "reconcile_unconfirmed_missing_status",
                        "order_id": order_id,
                        "condition_id": record.get("condition_id"),
                        "user_id": record["user_id"],
                        "side": side,
                    },
                )
            return False

        unresolved_status = (
            order_status not in _FINAL_FILL_STATUSES_TRADE
            and order_status not in _TERMINAL_NO_FILL_STATUSES
            and current_matched <= 0
        )
        if unresolved_status:
            timed_out, released = await self._handle_unresolved_confirmation_timeout(
                record,
                side=side,
                fill_info=fill_info,
            )
            if released:
                return True
            if timed_out:
                return False

        accounted_fill = None
        if self._accounted_fill_reader is not None:
            accounted_fill = await self._accounted_fill_reader.accounted_order_fill(
                record["user_id"], order_id
            )
            previous_matched = accounted_fill.shares
        else:
            previous_matched = await self._previous_matched_for_order(self.orders_repo, order_id)
        delta_matched = current_matched - previous_matched

        if current_matched < previous_matched:
            self._record_negative_delta_anomaly(
                side=side,
                order_id=order_id,
                current_matched=current_matched,
                previous_matched=previous_matched,
                logger=logger,
            )

        # Non-terminal intermediate status (live, delayed, matched, mined,
        # retrying). Persist the mapped local status and emit a single
        # lifecycle log per transition. Reservation stays active. No state
        # mutation beyond status — the existing terminal branches do the
        # fill/release work.
        if order_status in _VENUE_NON_TERMINAL and delta_matched <= 0:
            # Already-settled idempotency (2026-09-19 incident): a row that
            # already reached a terminal filled state at exactly this
            # cumulative matched total must not be downgraded to a live-family
            # status or churned forever by re-reconciliation — e.g. trade-id
            # evidence for a sibling-attributed order legitimately returns a
            # non-terminal "live" status while account fills stay empty.
            # Nothing new to apply; the settlement is complete.
            if current_matched > 0 and str(record.get("status") or "") == "filled_reconciled":
                return True
            mapped = _VENUE_TO_LIFECYCLE.get(order_status)
            if mapped is not None:
                local_status, lifecycle_status = mapped
                # Idempotency: if the row already carries this status, skip.
                current_local = str(record.get("status") or "")
                if current_local != local_status:
                    try:
                        await self.orders_repo.update_submission_status(
                            order_id=order_id,
                            status=local_status,
                            response_patch=fill_info,
                        )
                    except Exception:
                        pass
                self._emit_lifecycle(
                    status=lifecycle_status,
                    side=side,
                    record=record,
                    order_id=order_id,
                    venue_status=order_status,
                )
            return False

        # Terminal no-fill — unlock the trade key for retry.
        if current_matched <= 0 and order_status in _TERMINAL_NO_FILL_STATUSES:
            if logger is not None:
                logger.info(
                    "reconcile_%s_no_fill_unlock_retry | order_id=%s cond=%s",
                    side.lower(),
                    order_id,
                    str(record.get("condition_id", ""))[:12],
                    extra={
                        "type": f"reconcile_{side.lower()}_no_fill_unlock",
                        "order_id": order_id,
                        "condition_id": record.get("condition_id"),
                        "user_id": record["user_id"],
                        "order_status": order_status,
                    },
                )
            # Phase 4 — release the BUY's reserved spend on the rolling
            # daily-cap ledger before we drop the processed-trade row. The
            # BUY was recorded eagerly at submit time (see
            # ``orchestrators.trade_pipeline``); a terminal no-fill cancel
            # means the venue is returning the budget, so we must too.
            # SELLs do not participate in the daily-spend ledger.
            if (
                balance_regime is not None
                and str(side).upper() == "BUY"
                and hasattr(balance_regime, "record_cancel")
            ):
                cancel_amt = await self._resolve_buy_requested_amount(order_id)
                if cancel_amt > 0:
                    try:
                        balance_regime.record_cancel(
                            cancel_amt,
                            order_id=order_id,
                        )
                    except TypeError:
                        # Back-compat: older signature without order_id.
                        try:
                            balance_regime.record_cancel(cancel_amt)
                        except Exception:  # pragma: no cover — defensive
                            pass
                    except Exception:  # pragma: no cover — defensive
                        pass
            if self.task_budget_settlement is not None:
                try:
                    await self.task_budget_settlement.record_terminal_no_fill(
                        record=record,
                        side=side,
                        order_id=order_id,
                    )
                except Exception as exc:
                    if logger is not None:
                        logger.warning(
                            "reconcile_task_budget_no_fill_failed | order_id=%s error=%r",
                            order_id,
                            exc,
                        )
                    return False
            await self.orders_repo.delete_processed_trade(record["trade_key"])
            mapped = _VENUE_TO_LIFECYCLE.get(order_status, ("failed", "failed"))
            update_status = getattr(self.orders_repo, "update_submission_status", None)
            if callable(update_status):
                try:
                    await cast(Callable[..., Awaitable[Any]], update_status)(
                        order_id=order_id,
                        status=mapped[0],
                        response_patch=fill_info,
                    )
                except Exception as exc:
                    if logger is not None:
                        logger.warning(
                            "reconcile_no_fill_status_write_failed | order_id=%s error=%s",
                            order_id,
                            repr(exc),
                        )
            self._audit(
                "reconciliation_no_fill_unlock",
                {"order_id": order_id, "side": side, "venue_status": order_status},
            )
            # Release reservation if the record carries one (coordinator path).
            await self._release_reservation_if_present(record, reason=order_status)
            # Lifecycle log — render the canonical terminal-failure panel.
            self._emit_lifecycle(
                status=mapped[1],
                side=side,
                record=record,
                order_id=order_id,
                venue_status=order_status,
            )
            return True

        if delta_matched < 0:
            delta_matched = Decimal("0")

        is_final_fill = order_status in _FINAL_FILL_STATUSES_TRADE or (
            order_status in _TERMINAL_NO_FILL_STATUSES and current_matched > 0
        )

        recover_absent_position = False
        if startup_recovery and side == "SELL" and current_matched > 0:
            recovery = await self._startup_sell_inventory_recovery(
                user_id=record["user_id"],
                condition_id=record["condition_id"],
                token_id=record["asset"],
                confirmed=order_status in _FINAL_FILL_STATUSES_TRADE,
                get_position=get_position,
            )
            if recovery is None:
                raise StartupInventoryRecoveryUnavailable(
                    "Startup SELL inventory recovery is unresolved"
                )
            recover_absent_position = recovery

        if delta_matched <= 0:
            if is_final_fill:
                if self.task_budget_settlement is not None and current_matched > 0:
                    try:
                        await self.task_budget_settlement.settle_existing_fill(
                            record=record,
                            side=side,
                            order_id=order_id,
                            cumulative_matched=current_matched,
                        )
                    except Exception as exc:
                        if logger is not None:
                            logger.warning(
                                "reconcile_task_budget_terminal_failed | order_id=%s error=%r",
                                order_id,
                                exc,
                            )
                        return False
                # Detect "already settled": either the in-memory record
                # already shows the terminal state, or the orders_repo row
                # was updated by a previous pass.
                stored_row = None
                try:
                    stored_row = await self.orders_repo.get_by_order_id(order_id)
                except Exception:
                    stored_row = None
                stored_status = ""
                if isinstance(stored_row, dict):
                    stored_status = str(stored_row.get("status") or "")
                already_terminal = (
                    str(record.get("status") or "") == "filled_reconciled"
                    or stored_status == "filled_reconciled"
                )
                # Cumulative matched already accounted for — persist terminal
                # state without double-applying the fill.
                if not already_terminal:
                    try:
                        await self.orders_repo.update_submission_status(
                            order_id=order_id,
                            status="filled_reconciled",
                            response_patch=fill_info,
                        )
                    except Exception:
                        pass
                    await self.orders_repo.mark_processed_trade(
                        trade_key=record["trade_key"],
                        user_id=record["user_id"],
                        leader_id=record.get("leader_id"),
                        tx_hash=record.get("tx_hash"),
                        side=side,
                        asset=record["asset"],
                        condition_id=record["condition_id"],
                        status="filled_reconciled",
                        order_id=order_id,
                    )
                    self._audit(
                        "reconciliation_terminal_no_new_delta",
                        {
                            "order_id": order_id,
                            "side": side,
                            "matched_total": str(current_matched),
                        },
                    )
                    await self._release_reservation_if_present(record, reason="confirmed")
                    await self._record_funder_activity(
                        record=record,
                        side=side,
                        order_id=order_id,
                        size=current_matched,
                        price=None,
                    )
                # Lifecycle log fires once via the dedup map — idempotent.
                self._emit_lifecycle(
                    status="confirmed",
                    side=side,
                    record=record,
                    order_id=order_id,
                    venue_status=order_status,
                    size=current_matched,
                )
                return True
            return False

        avg_price = self._avg_price_from_fill(fill_info)
        if not avg_price.is_finite() or not Decimal(0) < avg_price < Decimal(1):
            return False
        fee_basis = accounted_fill or AccountedOrderFill(
            fees=Decimal("0") if previous_matched == 0 else None,
        )
        try:
            fee_amount = fee_basis.incremental_fee(fill_info.get("fee_amount"))
            if accounted_fill is not None:
                avg_price = accounted_fill.incremental_price(current_matched, avg_price)
        except ValueError:
            self._audit("reconciliation_invalid_notional_or_fee", {"order_id": order_id})
            return False

        # P0-1: read entry_price BEFORE the atomic settlement. A SELL apply does
        # not change avg_entry, but keep the ordering explicit.
        entry_price: Decimal | None = None
        if side == "SELL" and get_position is not None:
            pos = await get_position(
                record["user_id"], record["condition_id"], token_id=record["asset"]
            )
            if pos is not None:
                entry_raw = getattr(pos, "avg_entry_price", None)
                if entry_raw is not None:
                    try:
                        entry_price = PositionCostBasis.from_value(entry_raw).entry_price
                    except (TypeError, ValueError):
                        entry_price = None

        try:
            # P0-1: append the fill and apply the position delta atomically in a
            # single transaction, gated on the fill actually inserting and keyed
            # on a STABLE dedup_key = the cumulative matched total. A re-apply of
            # the same settlement (after a crash, or a swallowed watermark error
            # left the reconciler recomputing the same delta) conflicts on
            # idx_fills_dedup_v2 and returns (0, None), so the delta is applied
            # exactly once — no more double-apply across a wall-clock second.
            append_fill = (
                self.fills_repo.append_recovered_sell
                if recover_absent_position
                else self.fills_repo.append_fill_and_apply
            )
            fill_id, _applied = await append_fill(
                FillRecord(
                    user_id=record["user_id"],
                    leader_id=record.get("leader_id"),
                    order_id=order_id,
                    condition_id=record["condition_id"],
                    token_id=record["asset"],
                    side=side,
                    size_tokens=delta_matched,
                    price=avg_price,
                    fee=fee_amount,
                    fill_ts=int(time.time()),
                    dedup_key=str(current_matched),
                    accounted_shares=accounted_fill.shares if accounted_fill is not None else None,
                ),
                self.positions_repo,
                raw_payload={
                    "reconciled_from": "submitted_unconfirmed",
                    "copy_order_id": order_id,
                    "status": "reconciled",
                    "side": side,
                    **({"position_delta": "already_absorbed"} if recover_absent_position else {}),
                },
            )
        except Exception as exc:
            if recover_absent_position:
                raise StartupInventoryRecoveryUnavailable(
                    "Startup SELL inventory recovery is unresolved"
                ) from None
            if logger is not None:
                logger.warning(
                    "reconcile_unconfirmed_apply_error | side=%s order_id=%s error=%s",
                    side,
                    order_id,
                    repr(exc),
                )
            return False

        if fill_id == 0:
            # Duplicate settlement — a prior pass already appended this fill and
            # applied the delta. Do not re-apply or re-record PnL; fall through
            # so a prior pass that crashed before the status/terminal write still
            # completes that (idempotent) handling.
            if logger is not None:
                logger.info(
                    "reconcile_duplicate_settlement_skipped | order_id=%s cumulative=%s",
                    order_id,
                    current_matched,
                )
        if self.task_budget_settlement is not None:
            try:
                await self.task_budget_settlement.record_fill(
                    record=record,
                    side=side,
                    order_id=order_id,
                    cumulative_matched=current_matched,
                    delta_matched=delta_matched,
                    average_price=avg_price,
                    terminal=is_final_fill,
                )
            except Exception as exc:
                if logger is not None:
                    logger.warning(
                        "reconcile_task_budget_fill_failed | order_id=%s error=%r",
                        order_id,
                        exc,
                    )
                return False
        if fill_id != 0 and pnl_tracker is not None:
            if side == "SELL":
                realized_source = "entry_price" if entry_price is not None else "unknown_cost_basis"
                realized_d = (
                    (avg_price - entry_price) * delta_matched if entry_price is not None else None
                )
                pnl_tracker.record_sell(
                    condition_id=record["condition_id"],
                    price=avg_price,
                    size_tokens=delta_matched,
                    usdc_value=delta_matched * avg_price,
                    entry_price=entry_price,
                    realized_pnl=realized_d,
                    exit_reason="reconciled_sell",
                    leader_wallet=record.get("leader_id", ""),
                    event_slug="",
                    title=f"realized_source={realized_source}",
                )
            elif side == "BUY" and avg_price > 0 and hasattr(pnl_tracker, "record_buy"):
                pnl_tracker.record_buy(
                    condition_id=record["condition_id"],
                    price=avg_price,
                    size_tokens=delta_matched,
                    usdc_value=delta_matched * avg_price,
                    leader_wallet=record.get("leader_id", ""),
                    event_slug="",
                    title="reconciled_buy",
                )

        if is_final_fill:
            if (
                balance_regime is not None
                and str(side).upper() == "BUY"
                and order_status in _TERMINAL_NO_FILL_STATUSES
                and avg_price > 0
                and hasattr(balance_regime, "record_partial_fill")
            ):
                requested_amt = await self._resolve_buy_requested_amount(order_id)
                actual_usdc = current_matched * avg_price
                if requested_amt > actual_usdc:
                    try:
                        balance_regime.record_partial_fill(
                            recorded_usdc=requested_amt,
                            actual_usdc=actual_usdc,
                            order_id=order_id,
                        )
                    except Exception:
                        pass
            await self.orders_repo.mark_processed_trade(
                trade_key=record["trade_key"],
                user_id=record["user_id"],
                leader_id=record.get("leader_id"),
                tx_hash=record.get("tx_hash"),
                side=side,
                asset=record["asset"],
                condition_id=record["condition_id"],
                status="filled_reconciled",
                order_id=order_id,
            )
            submission_status = "filled_reconciled"
            await self._release_reservation_if_present(record, reason="confirmed")
            await self._record_funder_activity(
                record=record,
                side=side,
                order_id=order_id,
                size=current_matched,
                price=avg_price,
            )
            self._emit_lifecycle(
                status="confirmed",
                side=side,
                record=record,
                order_id=order_id,
                venue_status=order_status,
                size=current_matched,
                price=avg_price,
                usdc_value=float(current_matched * avg_price)
                if current_matched and avg_price
                else None,
            )
        else:
            # Non-terminal partial fill — keep the order row alive in
            # ``submitted_unconfirmed`` so duplicate-blocking and the next
            # reconcile pass continue to fire.
            submission_status = "submitted_unconfirmed"
            self._emit_lifecycle(
                status="matched",
                side=side,
                record=record,
                order_id=order_id,
                venue_status=order_status,
                size=current_matched,
                price=avg_price,
            )

        try:
            await self.orders_repo.update_submission_status(
                order_id=order_id,
                status=submission_status,
                response_patch=fill_info,
            )
        except Exception as exc:
            # P0-1: surface a failed watermark write instead of swallowing it.
            # idx_fills_dedup_v2 (keyed on cumulative matched) is now the durable
            # idempotency guard, so a stale watermark no longer double-applies —
            # but a persistent write failure is a real signal worth logging.
            if logger is not None:
                logger.warning(
                    "reconcile_watermark_write_failed | order_id=%s status=%s error=%s",
                    order_id,
                    submission_status,
                    repr(exc),
                )

        if logger is not None:
            logger.info(
                "reconcile_unconfirmed_filled | side=%s order_id=%s cond=%s size=%.4f price=%.4f",
                side,
                order_id,
                str(record.get("condition_id", ""))[:12],
                float(delta_matched),
                float(avg_price),
                extra={
                    "type": "reconcile_unconfirmed_filled",
                    "side": side,
                    "order_id": order_id,
                    "condition_id": record.get("condition_id"),
                    "user_id": record["user_id"],
                    "filled_amount": str(delta_matched),
                    "avg_price": str(avg_price),
                    "venue_status": order_status,
                    "matched_total": str(current_matched),
                    "matched_previous": str(previous_matched),
                },
            )

        self._audit(
            "reconciliation_applied_fill",
            {
                "order_id": order_id,
                "side": side,
                "delta": str(delta_matched),
                "matched_total": str(current_matched),
                "is_final": is_final_fill,
            },
        )
        try:
            from pmcopy.services.observability.metrics import (
                METRIC_RECONCILIATION_DELTA_APPLIED,
                metrics,
            )

            metrics.inc_sync(
                METRIC_RECONCILIATION_DELTA_APPLIED,
                value=float(delta_matched),
                labels={"side": str(side or "unknown")},
            )
        except Exception:
            pass
        if self._telemetry is not None:
            # Keyed by the cumulative matched total, so a repeated sweep over
            # the same total cannot be read as a second fill.
            self._telemetry.fill_observed(order_id, current_matched)
        return True

