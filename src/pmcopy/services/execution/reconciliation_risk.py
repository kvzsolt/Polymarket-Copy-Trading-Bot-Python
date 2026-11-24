"""Risk-exit SELL reconciliation and position-drift reconciliation mixin."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Awaitable, Callable

from pmcopy.domain.cost_basis import PositionCostBasis
from pmcopy.services.execution.fill_accounting import AccountedOrderFill
from pmcopy.services.execution.reconciliation_models import (
    _FINAL_FILL_STATUSES_RISK,
    _FINAL_FILL_STATUSES_TRADE,
    _TERMINAL_NO_FILL_STATUSES,
    _VENUE_NON_TERMINAL,
    _VENUE_TO_LIFECYCLE,
    ConfirmFillCallable,
    _ReconciliationMixinBase,
    _matched_amount_from_response,
    _normalize_venue_status,
)
from pmcopy.services.execution.recovery_inventory import StartupInventoryRecoveryUnavailable
from pmcopy.storage.postgres.fills_repo import FillRecord
from pmcopy.utils.logger import get_logger

_log = get_logger("ReconciliationService")


class _ReconciliationRiskMixin(_ReconciliationMixinBase):
    """Protective risk-exit settlement and venue-vs-local position drift reconciliation."""

    # ------------------------------------------------------------------
    # Risk SELL reconciliation
    # ------------------------------------------------------------------
    async def reconcile_risk_record(
        self,
        record: dict[str, Any],
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        confirm_fill: ConfirmFillCallable,
        pnl_tracker: Any | None = None,
        get_position: Callable[..., Any] | None = None,
        logger: Any | None = None,
        startup_recovery: bool = False,
    ) -> bool:
        """Reconcile one risk-monitor-originated SELL order.

        Returns True when reconciliation produced any state convergence.

        ``startup_recovery`` enables the same verified absent-inventory handling
        as the trade path; it never applies to ordinary runtime reconciliation.

        Raises:
            StartupInventoryRecoveryUnavailable: An absent-position startup SELL
                lacks authoritative inventory, final fill, or accounting evidence.
        """
        order_id = str(record.get("order_id") or "").strip()
        if not order_id:
            return False

        request = record.get("request") or {}

        try:
            fill_info = await confirm_fill(order_id)
        except Exception as exc:
            if logger is not None:
                logger.warning(
                    "risk_unconfirmed_reconcile_poll_error | user=%s order_id=%s error=%s",
                    user_id,
                    order_id,
                    repr(exc),
                )
            return False

        venue_status = _normalize_venue_status(fill_info.get("status", ""))
        current_matched = _matched_amount_from_response(fill_info)
        if venue_status == "poll_deferred":
            return False
        unresolved_status = (
            venue_status not in _FINAL_FILL_STATUSES_RISK
            and venue_status not in _TERMINAL_NO_FILL_STATUSES
            and current_matched <= 0
        )
        if unresolved_status:
            timed_out, released = await self._handle_unresolved_confirmation_timeout(
                {
                    **record,
                    "user_id": user_id,
                    "condition_id": condition_id,
                    "asset": token_id,
                },
                side="SELL",
                fill_info=fill_info,
            )
            if released:
                return True
            if timed_out:
                return False
        # CRITICAL: read previous matched from the persisted orders row,
        # not the in-memory ``record["response"]``. Pre-fix, on restart
        # the in-memory record was rebuilt without the prior fill
        # response and ``previous_matched`` would silently fall back to
        # zero — re-applying the entire cumulative venue size as a
        # fresh delta and double-counting every risk SELL fill that was
        # in-flight across the restart.
        accounted_fill = None
        if self._accounted_fill_reader is not None:
            accounted_fill = await self._accounted_fill_reader.accounted_order_fill(
                user_id, order_id
            )
            previous_matched = accounted_fill.shares
        else:
            previous_matched = await self._previous_matched_for_order(self.orders_repo, order_id)
        delta_matched = current_matched - previous_matched
        if current_matched < previous_matched:
            self._record_negative_delta_anomaly(
                side="SELL",
                order_id=order_id,
                current_matched=current_matched,
                previous_matched=previous_matched,
                logger=logger,
            )
        if delta_matched < 0:
            delta_matched = Decimal("0")

        # Non-terminal intermediate status (live, delayed, mined, retrying).
        # Persist mapped status; reservation stays active; emit lifecycle log.
        if venue_status in _VENUE_NON_TERMINAL and delta_matched <= 0:
            mapped = _VENUE_TO_LIFECYCLE.get(venue_status)
            if mapped is not None:
                local_status, lifecycle_status = mapped
                try:
                    await self.orders_repo.update_submission_status(
                        order_id=order_id,
                        status=local_status,
                        response_patch=fill_info,
                    )
                except Exception:
                    pass
                # Risk SELL log uses synthetic record shape — pass user/cond/token.
                synthetic_record = {
                    **record,
                    "condition_id": condition_id,
                    "asset": token_id,
                }
                self._emit_lifecycle(
                    status=lifecycle_status,
                    side="SELL",
                    record=synthetic_record,
                    order_id=order_id,
                    venue_status=venue_status,
                )
            return False

        # Terminal no-fill: persist status, drop the in-flight reservation.
        if current_matched <= 0 and venue_status in _TERMINAL_NO_FILL_STATUSES:
            await self.orders_repo.update_submission_status(
                order_id=order_id,
                status=venue_status,
                response_patch=fill_info,
            )
            self._audit(
                "reconciliation_risk_no_fill",
                {"order_id": order_id, "venue_status": venue_status},
            )
            if self.risk_triggers_repo is not None:
                try:
                    recovery_metadata = request.get("recovery_metadata")
                    trigger_meta = (
                        dict(recovery_metadata) if isinstance(recovery_metadata, dict) else {}
                    )
                    trigger_meta["venue_status"] = venue_status
                    await self.risk_triggers_repo.record_trigger(
                        user_id=user_id,
                        condition_id=condition_id,
                        token_id=token_id,
                        reason=str(request.get("reason") or "risk_exit"),
                        source="risk_reconciliation",
                        fill_status="rejected",
                        fill_order_id=order_id,
                        metadata=trigger_meta,
                    )
                except Exception:
                    _log.debug("risk_trigger_record_error order_id=%s", order_id, exc_info=True)
            await self._release_reservation_if_present(record, reason=venue_status)
            mapped = _VENUE_TO_LIFECYCLE.get(venue_status, ("failed", "failed"))
            self._emit_lifecycle(
                status=mapped[1],
                side="SELL",
                record={**record, "condition_id": condition_id, "asset": token_id},
                order_id=order_id,
                venue_status=venue_status,
            )
            return True

        is_final_fill = venue_status in _FINAL_FILL_STATUSES_RISK or (
            venue_status in _TERMINAL_NO_FILL_STATUSES and current_matched > 0
        )

        recover_absent_position = False
        if startup_recovery and current_matched > 0:
            recovery = await self._startup_sell_inventory_recovery(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                confirmed=venue_status in _FINAL_FILL_STATUSES_TRADE,
                get_position=get_position,
            )
            if recovery is None:
                raise StartupInventoryRecoveryUnavailable(
                    "Startup SELL inventory recovery is unresolved"
                )
            recover_absent_position = recovery

        if delta_matched <= 0 and not is_final_fill:
            return False

        if delta_matched <= 0 and is_final_fill:
            already_terminal = str(record.get("status") or "") == "filled_reconciled"
            if not already_terminal:
                await self.orders_repo.update_submission_status(
                    order_id=order_id,
                    status="filled_reconciled",
                    response_patch={
                        **fill_info,
                        "reconciled_from": "risk_submitted_unconfirmed",
                        "matched_previous": str(previous_matched),
                        "matched_delta": str(delta_matched),
                    },
                )
                self._audit(
                    "reconciliation_risk_terminal_no_new_delta",
                    {"order_id": order_id, "matched_total": str(current_matched)},
                )
                await self._release_reservation_if_present(record, reason="confirmed")
            self._emit_lifecycle(
                status="confirmed",
                side="SELL",
                record={**record, "condition_id": condition_id, "asset": token_id},
                order_id=order_id,
                venue_status=venue_status,
                size=current_matched,
            )
            return True

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

        reason = str(request.get("reason") or "risk_exit")
        leader_id = str(request.get("leader_id") or "risk-monitor")
        existing_pos = None
        if get_position is not None:
            try:
                existing_pos = await get_position(user_id, condition_id, token_id=token_id)
            except Exception:
                existing_pos = None
        entry_price = PositionCostBasis.from_value(
            getattr(existing_pos, "avg_entry_price", None) if existing_pos is not None else None
        ).entry_price

        try:
            # P0-1: atomic fill+position gated on insert, stable dedup_key = the
            # cumulative matched total (same idempotency guard as the trade
            # path). A re-apply of the same settlement returns (0, None) and is
            # not applied twice.
            append_fill = (
                self.fills_repo.append_recovered_sell
                if recover_absent_position
                else self.fills_repo.append_fill_and_apply
            )
            fill_id, updated_pos = await append_fill(
                FillRecord(
                    user_id=user_id,
                    leader_id=leader_id,
                    order_id=order_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    side="SELL",
                    size_tokens=delta_matched,
                    price=avg_price,
                    fee=fee_amount,
                    fill_ts=int(time.time()),
                    dedup_key=str(current_matched),
                    accounted_shares=accounted_fill.shares if accounted_fill is not None else None,
                ),
                self.positions_repo,
                raw_payload={
                    "reconciled_from": "risk_submitted_unconfirmed",
                    "copy_order_id": order_id,
                    "status": "reconciled",
                    "side": "SELL",
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
                    "risk_unconfirmed_reconcile_apply_error | user=%s order_id=%s error=%s",
                    user_id,
                    order_id,
                    repr(exc),
                )
            return False

        reapplied_duplicate = fill_id == 0
        if reapplied_duplicate:
            # Duplicate settlement — a prior pass already applied this SELL delta.
            # Re-derive the live position so the re-entry-block decision below is
            # made on the true closure state (a prior pass may have crashed
            # before marking the block; mark_risk_block is idempotent). The PnL
            # re-record further down is skipped so realized PnL is not double
            # counted.
            updated_pos = None
            if get_position is not None:
                try:
                    updated_pos = await get_position(user_id, condition_id, token_id=token_id)
                except Exception:
                    updated_pos = None
            if logger is not None:
                logger.info(
                    "risk_reconcile_duplicate_settlement_skipped | user=%s order_id=%s cumulative=%s",
                    user_id,
                    order_id,
                    current_matched,
                )

        # Persist a re-entry block only after a confirmed full close.
        recovery_metadata = request.get("recovery_metadata")
        block_metadata = dict(recovery_metadata) if isinstance(recovery_metadata, dict) else {}
        block_metadata["fill_price"] = float(avg_price)
        block_metadata["fill_size_tokens"] = float(delta_matched)
        block_metadata["fill_order_id"] = order_id
        if is_final_fill and updated_pos is None and self.risk_blocks_repo is not None:
            await self.risk_blocks_repo.mark_risk_block(
                user_id=user_id,
                condition_id=condition_id,
                token_id=token_id,
                reason=reason,
                source="risk_monitor",
                leader_anchor=leader_id,
                metadata=block_metadata,
            )
        elif logger is not None and not (is_final_fill and updated_pos is None):
            logger.info(
                "risk_unconfirmed_partial_fill_no_block | user=%s order_id=%s condition=%s token=%s remaining=%s",
                user_id,
                order_id,
                condition_id,
                token_id,
                getattr(updated_pos, "size_tokens", None),
            )

        if not reapplied_duplicate and self.risk_triggers_repo is not None:
            try:
                trigger_fill_status = (
                    "filled" if (is_final_fill and updated_pos is None) else "partial"
                )
                await self.risk_triggers_repo.record_trigger(
                    user_id=user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    reason=reason,
                    source="risk_reconciliation",
                    fill_status=trigger_fill_status,
                    fill_order_id=order_id,
                    metadata=block_metadata,
                )
            except Exception:
                _log.debug("risk_trigger_record_error order_id=%s", order_id, exc_info=True)

        if (
            not reapplied_duplicate
            and pnl_tracker is not None
            and hasattr(pnl_tracker, "record_sell")
        ):
            realized_d = (
                (avg_price - entry_price) * delta_matched if entry_price is not None else None
            )
            pnl_tracker.record_sell(
                condition_id=condition_id,
                price=avg_price,
                size_tokens=delta_matched,
                usdc_value=delta_matched * avg_price,
                entry_price=entry_price,
                realized_pnl=realized_d,
                exit_reason=reason,
                leader_wallet=leader_id,
            )

        submission_status = "filled_reconciled" if is_final_fill else "submitted_unconfirmed"
        await self.orders_repo.update_submission_status(
            order_id=order_id,
            status=submission_status,
            response_patch={
                **fill_info,
                "reconciled_from": "risk_submitted_unconfirmed",
                "matched_previous": str(previous_matched),
                "matched_delta": str(delta_matched),
            },
        )
        self._audit(
            "reconciliation_risk_applied_fill",
            {
                "order_id": order_id,
                "delta": str(delta_matched),
                "matched_total": str(current_matched),
                "is_final": is_final_fill,
            },
        )
        if is_final_fill:
            await self._release_reservation_if_present(record, reason="confirmed")
        self._emit_lifecycle(
            status="confirmed" if is_final_fill else "matched",
            side="SELL",
            record={**record, "condition_id": condition_id, "asset": token_id},
            order_id=order_id,
            venue_status=venue_status,
            size=current_matched,
            price=avg_price,
            usdc_value=float(current_matched * avg_price)
            if current_matched and avg_price
            else None,
        )
        return True

    # ------------------------------------------------------------------
    # Manual / off-bot position drift reconciliation (Issue #74)
    # ------------------------------------------------------------------
    async def reconcile_position_drift(
        self,
        *,
        user_id: str,
        condition_id: str,
        token_id: str,
        leader_id: str,
        live_position_fetcher: Callable[..., Awaitable[Decimal | None]],
        reservation_service: Any | None = None,
    ) -> dict[str, Any]:
        """Reconcile local position against live venue holdings.

        Called when a SELL fails with ``insufficient_balance`` / ``not enough
        balance`` / ``allowance`` — strong indicators that the true position
        has drifted from our local state due to manual or off-bot trades.

        Returns a dict with keys:
          - ``reconciled``: bool — whether drift was detected and corrected.
          - ``live_size``: Decimal — the actual live position size.
          - ``action``: str — ``"deleted"`` | ``"shrunk"`` | ``"no_drift"`` | ``"fetch_failed"``.
        """
        result: dict[str, Any] = {
            "reconciled": False,
            "live_size": Decimal("0"),
            "action": "fetch_failed",
        }

        try:
            live_size = await live_position_fetcher()
        except Exception as exc:
            _log.warning(
                "reconcile_position_drift_fetch_failed | user=%s cond=%s token=%s error=%s",
                user_id,
                condition_id,
                token_id,
                repr(exc),
            )
            self._audit(
                "manual_position_drift_fetch_failed",
                {
                    "user_id": user_id,
                    "condition_id": condition_id,
                    "token_id": token_id,
                    "error": str(exc),
                },
            )
            return result

        result["live_size"] = live_size if live_size is not None else Decimal("0")

        if live_size is None:
            return result

        existing = await self.positions_repo.get_position(user_id, condition_id, token_id=token_id)
        local_size = getattr(existing, "size_tokens", Decimal("0")) if existing else Decimal("0")

        if local_size <= Decimal("1e-12") and live_size <= Decimal("1e-12"):
            result["action"] = "no_drift"
            return result

        if abs(local_size - live_size) <= Decimal("1e-12"):
            result["action"] = "no_drift"
            return result

        avg_entry_price = (
            getattr(existing, "avg_entry_price", Decimal("0")) if existing else Decimal("0")
        )

        if live_size <= Decimal("1e-12"):
            await self.positions_repo.delete_position(user_id, condition_id, token_id=token_id)
            result["action"] = "deleted"
            result["reconciled"] = True

            if reservation_service is not None:
                try:
                    reservations_repo = getattr(reservation_service, "reservations_repo", None)
                    if reservations_repo is not None:
                        active_reservations = await reservations_repo.list_active_for_scope(
                            user_id=user_id,
                            condition_id=condition_id,
                            token_id=token_id,
                        )
                        for res in active_reservations:
                            if res.side.value == "SELL":
                                await reservation_service.release_terminal(
                                    res.id, reason="manual_position_drift_deleted"
                                )
                except Exception:
                    _log.debug("drift_reservation_cleanup_failed", exc_info=True)
        else:
            from pmcopy.storage.postgres.positions_repo import PositionRecord

            await self.positions_repo.upsert_position(
                PositionRecord(
                    user_id=user_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    leader_id=leader_id,
                    size_tokens=live_size,
                    avg_entry_price=Decimal(str(avg_entry_price or 0)),
                )
            )
            result["action"] = "shrunk"
            result["reconciled"] = True

            if reservation_service is not None:
                try:
                    reservations_repo = getattr(reservation_service, "reservations_repo", None)
                    if reservations_repo is not None:
                        active_reservations = await reservations_repo.list_active_for_scope(
                            user_id=user_id,
                            condition_id=condition_id,
                            token_id=token_id,
                        )
                        for res in active_reservations:
                            if res.side.value == "SELL" and res.reserved_size > live_size:
                                await reservation_service.release_terminal(
                                    res.id, reason="manual_position_drift_shrunk"
                                )
                except Exception:
                    _log.debug("drift_reservation_cleanup_failed", exc_info=True)

        self._audit(
            "manual_position_drift_reconciled",
            {
                "user_id": user_id,
                "leader_id": leader_id,
                "condition_id": condition_id,
                "token_id": token_id,
                "previous_local_size": str(local_size),
                "live_size": str(result["live_size"]),
                "action": result["action"],
                "source": "reconciliation_service",
            },
        )
        _log.info(
            "manual_position_drift_reconciled | user=%s cond=%s token=%s local=%s live=%s action=%s",
            user_id,
            condition_id,
            token_id,
            str(local_size),
            str(result["live_size"]),
            result["action"],
        )
        return result
