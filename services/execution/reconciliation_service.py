"""Canonical reconciliation service (Issue #71).

The reconciler is the **sole owner** of terminal order settlement and startup
convergence for unconfirmed orders. Other components (``trade_pipeline``,
``riskMonitor``) MUST delegate per-record settlement here instead of applying
fills directly. The execution service submits orders; it never self-settles.

This module exposes two public surfaces:

* The original ``ReconciliationService.reconcile_order`` Protocol-based path
  used by unit tests and (eventually) the V2 gateway-driven flow described in
  issue #66. It is preserved for backward compatibility.
* ``reconcile_trade_record`` and ``reconcile_risk_record`` — concrete methods
  that operate on the dict-shaped records produced by the production
  ``OrdersRepo``. These hold the canonical fill-application logic that used to
  live in ``orchestrators/trade_pipeline._reconcile_one`` and the inner loop of
  ``services/riskMonitor.reconcile_unconfirmed_risk_orders``.

Both concrete methods enforce:

* venue ``order_id`` lookup as the primary key (idempotency-key fallback only
  for legacy rows handled in ``OrdersRepo.get_by_order_id``);
* incremental delta application — only ``current_matched - previous_matched``
  is applied, so restarts cannot double-count fills;
* terminal-state ownership — only this service transitions an order to a
  terminal status and persists the matched_total back to the order row;
* optional reservation release on terminal transitions when the order record
  carries a ``reservation_id``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable, Protocol

from domain.order_lifecycle import OrderLifecycleStatus
from utils.logger import get_logger, redact_error_detail
from storage.sqlite.fills_repo import FillRecord

_log = get_logger("ReconciliationService")


# ---------------------------------------------------------------------------
# Protocol-based skeleton (existing tests + V2 gateway path)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ReconciliationResult:
    order_id: str
    is_resolved: bool
    status: OrderLifecycleStatus
    error: str = ""

    @classmethod
    def resolved(cls, order_id: str, status: OrderLifecycleStatus) -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=True, status=status)

    @classmethod
    def pending(cls, order_id: str, status: OrderLifecycleStatus) -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=False, status=status)

    @classmethod
    def unresolved(cls, order_id: str, status: OrderLifecycleStatus, error: str = "") -> "ReconciliationResult":
        return cls(order_id=order_id, is_resolved=False, status=status, error=error)


class OrderRecord(Protocol):
    id: str
    venue_order_id: str | None
    reservation_id: str
    status: str
    market_id: str | None


class OrdersRepo(Protocol):
    async def get(self, order_id: str) -> OrderRecord: ...
    async def mark_submitted_unconfirmed(self, order_id: str) -> None: ...
    async def mark_terminal(self, order_id: str, status: str) -> None: ...
    async def mark_reconciliation_required(self, order_id: str) -> None: ...
    async def mark_filled_or_partially_filled(self, order_id: str) -> None: ...


class FillsRepo(Protocol):
    async def upsert_venue_fill(self, order: OrderRecord, venue_fill: Any) -> Any: ...


class PositionsRepo(Protocol):
    def upsert_position(self, fill: Any) -> None: ...


class ReservationService(Protocol):
    def release_terminal(self, reservation_id: str, reason: str) -> None: ...


# NOTE: The canonical CLOB venue gateway Protocol lives in
# ``interfaces.clob_gateway.ClobGateway``. ``OrderQueryGateway`` is the
# query-only subset used by the reconciler; satisfied by the canonical
# gateway directly (``get_order`` and ``get_fills`` signatures match).
# See issue #66 for context.
class OrderQueryGateway(Protocol):
    async def get_order(self, venue_order_id: str) -> Any: ...
    async def get_fills(self, market: str | None = None) -> list[Any]: ...


# Backward-compatible alias for any out-of-tree imports that referenced the
# previous local name. New code should import ``OrderQueryGateway`` directly
# or the canonical ``interfaces.clob_gateway.ClobGateway``.
ClobGateway = OrderQueryGateway


class DbTransaction(Protocol):
    async def __aenter__(self) -> None: ...
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None: ...


class DbProvider(Protocol):
    def transaction(self) -> DbTransaction: ...


# ---------------------------------------------------------------------------
# Constants shared by the concrete record paths
# ---------------------------------------------------------------------------
_TERMINAL_NO_FILL_STATUSES = frozenset({"cancelled", "canceled", "expired", "rejected"})
_FINAL_FILL_STATUSES_TRADE = frozenset({"filled", "matched"})
# Risk reconcile historically treated empty status + matched > 0 as final fill
# (see services/riskMonitor.reconcile_unconfirmed_risk_orders) — preserve.
_FINAL_FILL_STATUSES_RISK = frozenset({"filled", "matched", ""})


def _decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _matched_amount_from_response(payload: dict[str, Any] | None) -> Decimal:
    if not isinstance(payload, dict):
        return Decimal("0")
    return _decimal_or_zero(
        payload.get("size_matched")
        or payload.get("filled_size")
        or payload.get("size")
        or 0
    )


ConfirmFillCallable = Callable[[str], Awaitable[dict[str, Any]]]


# ---------------------------------------------------------------------------
# Canonical reconciler
# ---------------------------------------------------------------------------
class ReconciliationService:
    """Sole owner of terminal order settlement.

    Constructed once per user runtime; safe to share across
    ``trade_pipeline`` and ``riskMonitor`` callers. The ``orders_repo`` /
    ``fills_repo`` / ``positions_repo`` arguments accept either the
    Protocol-based mocks (used by ``reconcile_order``) or the concrete
    ``storage.sqlite.*`` repos (used by ``reconcile_trade_record`` and
    ``reconcile_risk_record``).
    """

    def __init__(
        self,
        orders_repo: Any,
        fills_repo: Any,
        positions_repo: Any,
        reservation_service: Any | None = None,
        clob_gateway: Any | None = None,
        db: Any | None = None,
        audit_logger: Any | None = None,
        risk_blocks_repo: Any | None = None,
        risk_triggers_repo: Any | None = None,
    ):
        self.orders_repo = orders_repo
        self.fills_repo = fills_repo
        self.positions_repo = positions_repo
        self.reservation_service = reservation_service
        self.clob_gateway = clob_gateway
        self.db = db
        self.audit_logger = audit_logger
        self.risk_blocks_repo = risk_blocks_repo
        self.risk_triggers_repo = risk_triggers_repo

    # ------------------------------------------------------------------
    # Fill confirmation (venue poll)
    # ------------------------------------------------------------------
    async def confirm_fill(self, order_id: str) -> dict[str, Any]:
        """Poll the CLOB gateway for order status.

        Returns a dict with at least ``status`` and ``size_matched`` keys.
        This replaces the former ``ExecutionService._confirm_fill`` so that
        the reconciler owns the full settlement lifecycle (issue #71).
        """
        if self.clob_gateway is None:
            return {"status": "unknown", "size_matched": "0"}
        try:
            venue_state = await self.clob_gateway.get_order(order_id)
            if venue_state is None:
                return {"status": "unknown", "size_matched": "0"}
            # VenueOrderState or raw dict — normalise to a plain dict.
            if isinstance(venue_state, dict):
                return venue_state
            # Protocol-based VenueOrderState — convert to dict.
            return {
                "status": getattr(venue_state, "status", "unknown"),
                "size_matched": str(getattr(venue_state, "size_matched", "0")),
                "avg_price": str(getattr(venue_state, "avg_price", "0")),
            }
        except Exception as exc:
            _log.warning("confirm_fill poll failed for %s: %s", order_id, redact_error_detail(exc))
            return {"status": "unknown", "size_matched": "0"}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _audit(self, event: str, payload: dict[str, Any]) -> None:
        logger = self.audit_logger
        if logger is None:
            return
        try:
            logger.log_event(event, payload)
        except Exception:
            # Audit logging must never break reconciliation.
            pass

    def _record_negative_delta_anomaly(
        self,
        *,
        side: str,
        order_id: str,
        current_matched: Decimal,
        previous_matched: Decimal,
        logger: Any = None,
    ) -> None:
        """Surface a venue-state regression.

        ``current_matched < previous_matched`` means the venue reported a
        cumulative matched size smaller than what we have already applied
        locally. This should never happen in normal operation — Polymarket
        only increments cumulative matched. Clamp to zero (caller does that)
        but make the anomaly observable so operators can investigate.
        """
        emit_logger = logger if logger is not None else _log
        try:
            emit_logger.warning(
                "reconciliation_negative_delta_anomaly | order_id=%s side=%s "
                "current_matched=%s previous_matched=%s",
                order_id,
                side,
                str(current_matched),
                str(previous_matched),
                extra={
                    "type": "reconciliation_negative_delta_anomaly",
                    "order_id": order_id,
                    "side": side,
                    "current_matched": str(current_matched),
                    "previous_matched": str(previous_matched),
                },
            )
        except Exception:
            pass
        self._audit(
            "reconciliation_negative_delta_anomaly",
            {
                "order_id": order_id,
                "side": side,
                "current_matched": str(current_matched),
                "previous_matched": str(previous_matched),
            },
        )
        try:
            from services.metrics import (
                METRIC_RECONCILIATION_NEGATIVE_DELTA,
                metrics,
            )

            metrics.inc_sync(
                METRIC_RECONCILIATION_NEGATIVE_DELTA,
                labels={"side": str(side or "unknown")},
            )
        except Exception:
            pass

    @staticmethod
    def _previous_matched_for_order(orders_repo: Any, order_id: str) -> Decimal:
        if not order_id:
            return Decimal("0")
        getter = getattr(orders_repo, "get_by_order_id", None)
        if getter is None:
            return Decimal("0")
        try:
            existing = getter(order_id)
        except Exception:
            return Decimal("0")
        if not isinstance(existing, dict):
            return Decimal("0")
        return _matched_amount_from_response(existing.get("response"))

    @staticmethod
    def _avg_price_from_fill(fill_info: dict[str, Any]) -> Decimal:
        raw = (
            fill_info.get("avg_price")
            or fill_info.get("price")
            or fill_info.get("average_price")
            or 0
        )
        try:
            return Decimal(str(raw))
        except Exception:
            return Decimal("0")

    # ------------------------------------------------------------------
    # Protocol-based path (kept for unit tests + V2 gateway)
    # ------------------------------------------------------------------
    async def reconcile_order(self, order_id: str) -> ReconciliationResult:
        order = await self.orders_repo.get(order_id)

        if not order.venue_order_id:
            await self.orders_repo.mark_reconciliation_required(order.id)
            return ReconciliationResult.unresolved(
                order.id, OrderLifecycleStatus.RECONCILIATION_REQUIRED, "no_venue_order_id"
            )

        try:
            venue_order = await self.clob_gateway.get_order(order.venue_order_id)
            venue_fills = await self.clob_gateway.get_fills(market=order.market_id)
        except Exception as exc:
            self._audit("reconciliation_unreachable", {"order_id": order.id, "error": str(exc)})
            return ReconciliationResult.unresolved(
                order.id, OrderLifecycleStatus.UNKNOWN_UNCONFIRMED, str(exc)
            )

        if venue_fills:
            async with self.db.transaction():
                for venue_fill in venue_fills:
                    fill = await self.fills_repo.upsert_venue_fill(order, venue_fill)
                    self.positions_repo.upsert_position(fill)

                await self.orders_repo.mark_filled_or_partially_filled(order.id)
                if self.reservation_service is not None:
                    self.reservation_service.release_terminal(
                        order.reservation_id,
                        reason="venue_fill_confirmed",
                    )
            self._audit("reconciliation_resolved_filled", {"order_id": order.id})
            return ReconciliationResult.resolved(order.id, OrderLifecycleStatus.FILLED)

        if venue_order and venue_order.status.lower() in {"live", "open", "pending"}:
            await self.orders_repo.mark_submitted_unconfirmed(order.id)
            return ReconciliationResult.pending(order.id, OrderLifecycleStatus.SUBMITTED_UNCONFIRMED)

        if venue_order and venue_order.status.lower() in {"cancelled", "expired", "rejected"}:
            async with self.db.transaction():
                await self.orders_repo.mark_terminal(order.id, venue_order.status.lower())
                if self.reservation_service is not None:
                    self.reservation_service.release_terminal(
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
    async def reconcile_trade_record(
        self,
        record: dict[str, Any],
        side: str,
        *,
        confirm_fill: ConfirmFillCallable,
        pnl_tracker: Any | None = None,
        get_position: Callable[..., Any] | None = None,
        logger: Any | None = None,
    ) -> bool:
        """Reconcile one BUY/SELL processed_trade order.

        Returns True if the order was reconciled (fill applied or retry
        unlocked); False otherwise. Logs go through ``logger`` if supplied so
        callers retain their existing trace tags.
        """
        order_id = record.get("order_id")
        if not order_id:
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

        order_status = str(fill_info.get("status", "")).lower()
        current_matched = _matched_amount_from_response(fill_info)
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

        previous_matched = self._previous_matched_for_order(self.orders_repo, order_id)
        delta_matched = current_matched - previous_matched

        if current_matched < previous_matched:
            self._record_negative_delta_anomaly(
                side=side,
                order_id=order_id,
                current_matched=current_matched,
                previous_matched=previous_matched,
                logger=logger,
            )

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
            self.orders_repo.delete_processed_trade(record["trade_key"])
            self._audit(
                "reconciliation_no_fill_unlock",
                {"order_id": order_id, "side": side, "venue_status": order_status},
            )
            return True

        if delta_matched < 0:
            delta_matched = Decimal("0")

        is_final_fill = order_status in _FINAL_FILL_STATUSES_TRADE or (
            order_status in _TERMINAL_NO_FILL_STATUSES and current_matched > 0
        )

        if delta_matched <= 0:
            if is_final_fill:
                # Cumulative matched already accounted for — persist terminal
                # state without double-applying the fill.
                try:
                    self.orders_repo.update_submission_status(
                        order_id=order_id,
                        status="filled_reconciled",
                        response_patch=fill_info,
                    )
                except Exception:
                    pass
                self.orders_repo.mark_processed_trade(
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
                    {"order_id": order_id, "side": side, "matched_total": str(current_matched)},
                )
                return True
            return False

        avg_price = self._avg_price_from_fill(fill_info)
        if avg_price <= 0:
            return False

        try:
            self.fills_repo.append_fill(
                FillRecord(
                    user_id=record["user_id"],
                    leader_id=record.get("leader_id"),
                    order_id=order_id,
                    condition_id=record["condition_id"],
                    token_id=record["asset"],
                    side=side,
                    size_tokens=delta_matched,
                    price=avg_price,
                    fee=Decimal("0"),
                    fill_ts=int(time.time()),
                ),
                raw_payload={
                    "reconciled_from": "submitted_unconfirmed",
                    "copy_order_id": order_id,
                    "status": "reconciled",
                    "side": side,
                },
            )

            entry_price: float | None = None
            if side == "SELL" and get_position is not None:
                pos = get_position(record["user_id"], record["condition_id"], token_id=record["asset"])
                if pos is not None:
                    entry_raw = getattr(pos, "avg_entry_price", None)
                    if entry_raw is not None:
                        try:
                            entry_price = float(entry_raw)
                        except (TypeError, ValueError):
                            entry_price = None

            self.positions_repo.apply_fill(
                user_id=record["user_id"],
                leader_id=record.get("leader_id"),
                condition_id=record["condition_id"],
                token_id=record["asset"],
                side=side,
                fill_size=delta_matched,
                fill_price=avg_price,
            )

            if side == "SELL" and pnl_tracker is not None:
                realized_source = "entry_price"
                if entry_price is None:
                    if logger is not None:
                        logger.warning(
                            "entry_price missing, using fill price for realized PnL | trade_key=%s cond=%s asset=%s",
                            record.get("trade_key"),
                            record.get("condition_id"),
                            record.get("asset"),
                        )
                    entry_price = float(avg_price)
                    realized_source = "fill_price_fallback"
                realized = (float(avg_price) - entry_price) * float(delta_matched)
                pnl_tracker.record_sell(
                    condition_id=record["condition_id"],
                    price=float(avg_price),
                    size_tokens=float(delta_matched),
                    usdc_value=float(delta_matched * avg_price),
                    entry_price=entry_price,
                    realized_pnl=realized,
                    exit_reason="reconciled_sell",
                    leader_wallet=record.get("leader_id", ""),
                    event_slug="",
                    title=f"realized_source={realized_source}",
                )

        except Exception as exc:
            if logger is not None:
                logger.warning(
                    "reconcile_unconfirmed_apply_error | side=%s order_id=%s error=%s",
                    side,
                    order_id,
                    repr(exc),
                )
            return False

        if is_final_fill:
            self.orders_repo.mark_processed_trade(
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
        else:
            submission_status = "submitted_unconfirmed"

        try:
            self.orders_repo.update_submission_status(
                order_id=order_id,
                status=submission_status,
                response_patch=fill_info,
            )
        except Exception:
            pass

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
            from services.metrics import (
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
        return True

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
    ) -> bool:
        """Reconcile one risk-monitor-originated SELL order.

        Returns True when reconciliation produced any state convergence.
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

        venue_status = str(fill_info.get("status", "")).lower()
        current_matched = _matched_amount_from_response(fill_info)
        previous_matched = _matched_amount_from_response(record.get("response"))
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

        # Terminal no-fill: persist status, drop the in-flight reservation.
        if current_matched <= 0 and venue_status in _TERMINAL_NO_FILL_STATUSES:
            self.orders_repo.update_submission_status(
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
                    trigger_meta = dict(recovery_metadata) if isinstance(recovery_metadata, dict) else {}
                    trigger_meta["venue_status"] = venue_status
                    self.risk_triggers_repo.record_trigger(
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
            return True

        is_final_fill = venue_status in _FINAL_FILL_STATUSES_RISK or (
            venue_status in _TERMINAL_NO_FILL_STATUSES and current_matched > 0
        )

        if delta_matched <= 0 and not is_final_fill:
            return False

        if delta_matched <= 0 and is_final_fill:
            self.orders_repo.update_submission_status(
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
            return True

        avg_price = self._avg_price_from_fill(fill_info)
        if avg_price <= 0:
            return False

        reason = str(request.get("reason") or "risk_exit")
        leader_id = str(request.get("leader_id") or "risk-monitor")
        existing_pos = None
        if get_position is not None:
            try:
                existing_pos = get_position(user_id, condition_id, token_id=token_id)
            except Exception:
                existing_pos = None
        try:
            entry_price = float(getattr(existing_pos, "avg_entry_price", avg_price)) if existing_pos else float(avg_price)
        except (TypeError, ValueError):
            entry_price = float(avg_price)

        try:
            self.fills_repo.append_fill(
                FillRecord(
                    user_id=user_id,
                    leader_id=leader_id,
                    order_id=order_id,
                    condition_id=condition_id,
                    token_id=token_id,
                    side="SELL",
                    size_tokens=delta_matched,
                    price=avg_price,
                    fee=Decimal("0"),
                    fill_ts=int(time.time()),
                ),
                raw_payload={
                    "reconciled_from": "risk_submitted_unconfirmed",
                    "copy_order_id": order_id,
                    "status": "reconciled",
                    "side": "SELL",
                },
            )
            updated_pos = self.positions_repo.apply_fill(
                user_id=user_id,
                leader_id=leader_id,
                condition_id=condition_id,
                token_id=token_id,
                side="SELL",
                fill_size=delta_matched,
                fill_price=avg_price,
            )
        except Exception as exc:
            if logger is not None:
                logger.warning(
                    "risk_unconfirmed_reconcile_apply_error | user=%s order_id=%s error=%s",
                    user_id,
                    order_id,
                    repr(exc),
                )
            return False

        # Persist a re-entry block only after a confirmed full close.
        recovery_metadata = request.get("recovery_metadata")
        block_metadata = dict(recovery_metadata) if isinstance(recovery_metadata, dict) else {}
        block_metadata["fill_price"] = float(avg_price)
        block_metadata["fill_size_tokens"] = float(delta_matched)
        block_metadata["fill_order_id"] = order_id
        if (
            is_final_fill
            and updated_pos is None
            and self.risk_blocks_repo is not None
        ):
            self.risk_blocks_repo.mark_risk_block(
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

        if self.risk_triggers_repo is not None:
            try:
                trigger_fill_status = "filled" if (is_final_fill and updated_pos is None) else "partial"
                self.risk_triggers_repo.record_trigger(
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

        if pnl_tracker is not None and hasattr(pnl_tracker, "record_sell"):
            realized = (float(avg_price) - entry_price) * float(delta_matched)
            pnl_tracker.record_sell(
                condition_id=condition_id,
                price=float(avg_price),
                size_tokens=float(delta_matched),
                usdc_value=float(delta_matched * avg_price),
                entry_price=entry_price,
                realized_pnl=realized,
                exit_reason=reason,
                leader_wallet=leader_id,
            )

        submission_status = "filled_reconciled" if is_final_fill else "submitted_unconfirmed"
        self.orders_repo.update_submission_status(
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
                user_id, condition_id, token_id, repr(exc),
            )
            self._audit(
                "manual_position_drift_fetch_failed",
                {"user_id": user_id, "condition_id": condition_id, "token_id": token_id, "error": str(exc)},
            )
            return result

        result["live_size"] = live_size if live_size is not None else Decimal("0")

        if live_size is None:
            return result

        existing = self.positions_repo.get_position(user_id, condition_id, token_id=token_id)
        local_size = getattr(existing, "size_tokens", Decimal("0")) if existing else Decimal("0")

        if local_size <= Decimal("1e-12") and live_size <= Decimal("1e-12"):
            result["action"] = "no_drift"
            return result

        if abs(local_size - live_size) <= Decimal("1e-12"):
            result["action"] = "no_drift"
            return result

        avg_entry_price = getattr(existing, "avg_entry_price", Decimal("0")) if existing else Decimal("0")

        if live_size <= Decimal("1e-12"):
            self.positions_repo.delete_position(user_id, condition_id, token_id=token_id)
            result["action"] = "deleted"
            result["reconciled"] = True

            if reservation_service is not None:
                try:
                    reservations_repo = getattr(reservation_service, "reservations_repo", None)
                    if reservations_repo is not None:
                        active_reservations = reservations_repo.list_active_for_scope(
                            user_id=user_id,
                            condition_id=condition_id,
                            token_id=token_id,
                        )
                        for res in active_reservations:
                            if res.side.value == "SELL":
                                reservation_service.release_terminal(
                                    res.id, reason="manual_position_drift_deleted"
                                )
                except Exception:
                    _log.debug("drift_reservation_cleanup_failed", exc_info=True)
        else:
            from storage.sqlite.positions_repo import PositionRecord
            self.positions_repo.upsert_position(
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
                        active_reservations = reservations_repo.list_active_for_scope(
                            user_id=user_id,
                            condition_id=condition_id,
                            token_id=token_id,
                        )
                        for res in active_reservations:
                            if res.side.value == "SELL" and res.reserved_size > live_size:
                                reservation_service.release_terminal(
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
            user_id, condition_id, token_id, str(local_size), str(result["live_size"]), result["action"],
        )
        return result
