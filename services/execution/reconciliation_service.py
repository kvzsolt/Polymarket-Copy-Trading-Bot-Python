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
from storage.postgres.fills_repo import FillRecord

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
_TERMINAL_NO_FILL_STATUSES = frozenset({
    "cancelled", "canceled", "expired", "rejected", "failed",
    "order_status_canceled", "order_status_canceled_market_resolved",
    "order_status_invalid",
})
_FINAL_FILL_STATUSES_TRADE = frozenset({"filled", "matched", "confirmed"})
# Risk reconcile historically treated empty status + matched > 0 as final fill
# (see services/riskMonitor.reconcile_unconfirmed_risk_orders) — preserve.
_FINAL_FILL_STATUSES_RISK = frozenset({"filled", "matched", "confirmed", ""})
# Non-terminal intermediate statuses that should keep the order row alive in
# its current submitted/delayed/live state. Polymarket POST /order can
# return live/matched/delayed; ``matched``/`order_status_matched`/`filled` are
# treated as terminal fill by the reconciler (existing contract). WSS trade
# events report MATCHED / MINED / RETRYING and are handled in UserWebSocket;
# the reconciler only sees POST-poll results.
_VENUE_NON_TERMINAL = frozenset({
    "live", "delayed",
    "order_status_live",
    "submitted", "submitted_unconfirmed", "submitted_delayed",
    "order_live", "trade_mined", "trade_retrying",
    "mined", "retrying",
})

# Map normalised venue status -> (local submission status, lifecycle log status).
_VENUE_TO_LIFECYCLE: dict[str, tuple[str, str]] = {
    "live": ("order_live", "live"),
    "order_status_live": ("order_live", "live"),
    "order_live": ("order_live", "live"),
    "delayed": ("submitted_delayed", "delayed"),
    "submitted_delayed": ("submitted_delayed", "delayed"),
    "matched": ("matched_pending_confirmation", "matched"),
    "order_status_matched": ("matched_pending_confirmation", "matched"),
    "matched_pending_confirmation": ("matched_pending_confirmation", "matched"),
    "mined": ("trade_mined", "mined"),
    "trade_mined": ("trade_mined", "mined"),
    "retrying": ("trade_retrying", "retrying"),
    "trade_retrying": ("trade_retrying", "retrying"),
    "confirmed": ("filled_reconciled", "confirmed"),
    "trade_confirmed": ("filled_reconciled", "confirmed"),
    "filled": ("filled_reconciled", "confirmed"),
    "cancelled": ("cancelled", "cancelled"),
    "canceled": ("cancelled", "cancelled"),
    "order_status_canceled": ("cancelled", "cancelled"),
    "order_status_canceled_market_resolved": ("cancelled", "cancelled"),
    "rejected": ("rejected", "failed"),
    "order_status_invalid": ("rejected", "failed"),
    "failed": ("failed", "failed"),
    "expired": ("expired", "failed"),
    "submitted_unconfirmed": ("submitted_unconfirmed", "submitted_unconfirmed"),
    "submitted": ("submitted_unconfirmed", "submitted_unconfirmed"),
}


def _normalize_venue_status(raw: Any) -> str:
    return str(raw or "").strip().lower()


def _record_created_at(record: dict[str, Any]) -> float | None:
    """Best-effort timestamp from a processed_trade / order row."""
    for key in ("created_at", "createdAt", "submitted_at", "submittedAt", "timestamp"):
        value = record.get(key)
        if value is None:
            continue
        try:
            ts = float(value)
        except (TypeError, ValueError):
            continue
        if ts > 1e12:
            ts /= 1000.0
        return ts
    return None


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
    ``storage.postgres.*`` repos (used by ``reconcile_trade_record`` and
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

    async def _release_reservation_if_present(
        self,
        record: dict[str, Any],
        *,
        reason: str,
    ) -> None:
        """Release a reservation when the record carries reservation_id.

        Coordinator-path orders populate this either at the top level of
        ``request`` or inside ``request.recovery_metadata`` (PR6). Falls back
        to the top-level ``record["reservation_id"]`` if neither is present.
        Safe no-op when ``reservation_service`` is unset.
        """
        if self.reservation_service is None:
            return
        request = record.get("request") if isinstance(record, dict) else None
        reservation_id: Any = None
        if isinstance(request, dict):
            reservation_id = request.get("reservation_id")
            if not reservation_id:
                recovery = request.get("recovery_metadata")
                if isinstance(recovery, dict):
                    reservation_id = recovery.get("reservation_id")
        if not reservation_id:
            reservation_id = record.get("reservation_id")
        if not reservation_id:
            return
        try:
            await self.reservation_service.release_terminal(str(reservation_id), reason=reason)
        except Exception as exc:
            _log.warning(
                "reconciliation_reservation_release_failed | reservation_id=%s reason=%s error=%r",
                reservation_id, reason, exc,
            )

    def _emit_lifecycle(
        self,
        *,
        status: str,
        side: str,
        record: dict[str, Any],
        order_id: str | None,
        venue_status: str | None = None,
        size: Any = None,
        price: Any = None,
        usdc_value: Any = None,
        signal_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """Emit an order_lifecycle log via the canonical helper.

        Best-effort: never breaks reconciliation. The helper deduplicates by
        (order_id|signal_id|condition_id, status), so repeated reconciliation
        passes never re-render the same panel.
        """
        try:
            from utils.logger import log_order_lifecycle as _log_lc

            _log_lc(
                _log,
                status=status,
                side=side,
                condition_id=record.get("condition_id") or record.get("conditionId"),
                asset=record.get("asset") or record.get("token_id"),
                order_id=order_id,
                size=float(size) if size is not None else None,
                price=float(price) if price is not None else None,
                usdc_value=float(usdc_value) if usdc_value is not None else None,
                venue_status=venue_status,
                signal_id=signal_id or record.get("trade_key") or record.get("signal_id"),
                meta=meta,
            )
        except Exception:
            pass

    def _should_skip_for_delay(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
        last_known_venue_status: str | None = None,
    ) -> bool:
        """Return True if the reconciler should defer polling for this record.

        Only applies before the configured ``initial_delay_seconds`` elapses
        AND while no fill has been recorded locally. Once a venue status has
        been observed at least once (i.e. the record carries a non-empty
        response.status that's not the pending placeholder) the delay no
        longer applies.
        """
        if order_confirmation_cfg is None:
            return False

        created_at = _record_created_at(record)
        if created_at is None:
            return False

        token_id = str(record.get("asset") or record.get("token_id") or "")
        condition_id = str(record.get("condition_id") or record.get("conditionId") or "")
        delay = order_confirmation_cfg.resolve_initial_delay(
            token_id=token_id,
            condition_id=condition_id,
            venue_status=last_known_venue_status,
        )
        # Treat any existing matched-total > 0 as "already observed", skip the delay.
        previous_matched = _matched_amount_from_response(record.get("response"))
        if previous_matched > 0:
            return False
        elapsed = time.time() - created_at
        return elapsed < delay

    def _is_past_ambiguous_timeout(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
    ) -> bool:
        if order_confirmation_cfg is None:
            return False
        created_at = _record_created_at(record)
        if created_at is None:
            return False
        return (time.time() - created_at) >= float(order_confirmation_cfg.max_wait_seconds)

    def _is_past_auto_release_grace(
        self,
        record: dict[str, Any],
        *,
        order_confirmation_cfg: Any | None,
    ) -> bool:
        if order_confirmation_cfg is None:
            return False
        if not getattr(order_confirmation_cfg, "auto_release_ambiguous", False):
            return False
        created_at = _record_created_at(record)
        if created_at is None:
            return False
        threshold = (
            float(order_confirmation_cfg.max_wait_seconds)
            + float(order_confirmation_cfg.auto_release_grace_seconds)
        )
        return (time.time() - created_at) >= threshold

    async def _run_auto_release_probes(
        self,
        *,
        order_id: str,
        venue_order_id: str | None,
        market_id: str | None,
        user_id: str,
        condition_id: str,
        token_id: str,
        expected_size: Decimal,
    ) -> dict[str, Any]:
        """Run all four safety probes. Returns dict with probe outcomes.

        Each probe is classified as clean/inconclusive/error. The caller
        decides whether to auto-release based on the configured policy.
        """
        result: dict[str, Any] = {"errors": 0, "inconclusive": 0, "clean": 0, "details": {}}

        gw = self.clob_gateway
        if gw is None:
            result["errors"] += 1
            result["details"]["clob_gateway_missing"] = True
            return result

        # Probe 1: get_order
        try:
            venue_state = await gw.get_order(venue_order_id or order_id)
            status = _normalize_venue_status(
                getattr(venue_state, "status", None)
                if not isinstance(venue_state, dict)
                else venue_state.get("status")
            )
            if status in ("live", "matched", "delayed", "order_status_live", "order_status_matched"):
                result["inconclusive"] += 1
                result["details"]["get_order"] = status
            else:
                result["clean"] += 1
                result["details"]["get_order"] = status or "absent"
        except Exception as exc:
            result["errors"] += 1
            result["details"]["get_order_error"] = repr(exc)

        # Probe 2: get_orders_for_market (optional)
        list_orders = getattr(gw, "get_orders_for_market", None)
        if callable(list_orders):
            try:
                orders = await list_orders(market_id) if market_id else []
                ids = {str(o.get("id") if isinstance(o, dict) else getattr(o, "id", "")) for o in orders or []}
                if order_id in ids or (venue_order_id and venue_order_id in ids):
                    result["inconclusive"] += 1
                    result["details"]["get_orders_for_market"] = "order_present"
                else:
                    result["clean"] += 1
                    result["details"]["get_orders_for_market"] = "absent"
            except Exception as exc:
                result["errors"] += 1
                result["details"]["get_orders_for_market_error"] = repr(exc)
        else:
            result["inconclusive"] += 1
            result["details"]["get_orders_for_market"] = "not_supported"

        # Probe 3: get_fills
        try:
            fills = await gw.get_fills(market=market_id)
            related = [
                f for f in fills or []
                if str(f.get("order_id") if isinstance(f, dict) else getattr(f, "order_id", "")) == order_id
            ]
            if related:
                result["inconclusive"] += 1
                result["details"]["get_fills"] = f"fills_present:{len(related)}"
            else:
                result["clean"] += 1
                result["details"]["get_fills"] = "absent"
        except Exception as exc:
            result["errors"] += 1
            result["details"]["get_fills_error"] = repr(exc)

        # Probe 4: local position delta
        try:
            pos = await self.positions_repo.get_position(user_id, condition_id, token_id=token_id)
            local_size = Decimal(str(getattr(pos, "size_tokens", 0) or 0))
            if local_size >= expected_size > 0:
                # Position already absorbed at least the expected size — order
                # may have filled. Conservative: inconclusive.
                result["inconclusive"] += 1
                result["details"]["position_delta"] = f"local_size={local_size}"
            else:
                result["clean"] += 1
                result["details"]["position_delta"] = f"local_size={local_size}"
        except Exception as exc:
            result["errors"] += 1
            result["details"]["position_delta_error"] = repr(exc)

        return result

    async def _maybe_auto_release_ambiguous(
        self,
        record: dict[str, Any],
        *,
        side: str,
        order_confirmation_cfg: Any,
    ) -> bool:
        """Run safety probes and auto-release the reservation when clean.

        Returns True if the auto-release fired.
        """
        order_id = str(record.get("order_id") or "")
        if not order_id:
            return False

        request = record.get("request") or {}
        venue_order_id = str(record.get("venue_order_id") or order_id)
        market_id = str(request.get("market") or request.get("market_id") or "") or None
        user_id = str(record.get("user_id") or request.get("user_id") or "")
        condition_id = str(record.get("condition_id") or request.get("condition_id") or "")
        token_id = str(record.get("asset") or request.get("token_id") or "")
        try:
            expected_size = Decimal(str(request.get("amount") or request.get("size") or "0"))
        except Exception:
            expected_size = Decimal("0")

        probe = await self._run_auto_release_probes(
            order_id=order_id,
            venue_order_id=venue_order_id,
            market_id=market_id,
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
            expected_size=expected_size,
        )

        max_errors = int(order_confirmation_cfg.auto_release_max_probe_errors)
        require_clean = bool(order_confirmation_cfg.auto_release_require_all_probes_clean)
        if probe["errors"] > max_errors:
            _log.warning(
                "auto_release_aborted_probe_errors | order_id=%s errors=%d details=%s",
                order_id, probe["errors"], probe["details"],
            )
            self._audit(
                "reconciliation_auto_release_aborted",
                {"order_id": order_id, "reason": "probe_errors", "probe": probe},
            )
            return False
        if require_clean and probe["inconclusive"] > 0:
            _log.info(
                "auto_release_aborted_inconclusive | order_id=%s details=%s",
                order_id, probe["details"],
            )
            self._audit(
                "reconciliation_auto_release_aborted",
                {"order_id": order_id, "reason": "inconclusive", "probe": probe},
            )
            return False

        # All probes clean — release.
        try:
            await self.orders_repo.update_submission_status(
                order_id=order_id,
                status="auto_released_ambiguous",
                response_patch={"probe": probe},
            )
        except Exception:
            pass

        reservation_id = record.get("reservation_id") or request.get("reservation_id")
        if reservation_id and self.reservation_service is not None:
            try:
                await self.reservation_service.release_terminal(
                    str(reservation_id),
                    reason="auto_release_after_ambiguous_timeout",
                )
            except Exception as exc:
                _log.warning(
                    "auto_release_reservation_release_failed | order_id=%s error=%r",
                    order_id, exc,
                )

        self._emit_lifecycle(
            status="auto_released_ambiguous",
            side=side,
            record=record,
            order_id=order_id,
            venue_status="ambiguous_timeout",
        )
        self._audit(
            "reconciliation_auto_release_ambiguous",
            {"order_id": order_id, "probe": probe},
        )
        try:
            from services.metrics import (
                METRIC_RECONCILIATION_AUTO_RELEASE_AMBIGUOUS,
                metrics,
            )

            metrics.inc_sync(
                METRIC_RECONCILIATION_AUTO_RELEASE_AMBIGUOUS,
                labels={"side": (side or "unknown").upper()},
            )
        except Exception:
            pass
        return True

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
    async def _previous_matched_for_order(orders_repo: Any, order_id: str) -> Decimal:
        if not order_id:
            return Decimal("0")
        getter = getattr(orders_repo, "get_by_order_id", None)
        if getter is None:
            return Decimal("0")
        try:
            existing = await getter(order_id)
        except Exception:
            return Decimal("0")
        if not isinstance(existing, dict):
            return Decimal("0")
        return _matched_amount_from_response(existing.get("response"))

    async def _resolve_buy_requested_amount(self, order_id: str) -> Decimal:
        """Return the requested BUY notional from the persisted orders row.

        Used by the terminal-no-fill branch to release the eagerly recorded
        daily-cap spend back to the regime ledger. Reads ``request_json.amount``
        (falling back to ``size``) on the local orders row.
        """
        if not order_id:
            return Decimal("0")
        getter = getattr(self.orders_repo, "get_by_order_id", None)
        if getter is None:
            return Decimal("0")
        try:
            row = await getter(order_id)
        except Exception:
            return Decimal("0")
        if not isinstance(row, dict):
            return Decimal("0")
        request = row.get("request") or {}
        if not isinstance(request, dict):
            return Decimal("0")
        raw = request.get("amount") if request.get("amount") is not None else request.get("size")
        try:
            return Decimal(str(raw or 0))
        except Exception:
            return Decimal("0")

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
            # Issue #97: filter fills to the exact venue_order_id before
            # applying. get_fills(market=...) returns ALL fills for the
            # market — applying every one to the target order corrupts
            # position state and can trigger premature reservation release.
            # Invariant: every applied fill MUST carry an order_id that
            # matches order.venue_order_id. Fills missing an order_id are
            # inconclusive and must NOT be attributed to this order.
            matching_fills = [
                f for f in venue_fills
                if (
                    f.get("order_id") or f.get("orderID")
                    if isinstance(f, dict)
                    else getattr(f, "order_id", None)
                ) == order.venue_order_id
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
            return ReconciliationResult.pending(order.id, OrderLifecycleStatus.SUBMITTED_UNCONFIRMED)

        if venue_order and venue_order.status.lower() in {"cancelled", "expired", "rejected"}:
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
    ) -> bool:
        """Reconcile one BUY/SELL processed_trade order.

        Returns True if the order was reconciled (fill applied or retry
        unlocked); False otherwise. Logs go through ``logger`` if supplied so
        callers retain their existing trace tags.
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
            await self.orders_repo.delete_processed_trade(record["trade_key"])
            self._audit(
                "reconciliation_no_fill_unlock",
                {"order_id": order_id, "side": side, "venue_status": order_status},
            )
            # Release reservation if the record carries one (coordinator path).
            await self._release_reservation_if_present(record, reason=order_status)
            # Lifecycle log — render the canonical terminal-failure panel.
            mapped = _VENUE_TO_LIFECYCLE.get(order_status, ("failed", "failed"))
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

        if delta_matched <= 0:
            if is_final_fill:
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
                        {"order_id": order_id, "side": side, "matched_total": str(current_matched)},
                    )
                    await self._release_reservation_if_present(record, reason="confirmed")
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
        if avg_price <= 0:
            return False

        try:
            await self.fills_repo.append_fill(
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
                pos = await get_position(record["user_id"], record["condition_id"], token_id=record["asset"])
                if pos is not None:
                    entry_raw = getattr(pos, "avg_entry_price", None)
                    if entry_raw is not None:
                        try:
                            entry_price = float(entry_raw)
                        except (TypeError, ValueError):
                            entry_price = None

            await self.positions_repo.apply_fill(
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
                # Compute realized PnL in Decimal to avoid IEEE-754 drift on
                # tick-granular subtractions; cast at the tracker boundary only.
                entry_d = Decimal(str(entry_price))
                realized_d = (avg_price - entry_d) * delta_matched
                pnl_tracker.record_sell(
                    condition_id=record["condition_id"],
                    price=float(avg_price),
                    size_tokens=float(delta_matched),
                    usdc_value=float(delta_matched * avg_price),
                    entry_price=entry_price,
                    realized_pnl=float(realized_d),
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
            self._emit_lifecycle(
                status="confirmed",
                side=side,
                record=record,
                order_id=order_id,
                venue_status=order_status,
                size=current_matched,
                price=avg_price,
                usdc_value=float(current_matched * avg_price) if current_matched and avg_price else None,
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

        venue_status = _normalize_venue_status(fill_info.get("status", ""))
        current_matched = _matched_amount_from_response(fill_info)
        # CRITICAL: read previous matched from the persisted orders row,
        # not the in-memory ``record["response"]``. Pre-fix, on restart
        # the in-memory record was rebuilt without the prior fill
        # response and ``previous_matched`` would silently fall back to
        # zero — re-applying the entire cumulative venue size as a
        # fresh delta and double-counting every risk SELL fill that was
        # in-flight across the restart.
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
                    trigger_meta = dict(recovery_metadata) if isinstance(recovery_metadata, dict) else {}
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
        if avg_price <= 0:
            return False

        reason = str(request.get("reason") or "risk_exit")
        leader_id = str(request.get("leader_id") or "risk-monitor")
        existing_pos = None
        if get_position is not None:
            try:
                existing_pos = await get_position(user_id, condition_id, token_id=token_id)
            except Exception:
                existing_pos = None
        try:
            entry_price = float(getattr(existing_pos, "avg_entry_price", avg_price)) if existing_pos else float(avg_price)
        except (TypeError, ValueError):
            entry_price = float(avg_price)

        try:
            await self.fills_repo.append_fill(
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
            updated_pos = await self.positions_repo.apply_fill(
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

        if self.risk_triggers_repo is not None:
            try:
                trigger_fill_status = "filled" if (is_final_fill and updated_pos is None) else "partial"
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

        if pnl_tracker is not None and hasattr(pnl_tracker, "record_sell"):
            # Realized PnL in Decimal to avoid float subtraction drift; cast
            # at the tracker boundary only.
            entry_d = Decimal(str(entry_price))
            realized_d = (avg_price - entry_d) * delta_matched
            pnl_tracker.record_sell(
                condition_id=condition_id,
                price=float(avg_price),
                size_tokens=float(delta_matched),
                usdc_value=float(delta_matched * avg_price),
                entry_price=entry_price,
                realized_pnl=float(realized_d),
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
            usdc_value=float(current_matched * avg_price) if current_matched and avg_price else None,
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

        existing = await self.positions_repo.get_position(user_id, condition_id, token_id=token_id)
        local_size = getattr(existing, "size_tokens", Decimal("0")) if existing else Decimal("0")

        if local_size <= Decimal("1e-12") and live_size <= Decimal("1e-12"):
            result["action"] = "no_drift"
            return result

        if abs(local_size - live_size) <= Decimal("1e-12"):
            result["action"] = "no_drift"
            return result

        avg_entry_price = getattr(existing, "avg_entry_price", Decimal("0")) if existing else Decimal("0")

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
            from storage.postgres.positions_repo import PositionRecord
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
            user_id, condition_id, token_id, str(local_size), str(result["live_size"]), result["action"],
        )
        return result
