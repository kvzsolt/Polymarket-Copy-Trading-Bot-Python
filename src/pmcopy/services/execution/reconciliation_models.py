"""Reconciliation models, repository protocols, status sets, and pure payload helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol

from pmcopy.domain.order_lifecycle import OrderLifecycleStatus
from pmcopy.interfaces.clob_gateway import OrderQueryGateway

# ---------------------------------------------------------------------------
# Protocol-based skeleton (existing tests + V2 gateway path)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ReconciliationResult:
    """Describe the result of one reconciliation operation."""

    order_id: str
    is_resolved: bool
    status: OrderLifecycleStatus
    error: str = ""

    @classmethod
    def resolved(cls, order_id: str, status: OrderLifecycleStatus) -> "ReconciliationResult":
        """Build a resolved reconciliation result."""
        return cls(order_id=order_id, is_resolved=True, status=status)

    @classmethod
    def pending(cls, order_id: str, status: OrderLifecycleStatus) -> "ReconciliationResult":
        """Build a result requiring further reconciliation evidence."""
        return cls(order_id=order_id, is_resolved=False, status=status)

    @classmethod
    def unresolved(
        cls, order_id: str, status: OrderLifecycleStatus, error: str = ""
    ) -> "ReconciliationResult":
        """Build an unresolved result with a sanitized failure reason."""
        return cls(order_id=order_id, is_resolved=False, status=status, error=error)


class OrderRecord(Protocol):
    """Describe persisted order state required by reconciliation."""

    id: str
    venue_order_id: str | None
    reservation_id: str
    status: str
    market_id: str | None


class OrdersRepo(Protocol):
    """Persist order and idempotency state in PostgreSQL."""

    async def get(self, order_id: str) -> OrderRecord:
        """Return a persisted order by identifier."""
        ...

    async def mark_submitted_unconfirmed(self, order_id: str) -> None:
        """Retain the order as submitted but awaiting venue evidence."""
        ...

    async def mark_terminal(self, order_id: str, status: str) -> None:
        """Persist an authoritative terminal order state."""
        ...

    async def mark_reconciliation_required(self, order_id: str) -> None:
        """Persist that an order requires additional reconciliation."""
        ...

    async def mark_filled_or_partially_filled(self, order_id: str) -> None:
        """Persist fill evidence without fabricating terminal state."""
        ...


class FillsRepo(Protocol):
    """Define the narrow venue-fill persistence port."""

    async def upsert_venue_fill(self, order: OrderRecord, venue_fill: Any) -> Any:
        """Persist venue fill evidence idempotently."""
        ...


class PositionsRepo(Protocol):
    """Define the narrow normalized-position persistence port."""

    def upsert_position(self, fill: Any) -> None:
        """Apply one normalized fill to durable position state."""
        ...


class ReservationService(Protocol):
    """Define the terminal reservation-release port."""

    async def release_terminal(self, reservation_id: str, reason: str) -> None:
        """Release a reservation only after terminal evidence."""

        ...


class LeaderSellTargetRedriver(Protocol):
    """Re-drive a newer SELL target after terminal no-fill proof."""

    async def redrive_after_terminal_no_fill(
        self,
        record: dict[str, Any],
        *,
        reason: str,
    ) -> bool:
        """Attempt one durably claimed target re-drive."""

        ...


# Backward-compatible alias for any out-of-tree imports that referenced the
# previous local name. New code should import ``OrderQueryGateway`` directly
# or the canonical ``interfaces.clob_gateway.ClobGateway``.
ClobGateway = OrderQueryGateway


# ---------------------------------------------------------------------------
# Constants shared by the concrete record paths
# ---------------------------------------------------------------------------
_TERMINAL_NO_FILL_STATUSES = frozenset(
    {
        "cancelled",
        "canceled",
        "expired",
        "rejected",
        "failed",
        "order_status_canceled",
        "order_status_canceled_market_resolved",
        "order_status_invalid",
    }
)
# Repeated 404s are tracked only to throttle observability. They never settle an
# order: a missing order lookup plus missing fallback evidence is inconclusive.
_VENUE_ORDER_NOT_FOUND_THRESHOLD = 3

# Market trade-history attribution (2026-09-19 incident class): an ambiguous
# POST retry signs a fresh venue order hash, so every direct probe keyed on the
# locally persisted order_id can be absent while the venue trade history for
# the market holds the actual fill. Attribution is only allowed for a settled
# order (min age), a bounded submission window, and an exact Decimal amount
# match, and only when exactly one candidate trade survives.
_MARKET_ATTRIBUTION_MIN_AGE_SECONDS = 30.0
_MARKET_ATTRIBUTION_PRE_WINDOW_SECONDS = 60.0
_MARKET_ATTRIBUTION_POST_WINDOW_SECONDS = 300.0
# Bounded number of order ids probed per reconciliation pass: the row's own
# order hash plus every persisted attempted id (2026-09-19 incident class).
_MAX_PROBE_ORDER_ID_CANDIDATES = 8

_FINAL_FILL_STATUSES_TRADE = frozenset({"filled", "confirmed"})
# Risk reconcile historically treated empty status + matched > 0 as final fill
# (see services/riskMonitor.reconcile_unconfirmed_risk_orders) — preserve.
_FINAL_FILL_STATUSES_RISK = frozenset({"filled", "confirmed", ""})
# Non-terminal intermediate statuses that should keep the order row alive in
# its current submitted/delayed/live state. Polymarket POST /order can
# return live/matched/delayed. ``matched``/``mined``/``retrying`` remain pending
# until authenticated trade history reports CONFIRMED or FAILED.
_VENUE_NON_TERMINAL = frozenset(
    {
        "live",
        "delayed",
        "order_status_live",
        "submitted",
        "submitted_unconfirmed",
        "submitted_delayed",
        "order_live",
        "matched",
        "order_status_matched",
        "matched_pending_confirmation",
        "trade_mined",
        "trade_retrying",
        "mined",
        "retrying",
        "poll_unavailable",
        "poll_deferred",
        "ambiguous_timeout",
        # P1-q: a marketable order that did not match after the matching delay is
        # placed ON THE BOOK as a resting order (Context7: order status 'unmatched'
        # = "marketable order placed on the book after the delay expired without a
        # match"). It is live / non-terminal — keep polling; do NOT fall through to
        # the ("failed","failed") default and settle a live resting order as failed.
        "unmatched",
    }
)

# Map normalised venue status -> (local submission status, lifecycle log status).
_VENUE_TO_LIFECYCLE: dict[str, tuple[str, str]] = {
    "live": ("order_live", "live"),
    "order_status_live": ("order_live", "live"),
    "order_live": ("order_live", "live"),
    # P1-q: 'unmatched' marketable order is resting on the book → treat as live.
    "unmatched": ("order_live", "live"),
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
    status = str(raw or "").strip().lower()
    return {"open": "live", "closed": "matched"}.get(status, status)


def _normalize_trade_status(raw: Any) -> str:
    status = _normalize_venue_status(raw)
    for prefix in ("trade_status_", "trade_"):
        if status.startswith(prefix):
            status = status[len(prefix) :]
    return status


def _record_created_at(record: dict[str, Any]) -> float | None:
    """Best-effort timestamp from a processed_trade / order row."""
    for key in (
        "created_at",
        "createdAt",
        "submitted_at",
        "submittedAt",
        "processed_at",
        "processedAt",
        "timestamp",
    ):
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, datetime):
            return value.timestamp()
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


# Statuses that mean the order can still rest on the book. A fully matched
# order is not resting, even when the venue text is still "live".
_RESTING_ORDER_STATUSES = frozenset(
    {
        "live",
        "unmatched",
        "partially_filled",
        "order_live",
        "order_status_live",
    }
)
_TERMINAL_FILLED_SUBMISSION_STATUSES = frozenset(
    {
        "filled_reconciled",
        "confirmed",
        "filled",
    }
)


def _state_value(state: Any, *names: str) -> Any:
    if state is None:
        return None
    if isinstance(state, dict):
        for name in names:
            if state.get(name) is not None:
                return state.get(name)
        return None
    for name in names:
        value = getattr(state, name, None)
        if value is not None:
            return value
    return None


def _state_original_size(state: Any) -> Decimal:
    return _decimal_or_zero(_state_value(state, "size", "original_size", "amount"))


def _state_filled_size(state: Any) -> Decimal:
    if isinstance(state, dict):
        # ``size`` on an order is the original amount, not the matched amount.
        return _decimal_or_zero(
            state.get("size_matched")
            or state.get("filled_size")
            or state.get("matched_size")
            or state.get("matched_amount")
            or state.get("sizeMatched")
            or state.get("filledSize")
            or 0
        )
    return _decimal_or_zero(getattr(state, "filled_size", None))


def _order_is_fully_filled(state: Any) -> bool:
    """Return whether the venue order has no remaining size."""

    size = _state_original_size(state)
    if size <= 0:
        return False
    return _state_filled_size(state) >= size


def _order_is_resting(state: Any) -> bool:
    """Return whether the order can still match more size."""

    if state is None or _order_is_fully_filled(state):
        return False
    status = _normalize_venue_status(_state_value(state, "status"))
    return status in _RESTING_ORDER_STATUSES


def _fills_cover_order(matched: Decimal, state: Any) -> bool:
    """Return whether confirmed trade size accounts for the whole order."""

    size = _state_original_size(state)
    return size > 0 and matched >= size


def _matched_amount_from_response(payload: dict[str, Any] | None) -> Decimal:
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


def _order_id_candidates(order_id: Any) -> list[str]:
    raw = str(order_id or "").strip()
    if not raw:
        return []
    candidates: list[str] = []
    for part in raw.split(","):
        candidate = part.strip()
        if candidate and candidate not in candidates:
            candidates.append(candidate)
    if raw not in candidates:
        candidates.insert(0, raw)
    return candidates


def _probe_order_id_candidates(record: dict[str, Any] | None, order_id: str | None) -> list[str]:
    """Return the bounded order ids a reconciliation pass may probe.

    The row's own order hash comes first, then every persisted
    ``attempted_order_ids`` entry (deduped, capped). A sibling attempt's hash is
    never the row's order id, so it can only be probed from this list.
    """
    candidates: list[str] = []
    raw_order_id = str(order_id or "").strip()
    if raw_order_id:
        candidates.append(raw_order_id)
    response = record.get("response") if isinstance(record, dict) else None
    if isinstance(response, dict):
        raw = response.get("attempted_order_ids")
        if isinstance(raw, str):
            raw = [part.strip() for part in raw.split(",")]
        if isinstance(raw, (list, tuple)):
            for item in raw:
                candidate = str(item or "").strip()
                if candidate and candidate not in candidates:
                    candidates.append(candidate)
    return candidates[:_MAX_PROBE_ORDER_ID_CANDIDATES]


def _order_id_set(orders: Any) -> set[str]:
    """Return the order ids present in a venue open-order batch."""
    ids: set[str] = set()
    for order in orders or []:
        value = (
            order.get("order_id") or order.get("id")
            if isinstance(order, dict)
            else getattr(order, "order_id", None) or getattr(order, "id", "")
        )
        text = str(value or "").strip()
        if text:
            ids.add(text)
    return ids


ConfirmFillCallable = Callable[[str], Awaitable[dict[str, Any]]]


class _ReconciliationMixinBase:
    """Shared static typing surface across ``ReconciliationService`` mixins."""

    if TYPE_CHECKING:
        orders_repo: Any
        fills_repo: Any
        positions_repo: Any
        reservation_service: Any | None
        clob_gateway: Any | None
        db: Any | None
        audit_logger: Any | None
        risk_blocks_repo: Any | None
        risk_triggers_repo: Any | None
        order_confirmation_cfg: Any | None
        task_budget_settlement: Any | None
        _telemetry: Any | None
        _user_order_evidence_parser: Any
        leader_sell_target_service: LeaderSellTargetRedriver | None
        _accounted_fill_reader: Any | None
        _recovery_inventory: Any | None
        funder_address: str | None
        _venue_poll_backoffs: dict[str, dict[str, Any]]
        _last_order_poll_at: dict[str, float]
        _venue_not_found_counts: dict[str, int]
        _auto_release_next_probe_at: dict[str, tuple[float, float]]
        _auto_release_abort_counts: dict[str, int]
        _ambiguous_stalled_last_audit: dict[str, float]

        _audit: Any
        _emit_lifecycle: Any
        _record_funder_activity: Any
        _release_reservation_if_present: Any
        _lookup_active_reservation_id_for_order: Any
        _should_skip_for_delay: Any
        _is_past_ambiguous_timeout: Any
        _is_past_auto_release_grace: Any
        _is_past_baseline_missing_horizon: Any
        _classify_probe_exception: Any
        _run_auto_release_probes: Any
        _clear_auto_release_state: Any
        _maybe_emit_ambiguous_stalled: Any
        _log_throttled_auto_release_abort: Any
        _maybe_auto_release_ambiguous: Any
        _handle_unresolved_confirmation_timeout: Any
        _record_negative_delta_anomaly: Any
        _previous_matched_for_order: Any
        _resolve_buy_requested_amount: Any
        _avg_price_from_fill: Any
        _startup_sell_inventory_recovery: Any
        _hydrate_order_record: Any
        _submission_sequences: Any
        _venue_state_dict: Any
        _fill_value: Any
        _aggregate_fills: Any
        _source_backed_off: Any
        _record_poll_success: Any
        _record_poll_failure: Any
        _poll_source: Any
        _attribute_market_trade_fills: Any
        confirm_fill: Any



