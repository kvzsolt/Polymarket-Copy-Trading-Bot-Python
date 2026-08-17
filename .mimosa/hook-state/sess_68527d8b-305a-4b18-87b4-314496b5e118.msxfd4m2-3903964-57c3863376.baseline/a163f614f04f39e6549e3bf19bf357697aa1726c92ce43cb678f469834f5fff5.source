from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class IntentSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class IntentReason(StrEnum):
    COPY_BUY = "copy_buy"
    COPY_SELL = "copy_sell"
    RISK_EXIT = "risk_exit"
    NEG_RISK_ARB = "neg_risk_arb"


class OrderLifecycleStatus(StrEnum):
    CREATED = "created"
    RESERVED = "reserved"
    SUBMITTING = "submitting"
    SUBMITTED_UNCONFIRMED = "submitted_unconfirmed"
    SUBMITTED_DELAYED = "submitted_delayed"
    ORDER_LIVE = "order_live"
    MATCHED_PENDING_CONFIRMATION = "matched_pending_confirmation"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    TRADE_MINED = "trade_mined"
    TRADE_CONFIRMED = "trade_confirmed"
    TRADE_RETRYING = "trade_retrying"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REJECTED = "rejected"
    RETRYABLE_FAILED = "retryable_failed"
    TERMINAL_FAILED = "terminal_failed"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    UNKNOWN_UNCONFIRMED = "unknown_unconfirmed"
    AMBIGUOUS_TIMEOUT = "ambiguous_timeout"
    AUTO_RELEASED_AMBIGUOUS = "auto_released_ambiguous"


TERMINAL_FILLED_STATUSES = {"filled", "matched"}
TERMINAL_REJECTED_STATUSES = {
    "rejected",
    "cancelled",
    "expired",
    "failed",
    "terminal_failed",
}
NON_TERMINAL_STATUSES = {
    "live",
    "open",
    "pending",
    "delayed",
    "submitted",
    "submitted_unconfirmed",
}


def classify_venue_status(status: str | None) -> OrderLifecycleStatus:
    normalized = (status or "").strip().lower()

    if normalized in TERMINAL_FILLED_STATUSES:
        return OrderLifecycleStatus.FILLED

    if normalized in TERMINAL_REJECTED_STATUSES:
        return OrderLifecycleStatus.TERMINAL_FAILED

    if normalized in NON_TERMINAL_STATUSES:
        return OrderLifecycleStatus.SUBMITTED_UNCONFIRMED

    return OrderLifecycleStatus.UNKNOWN_UNCONFIRMED


@dataclass(frozen=True)
class OrderIntentKey:
    user_id: str
    leader_wallet: str | None
    condition_id: str
    token_id: str
    side: IntentSide
    reason: IntentReason
    # Per-trade source identity (e.g. leader tx hash). Two *different* leader
    # trades on the same (condition, token, side, reason) must reserve
    # independently; without this they would collide on the reservation key
    # and the second copy would be dropped as ``reservation_conflict``.
    # Retries of the *same* trade carry the same id, so they still collide and
    # remain deduped (alongside the tx-keyed processed-trade claim and the
    # coordinator idempotency layer).
    source_event_id: str | None = None

    def stable_key(self) -> str:
        # DEPLOYMENT NOTE: this format gained a 7th (``source_event_id``)
        # component. Reservations created by an older build store the 6-field
        # key in ``order_reservations.order_intent_key`` and will not match the
        # new lookup. The exposure is bounded and fail-safe: reservations are
        # short-lived (``RESERVATION_TTL_SECONDS``, default 120s) and reclaimed
        # by reconciliation / the risk-loop reaper, and the tx-keyed
        # processed-trade claim + coordinator idempotency still prevent
        # duplicate *orders*. For a zero-transient rollout, drain active
        # reservations (deploy during a quiescent window) before cutover.
        return "|".join(
            [
                self.user_id.lower(),
                (self.leader_wallet or "").lower(),
                self.condition_id,
                self.token_id,
                self.side.value,
                self.reason.value,
                self.source_event_id or "",
            ]
        )

    def legacy_stable_key(self) -> str:
        """Pre-``source_event_id`` key format (6 components).

        Used only as a transitional dual-read in
        ``ReservationService.reserve_for_intent`` so an active reservation
        created by a build that predates the ``source_event_id`` component is
        still found during rollout (see the ``stable_key`` DEPLOYMENT NOTE).
        New reservations are always stored under the 7-component
        ``stable_key``, so this only ever matches pre-deployment rows and
        becomes a no-op once they drain.
        """
        return "|".join(
            [
                self.user_id.lower(),
                (self.leader_wallet or "").lower(),
                self.condition_id,
                self.token_id,
                self.side.value,
                self.reason.value,
            ]
        )


@dataclass(frozen=True)
class OrderIntent:
    key: OrderIntentKey
    size: Decimal
    price: Decimal
    max_slippage_bps: int
    created_at: datetime
    source_signal_id: str | None = None


@dataclass(frozen=True)
class Reservation:
    id: str
    user_id: str
    intent_key: str
    condition_id: str
    token_id: str
    side: IntentSide
    reason: IntentReason
    reserved_usdc: Decimal
    reserved_size: Decimal
    status: str
    created_at: datetime
    expires_at: datetime
    order_id: str | None = None
    venue_order_id: str | None = None
