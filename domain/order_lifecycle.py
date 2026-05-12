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
    NEAR_ONE_EXIT = "near_one_exit"
    NEG_RISK_ARB = "neg_risk_arb"


class OrderLifecycleStatus(StrEnum):
    CREATED = "created"
    RESERVED = "reserved"
    SUBMITTING = "submitting"
    SUBMITTED_UNCONFIRMED = "submitted_unconfirmed"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    EXPIRED = "expired"
    REJECTED = "rejected"
    RETRYABLE_FAILED = "retryable_failed"
    TERMINAL_FAILED = "terminal_failed"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    UNKNOWN_UNCONFIRMED = "unknown_unconfirmed"


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

    def stable_key(self) -> str:
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

