"""Shared types and constants for the execution subsystem."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


_MAX_PERSISTED_FIELD_CHARS = 160
_MAX_PERSISTED_ERROR_CHARS = 512
_TRUNCATED_SUFFIX = "...[truncated]"

_PERSISTED_REQUEST_FIELDS = (
    "user_id",
    "leader_id",
    "token_id",
    "condition_id",
    "side",
    "amount",
    "max_slippage_bps",
    "reason",
    "signal_id",
    "recovery_metadata",
    "worst_price",
    "price_limit",
    "tick_size",
    "neg_risk",
    "order_type",
)

_PERSISTED_RESPONSE_FIELDS = (
    "success",
    "status",
    "orderID",
    "order_id",
    "size_matched",
    "filled_size",
    "matched_size",
    "matched_amount",
    "avg_price",
    "avg_fill_price",
    "error",
    "pending_since",
    "pending_ttl_seconds",
    "pending_expires_at",
    "expired_at",
)

_RETRY_BLOCKING_STATUSES = {
    "pending",
    "submitted",
    "submitted_unconfirmed",
    "filled",
    "duplicate_ignored",
    "live",
    "matched",
}

_SUPPORTED_BOOK_SPREAD_MODES = {"absolute_bps", "relative_bps", "ticks"}


@dataclass(frozen=True)
class ExecutionRequest:
    user_id: str
    leader_id: str
    token_id: str
    side: str  # BUY or SELL
    amount: Decimal
    max_slippage_bps: int
    reason: str  # copy_trade, advanced_risk_exit, rebalance
    signal_id: str | None = None
    condition_id: str | None = None
    recovery_metadata: dict[str, Any] | None = None
    order_type: str | None = None
    price_limit: Decimal | None = None


@dataclass
class ExecutionResult:
    accepted: bool
    order_id: str | None
    status: str
    requested_amount: Decimal
    filled_amount: Decimal
    avg_price: Decimal | None
    fees_paid: Decimal
    error: str | None = None


@dataclass(frozen=True)
class BookSpreadMetrics:
    absolute: Decimal
    absolute_bps: int
    relative_bps: int | None
    ticks: int | None
