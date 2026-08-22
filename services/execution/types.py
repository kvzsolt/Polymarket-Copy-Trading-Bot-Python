"""Shared types and constants for the execution subsystem."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Self

from domain.execution import ExecutionOrderType


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
    "min_order_size",
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
    # The venue's own (already scrubbed and bounded) rejection text, kept
    # beside ``error`` so it survives the ``insufficient_balance |`` prefixing.
    "venue_reason",
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
    """Describe one validated execution intent at the venue boundary."""

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
    order_type: ExecutionOrderType | None = None
    price_limit: Decimal | None = None
    tick_size: Decimal | None = None
    min_order_size: Decimal | None = None
    neg_risk: bool | None = None


@dataclass(frozen=True, slots=True)
class ExecutionAmountProjection:
    """Represent request amounts in operator-facing token and USDC units."""

    token_size: Decimal | None
    usdc_value: Decimal | None
    token_size_estimated: bool
    request_amount: Decimal
    request_amount_unit: str

    @classmethod
    def from_request(
        cls,
        *,
        side: str,
        amount: Decimal,
        price: Decimal | None,
    ) -> Self:
        """Project a venue-neutral request into display quantities.

        Args:
            side: Normalized order side, either ``BUY`` or ``SELL``.
            amount: BUY collateral cost or SELL outcome-token quantity.
            price: Effective limit price used to estimate the opposite unit.

        Returns:
            Exact decimal quantities with an explicit estimation marker.

        Raises:
            ValueError: If ``side`` is not BUY or SELL, or amount is negative.
        """
        normalized_side = side.strip().upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported execution side: {side!r}")
        if amount < 0:
            raise ValueError("execution amount must be non-negative")
        usable_price = price if price is not None and price > 0 else None
        if normalized_side == "BUY":
            return cls(
                token_size=amount / usable_price if usable_price is not None else None,
                usdc_value=amount,
                token_size_estimated=True,
                request_amount=amount,
                request_amount_unit="USDC",
            )
        return cls(
            token_size=amount,
            usdc_value=amount * usable_price if usable_price is not None else None,
            token_size_estimated=False,
            request_amount=amount,
            request_amount_unit="tokens",
        )


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
