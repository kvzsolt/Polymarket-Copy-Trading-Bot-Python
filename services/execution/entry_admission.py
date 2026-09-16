"""Trader-price-based entry admission for marketable Polymarket orders.

The policy deliberately does not inspect spread or book depth. Entry risk is
bounded by a copied-trader-price-derived explicit limit and FAK semantics. The
venue adapter remains responsible for tick-aware quantization and metadata
validation immediately before signing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HUNDRED = Decimal("100")


class OffsetMode(StrEnum):
    """Supported trader-price offset calculation modes."""

    PERCENTAGE = "percentage"
    FIXED = "fixed"


class OffsetDirection(StrEnum):
    """Direction used when applying an explicit copied-price offset."""

    MARKETABLE = "marketable"
    ABOVE = "above"
    BELOW = "below"


@dataclass(frozen=True, slots=True)
class EntryPriceBandPolicy:
    """Controls the copied-trader entry-price band gate."""

    enabled: bool = True
    minimum: Decimal = Decimal("0.20")
    maximum: Decimal = Decimal("0.80")

    def __post_init__(self) -> None:
        """Validate the probability band."""

        minimum = _finite_decimal(self.minimum, name="entry band minimum")
        maximum = _finite_decimal(self.maximum, name="entry band maximum")
        if minimum < _ZERO or maximum > _ONE:
            raise ValueError("entry price band must stay within [0, 1]")
        if minimum > maximum:
            raise ValueError("entry price band minimum must not exceed maximum")
        object.__setattr__(self, "minimum", minimum)
        object.__setattr__(self, "maximum", maximum)


@dataclass(frozen=True, slots=True)
class TraderPriceOffsetPolicy:
    """Controls the explicit price-cap/floor offset from the leader price."""

    enabled: bool = True
    mode: OffsetMode = OffsetMode.PERCENTAGE
    direction: OffsetDirection = OffsetDirection.MARKETABLE
    value: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        """Validate offset semantics without silently clamping money values."""

        value = _finite_decimal(self.value, name="trader price offset")
        if value < _ZERO:
            raise ValueError("trader price offset must be non-negative")
        if self.mode is OffsetMode.PERCENTAGE and value > _HUNDRED:
            raise ValueError("percentage trader price offset must be <= 100")
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, slots=True)
class EntryPolicy:
    """Complete entry-admission policy for copied marketable orders."""

    enabled: bool = True
    price_band: EntryPriceBandPolicy = field(default_factory=EntryPriceBandPolicy)
    offset: TraderPriceOffsetPolicy = field(default_factory=TraderPriceOffsetPolicy)


@dataclass(frozen=True, slots=True)
class EntryAdmissionDecision:
    """Immutable decision emitted before risk and order construction."""

    allowed: bool
    reason: str
    order_type: str
    price_limit: Decimal | None
    audit: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))


class EntryAdmissionEngine:
    """Apply a price band and derive an explicit FAK price limit."""

    def __init__(self, policy: EntryPolicy) -> None:
        """Initialize the engine with an immutable policy."""

        self._policy = policy

    def evaluate(
        self,
        *,
        side: str,
        copied_trader_price: Decimal,
    ) -> EntryAdmissionDecision:
        """Evaluate an entry signal and derive its explicit venue price limit.

        Args:
            side: ``BUY`` for entry/add orders or ``SELL`` for a conservative
                marketable floor calculation.
            copied_trader_price: Price observed on the copied trader's fill.

        Returns:
            An immutable admission decision. Every allowed decision contains
            an explicit price limit and uses FAK.
        """

        if not self._policy.enabled:
            try:
                copied_price = _finite_decimal(
                    copied_trader_price,
                    name="copied trader price",
                )
            except ValueError:
                return self._reject("invalid_copied_trader_price")
            if copied_price <= _ZERO or copied_price >= _ONE:
                return self._reject("invalid_copied_trader_price")
            return self._allow(
                reason="entry_policy_disabled",
                price_limit=copied_price,
                copied_price=copied_price,
                side="DISABLED",
            )

        normalized_side = str(side).strip().upper()
        if normalized_side not in {"BUY", "SELL"}:
            return self._reject("invalid_side")
        try:
            copied_price = _finite_decimal(
                copied_trader_price,
                name="copied trader price",
            )
        except ValueError:
            return self._reject("invalid_copied_trader_price")
        if copied_price <= _ZERO or copied_price >= _ONE:
            return self._reject("invalid_copied_trader_price")

        band = self._policy.price_band
        if band.enabled:
            if copied_price < band.minimum:
                return self._reject("copied_price_below_entry_band")
            if copied_price > band.maximum:
                return self._reject("copied_price_above_entry_band")

        try:
            limit = self._derive_limit(
                side=normalized_side,
                copied_price=copied_price,
            )
        except ValueError:
            return self._reject("derived_price_limit_out_of_range")
        return self._allow(
            reason="entry_allowed",
            price_limit=limit,
            copied_price=copied_price,
            side=normalized_side,
        )

    def _derive_limit(self, *, side: str, copied_price: Decimal) -> Decimal:
        """Return the unquantized explicit cap or floor for the venue adapter."""

        offset = self._policy.offset
        if not offset.enabled or offset.value == _ZERO:
            result = copied_price
        else:
            delta = (
                copied_price * (offset.value / _HUNDRED)
                if offset.mode is OffsetMode.PERCENTAGE
                else offset.value
            )
            if offset.direction is OffsetDirection.ABOVE:
                result = copied_price + delta
            elif offset.direction is OffsetDirection.BELOW:
                result = copied_price - delta
            else:
                result = copied_price + delta if side == "BUY" else copied_price - delta
        if result <= _ZERO or result >= _ONE:
            raise ValueError("derived marketable limit must stay in (0, 1)")
        return result

    @staticmethod
    def _reject(reason: str) -> EntryAdmissionDecision:
        """Create a fail-closed decision without a marketable price."""

        return EntryAdmissionDecision(
            allowed=False,
            reason=reason,
            order_type="FAK",
            price_limit=None,
        )

    @staticmethod
    def _allow(
        *,
        reason: str,
        price_limit: Decimal,
        copied_price: Decimal,
        side: str,
    ) -> EntryAdmissionDecision:
        """Create an allowed decision with an explicit marketable bound."""

        return EntryAdmissionDecision(
            allowed=True,
            reason=reason,
            order_type="FAK",
            price_limit=price_limit,
            audit=MappingProxyType(
                {
                    "side": side,
                    "copied_trader_price": str(copied_price),
                    "price_limit": str(price_limit),
                }
            ),
        )


def _finite_decimal(value: object, *, name: str) -> Decimal:
    """Convert a boundary value to a finite ``Decimal``."""

    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be decimal-like") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


__all__ = [
    "EntryAdmissionDecision",
    "EntryAdmissionEngine",
    "EntryPolicy",
    "EntryPriceBandPolicy",
    "OffsetDirection",
    "OffsetMode",
    "TraderPriceOffsetPolicy",
]
