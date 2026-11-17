"""Exact Decimal tick-grid quantization and price-bound policies.

Polymarket outcome prices live in ``(0, 1)`` and venue metadata may prescribe
arbitrary exact tick increments (``0.1``, ``0.01``, ``0.005``, ``0.0025``,
``0.001``, ``0.0001``). All arithmetic here uses exact ``Decimal`` tick-unit
division — never decimal-place rounding or ``0.01`` fallbacks.

Two distinct domain policies intentionally round in opposite directions:

* :class:`HardBoundTickPolicy` / :class:`HardPriceBound`: snaps a hard caller
  BUY ceiling downward (``ROUND_FLOOR``) and a hard caller SELL floor upward
  (``ROUND_CEILING``), failing closed when inputs are invalid or no executable
  tick satisfies the bound.
* :class:`MarketableTickPolicy`: snaps marketable FAK/FOK quotes toward
  crossing (:attr:`TickRounding.MARKETABLE`), then clamps and conservatively
  re-snaps against an optional caller protection bound while preserving legacy
  ``tick_size <= 0`` pass-through for synthetic test fixtures.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from enum import Enum
from typing import ClassVar, Literal, final, override

OrderSide = Literal["BUY", "SELL"]


class TickRounding(Enum):
    """Direction a price moves when snapped onto the venue tick grid.

    Attributes:
        CONSERVATIVE: Round away from crossing — BUY rounds down, SELL rounds
            up — so the order never breaches the caller's cap or floor.
        MARKETABLE: Round toward crossing — BUY rounds up, SELL rounds down —
            so quantization never strips marketability before caller-bound
            clamping is applied.
    """

    CONSERVATIVE = "conservative"
    MARKETABLE = "marketable"


class InvalidTickSize(ValueError):
    """Raised when a tick size is unusable or produces an out-of-range price."""


def _coerce_decimal(value: object, *, field: str, error: type[Exception]) -> Decimal:
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise error(f"{field} must be a finite decimal") from exc
    return result


def _snap_integral_tick_units(price: Decimal, tick_size: Decimal, *, round_up: bool) -> Decimal:
    """Return ``price`` snapped to an exact integer multiple of ``tick_size``."""
    rounding = ROUND_CEILING if round_up else ROUND_FLOOR
    units = (price / tick_size).to_integral_value(rounding=rounding)
    return units * tick_size


@dataclass(frozen=True, slots=True)
class TickSize:
    """Validated finite positive venue tick size."""

    value: Decimal

    def __post_init__(self) -> None:
        if not self.value.is_finite():
            raise ValueError("Tick size must be finite")
        if self.value <= Decimal("0"):
            raise ValueError("Tick size must be greater than zero")


def quantize_price_to_tick(
    *,
    price: Decimal,
    tick_size: Decimal,
    side: OrderSide | str,
    rounding: TickRounding = TickRounding.CONSERVATIVE,
) -> Decimal:
    """Return an exact integer multiple of ``tick_size`` within ``(0, 1)``.

    Args:
        price: Unquantized collateral price per outcome share.
        tick_size: Authoritative positive venue tick increment.
        side: ``"BUY"`` or ``"SELL"`` (case-insensitive).
        rounding: Tick-snap direction (`CONSERVATIVE` or `MARKETABLE`).

    Returns:
        Quantized ``Decimal`` price strictly inside ``(0, 1)``.

    Raises:
        InvalidTickSize: If ``tick_size`` is non-finite/non-positive or the
            quantized result falls outside ``(0, 1)``.
        ValueError: If ``price`` is non-finite or ``side`` is unsupported.
    """
    tick = _coerce_decimal(tick_size, field="tick_size", error=InvalidTickSize)
    if not tick.is_finite() or tick <= 0:
        raise InvalidTickSize("tick_size must be finite and greater than zero")
    raw_price = _coerce_decimal(price, field="price", error=ValueError)
    if not raw_price.is_finite():
        raise ValueError("price must be finite")
    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise ValueError(f"unsupported order side: {side!r}")

    rounds_up = (normalized_side == "BUY") == (rounding is TickRounding.MARKETABLE)
    quantized = _snap_integral_tick_units(raw_price, tick, round_up=rounds_up)
    if quantized <= 0 or quantized >= 1:
        raise InvalidTickSize("tick-quantized price is outside the tradable range")
    return quantized


class TickPricePolicy(ABC):
    """Abstract base class for side-aware tick-grid price policies."""

    policy_name: ClassVar[str]

    @abstractmethod
    def quantize(self, *, price: Decimal, tick_size: Decimal, side: str) -> Decimal:
        """Return ``price`` quantized onto ``tick_size`` under this policy."""
        raise NotImplementedError


@final
class HardBoundTickPolicy(TickPricePolicy):
    """Strict conservative policy for hard caller BUY ceilings and SELL floors."""

    policy_name: ClassVar[str] = "hard_bound"

    @override
    def quantize(self, *, price: Decimal, tick_size: Decimal, side: str) -> Decimal:
        """Snap ``price`` conservatively in ``(0, 1)`` without weakening the bound.

        Raises:
            ValueError: Inputs are non-Decimal, non-finite, outside ``(0, 1)``,
                use an invalid side, or leave no executable tick in ``(0, 1)``.
        """
        for value in (price, tick_size):
            if not isinstance(value, Decimal) or not value.is_finite() or not 0 < value < 1:
                raise ValueError("Price bound and tick must be finite Decimals in (0, 1)")
        if side not in {"BUY", "SELL"}:
            raise ValueError("Price bound requires BUY or SELL")
        rounded = _snap_integral_tick_units(price, tick_size, round_up=(side == "SELL"))
        if not 0 < rounded < 1:
            raise ValueError("No executable tick satisfies the price bound")
        return rounded

    def snap_copy_fak_limit(
        self,
        *,
        price_limit: Decimal,
        tick_size: Decimal,
        side: str,
        max_share_price: Decimal = Decimal("0"),
    ) -> Decimal:
        """Clamp BUY ceiling to ``max_share_price`` (when > 0) and snap conservatively."""
        bound = price_limit
        if side == "BUY" and max_share_price > 0:
            bound = min(bound, max_share_price)
        return self.quantize(price=bound, tick_size=tick_size, side=side)


@final
class MarketableTickPolicy(TickPricePolicy):
    """Marketable FAK/FOK slippage quantization + conservative caller-cap clamp."""

    policy_name: ClassVar[str] = "marketable"

    @override
    def quantize(self, *, price: Decimal, tick_size: Decimal, side: str) -> Decimal:
        """Quantize ``price`` toward crossing when ``tick_size > 0``."""
        if tick_size <= 0:
            return price
        normalized_side: OrderSide = "BUY" if side == "BUY" else "SELL"
        return quantize_price_to_tick(
            price=price,
            tick_size=tick_size,
            side=normalized_side,
            rounding=TickRounding.MARKETABLE,
        )

    def apply_caller_protection(
        self,
        *,
        calculated_price: Decimal,
        caller_limit: Decimal,
        tick_size: Decimal,
        side: str,
    ) -> Decimal:
        """Clamp ``calculated_price`` to ``caller_limit`` and snap conservatively."""
        constrained = (
            min(calculated_price, caller_limit)
            if side == "BUY"
            else max(calculated_price, caller_limit)
        )
        if tick_size <= 0:
            return constrained
        return _snap_integral_tick_units(constrained, tick_size, round_up=(side != "BUY"))


HARD_BOUND_TICK_POLICY = HardBoundTickPolicy()
MARKETABLE_TICK_POLICY = MarketableTickPolicy()


class HardPriceBound:
    """Snap a caller's limit without increasing permitted adverse execution."""

    @staticmethod
    def on_tick(bound: Decimal, tick_size: Decimal, side: str) -> Decimal:
        """Return a BUY ceiling or SELL floor on the venue's exact tick grid.

        Raises:
            ValueError: The inputs are invalid or no price satisfies the bound.
        """
        return HARD_BOUND_TICK_POLICY.quantize(price=bound, tick_size=tick_size, side=side)


__all__ = [
    "HARD_BOUND_TICK_POLICY",
    "HardBoundTickPolicy",
    "HardPriceBound",
    "InvalidTickSize",
    "MARKETABLE_TICK_POLICY",
    "MarketableTickPolicy",
    "OrderSide",
    "TickPricePolicy",
    "TickRounding",
    "TickSize",
    "quantize_price_to_tick",
]
