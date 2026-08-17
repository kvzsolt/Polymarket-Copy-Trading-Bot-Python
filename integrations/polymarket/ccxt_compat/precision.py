"""Exact Polymarket order amount construction for CCXT prediction clients.

CCXT's first Polymarket prediction implementation used a hard-coded table for a
small set of tick sizes.  Polymarket can expose additional exact increments
(e.g. ``0.005`` and ``0.0025``).  Falling back to ``0.01`` can round a valid
price to zero or sign amounts for a different implied price.

This module installs a narrow instance-level compatibility patch.  It does not
fork CCXT and is safe to remove once upstream accepts arbitrary tick sizes.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_DOWN, ROUND_UP
from types import MethodType
from typing import Any

from integrations.polymarket.ccxt_compat.capabilities import PRECISION_PATCH_ID
from integrations.polymarket.ccxt_compat.tick_size import quantize_price_to_tick

_PATCH_MARKER = PRECISION_PATCH_ID
_SCALE = Decimal("1000000")
_SIZE_QUANTUM = Decimal("0.01")


class CcxtPrecisionError(ValueError):
    """Raised when a CCXT order cannot be represented safely."""


def _decimal(value: Any, *, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise CcxtPrecisionError(f"{field} must be a finite decimal") from exc
    if not result.is_finite():
        raise CcxtPrecisionError(f"{field} must be finite")
    return result


def _decimal_places(step: Decimal) -> int:
    normalized = step.normalize()
    exponent = normalized.as_tuple().exponent
    if not isinstance(exponent, int):
        raise CcxtPrecisionError("step must be a finite decimal")
    return max(0, -exponent)


def _truncate(value: Decimal, quantum: Decimal) -> Decimal:
    return value.quantize(quantum, rounding=ROUND_DOWN)


def polymarket_raw_amounts(
    side: str,
    size: Any,
    price: Any,
    tick_size: Any,
    cost: Any | None = None,
) -> dict[str, str]:
    """Build integer maker/taker amounts for a Polymarket CLOB order.

    The result mirrors ``ccxt.prediction.polymarket.polymarket_order_raw_amounts``
    while deriving precision from the actual tick and requiring the price to be
    an exact tick multiple.  USDC and outcome shares are represented with six
    on-chain decimals.
    """

    normalized_side = str(side).upper()
    if normalized_side not in {"BUY", "SELL"}:
        raise CcxtPrecisionError(f"unsupported side: {side!r}")

    tick = _decimal(tick_size, field="tick_size")
    if tick <= 0:
        raise CcxtPrecisionError("tick_size must be positive")
    try:
        px = quantize_price_to_tick(
            price=price,
            tick_size=tick,
            side=normalized_side,  # type: ignore[arg-type]
        )
    except ValueError as exc:
        raise CcxtPrecisionError(str(exc)) from exc
    requested_size = _decimal(size, field="size")
    if requested_size <= 0:
        raise CcxtPrecisionError("size must be positive")

    price_decimals = _decimal_places(tick)
    amount_decimals = min(6, price_decimals + 2)
    amount_quantum = Decimal(1).scaleb(-amount_decimals)

    maker: Decimal
    taker: Decimal
    if cost is not None:
        if normalized_side != "BUY":
            raise CcxtPrecisionError("cost sizing is supported only for BUY orders")
        spend = _decimal(cost, field="cost")
        if spend <= 0:
            raise CcxtPrecisionError("cost must be positive")
        maker = _truncate(spend, _SIZE_QUANTUM)
        # Round received shares upward so maker/taker never exceeds max price.
        taker = (maker / px).quantize(amount_quantum, rounding=ROUND_CEILING)
    elif normalized_side == "BUY":
        taker = _truncate(requested_size, _SIZE_QUANTUM)
        # Round spend downward so the implied BUY price stays at/below the cap.
        maker = (taker * px).quantize(amount_quantum, rounding=ROUND_DOWN)
    else:
        maker = _truncate(requested_size, _SIZE_QUANTUM)
        # Round proceeds upward so the implied SELL price stays at/above the floor.
        taker = (maker * px).quantize(amount_quantum, rounding=ROUND_UP)

    maker_amount = (maker * _SCALE).to_integral_value(rounding=ROUND_DOWN)
    taker_amount = (taker * _SCALE).to_integral_value(rounding=ROUND_DOWN)
    if maker_amount <= 0 or taker_amount <= 0:
        raise CcxtPrecisionError("maker and taker amounts must be greater than zero")

    return {
        "makerAmount": format(maker_amount, "f"),
        "takerAmount": format(taker_amount, "f"),
    }


def install_ccxt_polymarket_precision_patch(exchange: Any) -> Any:
    """Install the arbitrary-tick amount builder on one CCXT exchange instance."""

    if getattr(exchange, "_pmcopy_precision_patch", None) == _PATCH_MARKER:
        return exchange

    original = getattr(exchange, "polymarket_order_raw_amounts", None)
    if original is not None and not hasattr(exchange, "_pmcopy_original_raw_amounts"):
        exchange._pmcopy_original_raw_amounts = original

    def _patched(
        self: Any,
        side: str,
        size: Any,
        price: Any,
        tickSize: Any,
        cost: Any | None = None,
    ) -> dict[str, str]:
        del self
        return polymarket_raw_amounts(side, size, price, tickSize, cost)

    exchange.polymarket_order_raw_amounts = MethodType(_patched, exchange)
    exchange._pmcopy_precision_patch = _PATCH_MARKER
    return exchange


__all__ = [
    "CcxtPrecisionError",
    "install_ccxt_polymarket_precision_patch",
    "polymarket_raw_amounts",
]
