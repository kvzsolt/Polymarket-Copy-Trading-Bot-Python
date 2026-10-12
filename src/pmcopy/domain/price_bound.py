"""Exact price limits for orders with a hard collateral-price bound."""

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR


class HardPriceBound:
    """Snap a caller's limit without increasing permitted adverse execution."""

    @staticmethod
    def on_tick(bound: Decimal, tick_size: Decimal, side: str) -> Decimal:
        """Return a BUY ceiling or SELL floor on the venue's exact tick grid.

        Raises:
            ValueError: The inputs are invalid or no price satisfies the bound.
        """
        for value in (bound, tick_size):
            if not isinstance(value, Decimal) or not value.is_finite() or not 0 < value < 1:
                raise ValueError("Price bound and tick must be finite Decimals in (0, 1)")
        if side not in {"BUY", "SELL"}:
            raise ValueError("Price bound requires BUY or SELL")
        rounding = ROUND_FLOOR if side == "BUY" else ROUND_CEILING
        rounded = (bound / tick_size).to_integral_value(rounding=rounding) * tick_size
        if not 0 < rounded < 1:
            raise ValueError("No executable tick satisfies the price bound")
        return rounded
