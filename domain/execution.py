"""Immutable execution policy values independent of venue adapters."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from enum import StrEnum


class ExecutionOrderType(StrEnum):
    """Supported execution policies for copy orders."""

    FAK = "FAK"


_BAND_QUANTUM = Decimal("0.0001")
_PERCENT = Decimal("100")


@dataclass(frozen=True)
class LeaderPriceBand:
    """Adjustable price band around the leader's own fill price.

    A copy entry is only worth submitting while the executable side of the
    book is still close to the price the leader paid. The band turns that
    tolerance into an explicit BUY cap / SELL floor so a copy is either
    submitted marketable or refused before it reaches the venue — never sent
    as a FAK that cannot cross the spread.

    Attributes:
        leader_price: The leader's own fill price for the copied trade.
        drift_pct: Half-width of the band in percent. ``0`` disables the band.
    """

    leader_price: Decimal
    drift_pct: Decimal

    @property
    def enabled(self) -> bool:
        """Return True when the band constrains anything.

        A non-positive drift or leader price carries no usable reference, so
        the band is inert rather than silently clamping to zero.
        """
        return self.drift_pct > 0 and self.leader_price > 0

    def limit_for(self, side: str) -> Decimal:
        """Return the FAK price limit this band imposes on ``side``.

        Args:
            side: ``"BUY"`` (cap) or ``"SELL"`` (floor); case-insensitive.

        Returns:
            The leader price widened by ``drift_pct`` and quantized to four
            decimal places — away from the leader for a BUY cap, toward zero
            for a SELL floor, so quantization never tightens the band. A
            disabled band returns the leader price unchanged.
        """
        if not self.enabled:
            return self.leader_price
        factor = self.drift_pct / _PERCENT
        if side.upper() == "BUY":
            return (self.leader_price * (Decimal("1") + factor)).quantize(
                _BAND_QUANTUM, rounding=ROUND_UP
            )
        return (self.leader_price * (Decimal("1") - factor)).quantize(
            _BAND_QUANTUM, rounding=ROUND_DOWN
        )

    def excludes(self, quote_price: Decimal, side: str) -> bool:
        """Return True when ``quote_price`` sits outside the band.

        Args:
            quote_price: Best ask for a BUY, best bid for a SELL.
            side: ``"BUY"`` or ``"SELL"``; case-insensitive.

        Returns:
            True when submitting would require crossing beyond the band. A
            disabled band never excludes.
        """
        if not self.enabled:
            return False
        limit = self.limit_for(side)
        if side.upper() == "BUY":
            return quote_price > limit
        return quote_price < limit


__all__ = ["ExecutionOrderType", "LeaderPriceBand"]
