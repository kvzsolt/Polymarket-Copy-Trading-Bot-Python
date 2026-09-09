"""Known historical entry price remains distinct from missing cost evidence."""

from dataclasses import dataclass
from decimal import Decimal

from domain.performance import PerformanceUnits


@dataclass(frozen=True, slots=True)
class PositionCostBasis:
    """A known entry price, or explicit missing evidence for imported inventory."""

    entry_price: Decimal | None

    @classmethod
    def from_value(cls, value: object) -> "PositionCostBasis":
        """Parse historical position data; zero is the legacy unknown-basis sentinel."""
        try:
            price = PerformanceUnits.finite(value)
        except ValueError:
            price = None
        if price is not None and not Decimal(0) < price <= Decimal(1):
            price = None
        return cls(price)

    def gross_realized_pnl(self, price: Decimal, shares: Decimal) -> Decimal | None:
        """Return realized collateral before fees, or None for unknown entry cost."""
        if self.entry_price is None:
            return None
        return (price - self.entry_price) * shares
