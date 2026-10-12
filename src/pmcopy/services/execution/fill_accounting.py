"""Exact cumulative order evidence reconciled against accounted fills."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from pmcopy.domain.performance import PerformanceUnits


@dataclass(frozen=True, slots=True)
class AccountedOrderFill:
    """Shares and gross collateral already present in the confirmed fill ledger."""

    shares: Decimal = Decimal("0")
    collateral: Decimal = Decimal("0")
    fees: Decimal | None = Decimal("0")

    def incremental_fee(self, cumulative_fee: object) -> Decimal | None:
        """Return newly evidenced collateral fees, preserving any missing history."""
        total = PerformanceUnits.finite(cumulative_fee)
        if total is None or self.fees is None:
            return None
        if total < self.fees or self.fees < 0:
            raise ValueError("Cumulative fee evidence regressed")
        return total - self.fees

    def incremental_price(self, shares: Decimal, average_price: Decimal) -> Decimal:
        """Return the new fill price from cumulative shares and average price.

        Raises:
            ValueError: Evidence is non-finite, regresses, or has an invalid price.
        """
        values = (self.shares, self.collateral, shares, average_price)
        if any(not value.is_finite() for value in values):
            raise ValueError("Order fill evidence must be finite")
        if shares <= self.shares or not Decimal(0) < average_price < Decimal(1):
            raise ValueError("Order fill evidence must advance with a valid average price")
        price = (shares * average_price - self.collateral) / (shares - self.shares)
        if not Decimal(0) < price < Decimal(1):
            raise ValueError("Cumulative order notional contradicts accounted fills")
        return price


class AccountedFillReader(Protocol):
    """Read durable financial evidence independently of submission acknowledgements."""

    async def accounted_order_fill(self, user_id: str, order_id: str) -> AccountedOrderFill:
        """Return the follower's already accounted shares and gross collateral."""
        ...


class FillAccountingConflict(RuntimeError):
    """Another settlement advanced the ledger; refresh evidence before applying."""
