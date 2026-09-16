"""Separate held outcome tokens from active exposure using explicit settlement evidence."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum


class PositionLifecycle(StrEnum):
    """Classify inventory without inferring disposal from its market value."""

    UNKNOWN = "unknown"
    ACTIVE = "active"
    SETTLED = "settled"


@dataclass(frozen=True, slots=True)
class PositionClassification:
    """Retain settlement evidence independently of shares and historical cost."""

    lifecycle: PositionLifecycle = PositionLifecycle.UNKNOWN
    settlement_price: Decimal | None = None

    def __post_init__(self) -> None:
        """Reject invalid or misplaced payout evidence."""
        if not isinstance(self.lifecycle, PositionLifecycle):
            raise ValueError("Position lifecycle is invalid")
        price = self.settlement_price
        if price is not None and (
            self.lifecycle is not PositionLifecycle.SETTLED
            or not isinstance(price, Decimal)
            or not price.is_finite()
            or not Decimal(0) <= price <= Decimal(1)
        ):
            raise ValueError("Position settlement price is invalid")


class PositionLifecyclePolicy:
    """Classify normalized public evidence without I/O or assumptions about zero marks."""

    def classify(
        self, *, redeemable: object, settlement_price: object = None
    ) -> PositionClassification:
        """Return the conservative lifecycle supported by explicit redeemability.

        Args:
            redeemable: A provider boolean, or None for missing evidence.
            settlement_price: Exact collateral per settled share, when known.

        Returns:
            Immutable classification; missing evidence never excludes active risk.

        Raises:
            ValueError: Redeemability or a supplied settlement price is malformed.
        """
        if redeemable is not None and type(redeemable) is not bool:
            raise ValueError("Position redeemable flag must be a boolean")
        if redeemable is None:
            return PositionClassification()
        if redeemable is False:
            return PositionClassification(PositionLifecycle.ACTIVE)
        price = None
        if settlement_price is not None:
            if isinstance(settlement_price, bool) or not isinstance(
                settlement_price, (str, int, Decimal)
            ):
                raise ValueError("Position settlement price must be exact")
            try:
                price = Decimal(settlement_price)
            except (InvalidOperation, ValueError):
                raise ValueError("Position settlement price is invalid") from None
        return PositionClassification(PositionLifecycle.SETTLED, price)
