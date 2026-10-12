"""Explicit performance units shared by analytics boundaries and scoring."""

from decimal import Decimal, InvalidOperation


class PerformanceUnits:
    """Validate observations without conflating missing data and numeric zero."""

    @staticmethod
    def finite(value: object) -> Decimal | None:
        """Parse a finite numeric observation; only ``None`` means missing.

        Raises:
            ValueError: For booleans, invalid types, or non-finite values.
                Messages never include the untrusted value.
        """
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
            raise ValueError("Performance observation must be numeric")
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            raise ValueError("Performance observation must be numeric") from None
        if not number.is_finite():
            raise ValueError("Performance observation must be finite")
        return number

    @classmethod
    def fraction(cls, value: object) -> Decimal | None:
        """Parse a fraction in [0, 1] without magnitude-based conversion."""
        number = cls.finite(value)
        if number is not None and not Decimal(0) <= number <= Decimal(1):
            raise ValueError("Performance fraction must be between zero and one")
        return number

    @classmethod
    def percentage_points_to_fraction(cls, value: object) -> Decimal | None:
        """Convert percentage points to a fraction; negative ROI is valid."""
        number = cls.finite(value)
        return None if number is None else number / Decimal(100)

    @classmethod
    def win_rate_percent_to_fraction(cls, value: object) -> Decimal | None:
        """Convert win-rate percentage points in [0, 100] to a fraction."""
        return cls.fraction(cls.percentage_points_to_fraction(value))
