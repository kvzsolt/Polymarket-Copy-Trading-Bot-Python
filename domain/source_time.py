"""Immutable transport receipt evidence with explicit session-local clock units."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation


class SourceTimestampParser:
    """Parse source time while preserving the legacy untagged millisecond contract."""

    @staticmethod
    def parse(value: object, *, unit: object = None) -> datetime | None:
        """Return UTC source time, or missing evidence for an absent field.

        Explicit units take precedence. Untagged older detector payloads retain
        their existing Unix-millisecond threshold for compatibility.

        Raises:
            ValueError: If the units or timestamp cannot represent UTC time.
        """
        if unit not in (None, "seconds", "milliseconds"):
            raise ValueError("Timestamp unit must be seconds or milliseconds")
        if value is None or value == "":
            return None
        if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
            raise ValueError("Timestamp must be numeric")
        try:
            seconds = Decimal(str(value))
        except InvalidOperation:
            raise ValueError("Timestamp must be numeric") from None
        if not seconds.is_finite() or seconds < 0:
            raise ValueError("Timestamp must be finite and non-negative")
        if unit == "milliseconds" or (unit is None and seconds > Decimal("100000000000")):
            seconds /= Decimal(1000)
        if seconds >= Decimal("253402300800"):
            raise ValueError("Timestamp is outside the supported UTC range")
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            microseconds=int(seconds * Decimal(1000000))
        )


@dataclass(frozen=True, slots=True)
class TransportReceipt:
    """Time sampled when the application reads a frame, before decoding it.

    This is not kernel arrival time, source event time, or durable replay ordering.
    """

    wall_timestamp_ms: int
    monotonic_ns: int
    boot_id: str

    def __post_init__(self) -> None:
        """Require exact non-negative clock values and a bounded session identity."""
        for value in (self.wall_timestamp_ms, self.monotonic_ns):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("Receipt clocks must be non-negative integers")
        if self.wall_timestamp_ms >= 253402300800000:
            raise ValueError("Receipt wall time is outside the supported UTC range")
        if (
            not isinstance(self.boot_id, str)
            or not 1 <= len(self.boot_id) <= 128
            or any(
                not (char.isascii() and (char.isalnum() or char in "_-")) for char in self.boot_id
            )
        ):
            raise ValueError("Receipt boot identity must be a bounded safe token")

    @property
    def received_at(self) -> datetime:
        """Return UTC receipt wall time without floating-point timestamp conversion."""
        return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            milliseconds=self.wall_timestamp_ms
        )

    def elapsed_since(self, earlier: "TransportReceipt") -> int | None:
        """Return elapsed nanoseconds only within one non-regressing monotonic session."""
        if self.boot_id != earlier.boot_id or self.monotonic_ns < earlier.monotonic_ns:
            return None
        return self.monotonic_ns - earlier.monotonic_ns
