"""Production clock adapters."""

from __future__ import annotations

from datetime import UTC, datetime
from time import time_ns, monotonic, monotonic_ns
from typing import Protocol
from uuid import uuid4
from os import getpid
from pmcopy.domain.source_time import TransportReceipt

_PROCESS_RECEIPT_BOOT_ID = uuid4().hex


class SystemClock:
    """Provide the current timezone-aware UTC instant."""

    def now(self) -> datetime:
        """Return current UTC time."""

        return datetime.now(UTC)


class MonotonicClock(Protocol):
    """Monotonic clock seam for deterministic throttling and uptime."""

    def monotonic(self) -> float:
        """Return monotonic seconds."""

        ...


class SystemMonotonicClock:
    """Production monotonic clock."""

    def monotonic(self) -> float:
        """Return process-local monotonic seconds."""

        return monotonic()


class SystemReceiptClock:
    """Sample system clocks under an injectable receipt session identity."""

    def __init__(self, *, boot_id: str | None = None) -> None:
        """Own one session token for the lifetime of this transport clock."""
        self._boot_id = f"{_PROCESS_RECEIPT_BOOT_ID}_{getpid()}" if boot_id is None else boot_id

    def capture(self) -> TransportReceipt:
        """Retain the actual wall sample and its compatible millisecond projection.

        Nanosecond units describe the sampled clock value, not an assertion of
        nanosecond physical accuracy or kernel packet-arrival precision.
        """
        wall_ns = time_ns()
        return TransportReceipt(wall_ns // 1_000_000, monotonic_ns(), self._boot_id, wall_ns)


__all__ = ["MonotonicClock", "SystemClock", "SystemMonotonicClock", "SystemReceiptClock"]
