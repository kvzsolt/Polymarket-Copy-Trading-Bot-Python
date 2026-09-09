"""Production clock adapters."""

from __future__ import annotations

from datetime import UTC, datetime
from time import time_ns, monotonic_ns
from uuid import uuid4
from os import getpid
from domain.source_time import TransportReceipt

_PROCESS_RECEIPT_BOOT_ID = uuid4().hex


class SystemClock:
    """Provide the current timezone-aware UTC instant."""

    def now(self) -> datetime:
        """Return current UTC time."""

        return datetime.now(UTC)


class SystemReceiptClock:
    """Sample system clocks under an injectable receipt session identity."""

    def __init__(self, *, boot_id: str | None = None) -> None:
        """Own one session token for the lifetime of this transport clock."""
        self._boot_id = f"{_PROCESS_RECEIPT_BOOT_ID}_{getpid()}" if boot_id is None else boot_id

    def capture(self) -> TransportReceipt:
        """Return wall milliseconds and monotonic nanoseconds at application ingress."""
        return TransportReceipt(time_ns() // 1_000_000, monotonic_ns(), self._boot_id)


__all__ = ["SystemClock", "SystemReceiptClock"]
