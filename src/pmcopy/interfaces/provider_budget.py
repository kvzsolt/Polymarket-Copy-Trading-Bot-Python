"""Narrow time and admission ports shared by provider transports."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol


class ProviderClock(Protocol):
    """Supply UTC billing time and monotonic durations without hidden clocks."""

    def now(self) -> datetime:
        """Return a timezone-aware UTC instant."""
        ...

    def monotonic_ns(self) -> int:
        """Return monotonic nanoseconds for bounded request durations."""
        ...


class ProviderAdmission(Protocol):
    """Account for a priced read before transport dispatch."""

    def admit_rpc(self, provider: str, method: str, *, category: str = "essential") -> int:
        """Atomically record estimated usage and return the read's cost."""
        ...
