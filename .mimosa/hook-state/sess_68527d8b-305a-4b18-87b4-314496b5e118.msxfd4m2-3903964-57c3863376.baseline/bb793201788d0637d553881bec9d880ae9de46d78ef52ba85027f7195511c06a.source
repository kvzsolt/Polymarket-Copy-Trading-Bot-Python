"""Production clock adapters."""

from __future__ import annotations

from datetime import UTC, datetime


class SystemClock:
    """Provide the current timezone-aware UTC instant."""

    def now(self) -> datetime:
        """Return current UTC time."""

        return datetime.now(UTC)


__all__ = ["SystemClock"]
