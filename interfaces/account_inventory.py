"""Narrow public account inventory evidence consumed by execution services."""

from decimal import Decimal
from typing import Protocol


class PositionSizeReader(Protocol):
    """Read outcome shares without exposing venue responses or credentials."""

    async def shares(self, wallet: str, token_id: str) -> Decimal | None:
        """Return verified shares, or None when holdings evidence is unavailable."""
        ...
