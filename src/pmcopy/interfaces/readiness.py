"""Narrow health publication contract for owned runtime components."""

from typing import Protocol


class ReadinessWriter(Protocol):
    """Publish health fields without accessing the rest of the application."""

    def update(self, **fields: object) -> None:
        """Update the owned health projection."""
        ...
