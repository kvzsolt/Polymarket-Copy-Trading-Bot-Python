"""Enforce inventory convergence before admitting new account risk at startup."""

from typing import Protocol

from services.healthServer import ReadinessTracker
from utils.logger import get_logger

logger = get_logger("InventoryStartup")


class InventorySyncPort(Protocol):
    """Provide the existing account-bound inventory initialization operation."""

    async def initial_sync(self) -> None:
        """Apply the initial inventory or raise when required state is unavailable."""
        ...


class InitialInventorySync:
    """Abort startup with a visible inhibit when inventory cannot converge."""

    def __init__(self, readiness: ReadinessTracker) -> None:
        self._readiness = readiness

    async def initialize(self, sync: InventorySyncPort | None) -> None:
        """Initialize one bound account without overriding existing inhibits.

        Args:
            sync: The account's canonical inventory synchronization service.

        Raises:
            RuntimeError: A binding or required snapshot/application step failed.
        """
        try:
            if sync is None:
                raise RuntimeError("Inventory synchronization is not bound")
            await sync.initial_sync()
        except Exception as exc:
            self._readiness.update(reconciler_ok=False)
            logger.error("initial_inventory_unavailable error_type=%s", type(exc).__name__)
            raise RuntimeError("Initial account inventory is unavailable") from None
