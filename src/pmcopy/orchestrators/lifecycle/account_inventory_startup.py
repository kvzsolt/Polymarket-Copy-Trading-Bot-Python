"""Enforce inventory convergence before admitting new account risk at startup."""

from typing import Protocol

from pmcopy.services.positions.position_manager import PositionManager
from pmcopy.services.positions.position_drift_service import PositionDriftService
from pmcopy.storage.postgres.positions_repo import PositionsRepo

from pmcopy.services.observability.health_server import ReadinessTracker
from pmcopy.utils.logger import get_logger

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


class InventoryRuntimeRefresh:
    """Refresh monitoring and per-account drift after a complete inventory sync."""

    def __init__(
        self,
        *,
        user_id: str,
        wallet: str,
        positions: PositionsRepo,
        manager: PositionManager,
        drift: PositionDriftService | None,
    ) -> None:
        self._user_id = user_id
        self._wallet = wallet
        self._positions = positions
        self._manager = manager
        self._drift = drift
        self._readiness: ReadinessTracker | None = None

    def bind_readiness(self, readiness: ReadinessTracker) -> None:
        """Attach the shared tracker when runtime contexts become available."""
        self._readiness = readiness
        readiness.register_drift_account(self._user_id)

    async def refresh(self) -> None:
        """Retire inactive tracking and publish only a completed drift scan."""
        try:
            report = (
                await self._drift.scan_user(user_id=self._user_id, wallet=self._wallet)
                if self._drift is not None
                else None
            )
            positions = await self._positions.list_open_positions(self._user_id)
            self._manager.retain_active_positions({(p.condition_id, p.token_id) for p in positions})
        except Exception:
            if self._readiness is not None:
                self._readiness.update_account_drift(self._user_id, False)
            raise
        if self._readiness is not None and report is not None:
            self._readiness.update_account_drift(self._user_id, bool(report.clean))
