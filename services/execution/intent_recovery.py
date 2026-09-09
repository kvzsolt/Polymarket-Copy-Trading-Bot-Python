"""Startup recovery for active execution reservations (Issue #70).

The durable source of in-flight reservation ownership is
``order_reservations``.  On process restart, rebuild the process-local
``IntentRegistry`` from active reservation rows so duplicate-intent blocking and
reserved amount accounting match the database instead of relying on fake order
columns that the submission ``OrdersRepo`` does not own.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from domain.order_lifecycle import IntentSide as LifecycleIntentSide
from services.intentRegistry import (
    IntentRegistry,
    IntentRecord,
    IntentSide as RegistryIntentSide,
    IntentState,
    Reservation as IntentReservation,
)
from services.positionActionCoordinator import PositionScope

if TYPE_CHECKING:
    from storage.postgres.reservations_repo import ReservationsRepo

logger = logging.getLogger(__name__)


def _registry_side(side: LifecycleIntentSide | str) -> RegistryIntentSide:
    value = side.value if isinstance(side, LifecycleIntentSide) else str(side)
    return RegistryIntentSide.SELL if value.upper() == "SELL" else RegistryIntentSide.BUY


async def recover_intents_from_db(
    registry: IntentRegistry,
    reservations_repo: ReservationsRepo,
) -> int:
    """Recover active reservations into the in-memory IntentRegistry.

    Only active rows from ``order_reservations`` are recovered. Released rows
    have already reached a terminal decision and must not re-open an intent.
    """
    return await IntentRecoveryService(registry, reservations_repo).restore()


class IntentRecoveryService:
    """Rebuild every allocation from durable reservation ownership or fail startup."""

    def __init__(self, registry: IntentRegistry, reservations_repo: ReservationsRepo) -> None:
        """Inject the process projection and the existing durable reader."""
        self._registry = registry
        self._reservations = reservations_repo

    async def restore(self) -> int:
        """Return the number of restored rows, preserving cancellation and sanitized failures."""
        try:
            active = await self._reservations.list_active()
            for reservation in active:
                await self._registry.restore_intent(
                    IntentRecord(
                        intent_id=reservation.order_id or reservation.id,
                        scope=PositionScope(
                            user_id=reservation.user_id,
                            condition_id=reservation.condition_id,
                            token_id=reservation.token_id,
                        ),
                        side=_registry_side(reservation.side),
                        reservation=IntentReservation(
                            quote_usdc=reservation.reserved_usdc,
                            token_shares=reservation.reserved_size,
                        ),
                        state=(
                            IntentState.CONFIRMING
                            if reservation.order_id or reservation.venue_order_id
                            else IntentState.INTENT_PENDING
                        ),
                        created_at=reservation.created_at.timestamp(),
                        updated_at=reservation.created_at.timestamp(),
                        venue_order_id=reservation.venue_order_id,
                    )
                )
        except Exception as exc:
            logger.error("intent_recovery_failed", extra={"error_type": type(exc).__name__})
            raise RuntimeError("reservation ownership recovery unavailable") from None
        logger.info("intent.recovery recovered=%d", len(active))
        return len(active)
