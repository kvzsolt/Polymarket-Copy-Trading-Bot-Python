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
    active_reservations = await reservations_repo.list_active()
    if not active_reservations:
        logger.info("intent.recovery no active reservations to recover")
        return 0

    recovered = 0
    for res in active_reservations:
        scope = PositionScope(
            user_id=res.user_id,
            condition_id=res.condition_id,
            token_id=res.token_id,
        )
        intent_id = res.order_id or res.id
        try:
            await registry.begin_intent(
                scope=scope,
                side=_registry_side(res.side),
                reservation=IntentReservation(
                    quote_usdc=res.reserved_usdc,
                    token_shares=res.reserved_size,
                ),
                intent_id=intent_id,
            )
            if res.order_id or res.venue_order_id:
                await registry.transition(
                    intent_id=intent_id,
                    new_state=IntentState.CONFIRMING,
                    venue_order_id=res.venue_order_id,
                )
            recovered += 1
            logger.info(
                "intent.recovery reservation_id=%s intent_id=%s scope=(%s,%s,%s) "
                "side=%s reserved_usdc=%s reserved_size=%s",
                res.id,
                intent_id,
                res.user_id,
                res.condition_id,
                res.token_id,
                res.side.value,
                res.reserved_usdc,
                res.reserved_size,
            )
        except Exception as exc:
            logger.warning(
                "intent.recovery.skip reservation_id=%s intent_id=%s error=%s",
                res.id,
                intent_id,
                exc,
            )

    logger.info(
        "intent.recovery recovered=%d total_active=%d",
        recovered,
        len(active_reservations),
    )
    return recovered
