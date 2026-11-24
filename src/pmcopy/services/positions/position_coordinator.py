"""Phase 4 ``PositionCoordinator``.

Composes the existing :class:`PositionActionCoordinator` (per-scope lock)
with the new :class:`IntentRegistry` (reservations + duplicate-blocking +
reconciler-owned terminal transitions).

Spec usage shape::

    async with coordinator.acquire(position_key):
        await coordinator.begin_intent(...)
        # submit order
        # wait for order/trade events
        # reconciler finalizes state

This is a thin composition layer so existing call sites that already use
``PositionActionCoordinator.acquire`` keep working unchanged while new
lifecycle work is funneled through the registry.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from pmcopy.services.execution.intent_registry import (
    DuplicateIntentError,
    IllegalTransitionError,
    IntentRecord,
    IntentRegistry,
    IntentSide,
    IntentState,
    Reservation,
    UnknownIntentError,
)
from pmcopy.services.positions.position_action_coordinator import (
    PositionActionCoordinator,
    PositionScope,
)


logger = logging.getLogger(__name__)


__all__ = [
    "DuplicateIntentError",
    "IllegalTransitionError",
    "IntentRecord",
    "IntentSide",
    "IntentState",
    "PositionCoordinator",
    "PositionScope",
    "Reservation",
    "UnknownIntentError",
]


class PositionCoordinator:
    """Phase 4 coordinator: per-scope lock + intent registry."""

    def __init__(
        self,
        action_coordinator: Optional[PositionActionCoordinator] = None,
        intent_registry: Optional[IntentRegistry] = None,
    ) -> None:
        self._action = action_coordinator or PositionActionCoordinator()
        self._intents = intent_registry or IntentRegistry()

    @property
    def action_coordinator(self) -> PositionActionCoordinator:
        return self._action

    @property
    def intent_registry(self) -> IntentRegistry:
        return self._intents

    @asynccontextmanager
    async def acquire(self, scope: PositionScope) -> AsyncIterator[None]:
        """Hold the per-scope lock for the duration of the body."""
        async with self._action.acquire(scope):
            yield

    async def begin_intent(
        self,
        scope: PositionScope,
        side: IntentSide,
        reservation: Reservation,
        intent_id: Optional[str] = None,
    ) -> IntentRecord:
        return await self._intents.begin_intent(
            scope=scope,
            side=side,
            reservation=reservation,
            intent_id=intent_id,
        )

    async def transition(
        self,
        intent_id: str,
        new_state: IntentState,
        venue_order_id: Optional[str] = None,
    ) -> IntentRecord:
        return await self._intents.transition(
            intent_id=intent_id,
            new_state=new_state,
            venue_order_id=venue_order_id,
        )

    async def finalize_intent(
        self,
        intent_id: str,
        terminal_state: IntentState,
        *,
        by_reconciler: bool = False,
    ) -> IntentRecord:
        return await self._intents.finalize_intent(
            intent_id=intent_id,
            terminal_state=terminal_state,
            by_reconciler=by_reconciler,
        )

    async def release_on_cancel_or_fail(
        self,
        intent_id: str,
        terminal_state: IntentState = IntentState.CANCELLED,
    ) -> IntentRecord:
        return await self._intents.release_on_cancel_or_fail(
            intent_id=intent_id,
            terminal_state=terminal_state,
        )

    def open_intent_for(self, scope: PositionScope) -> Optional[IntentRecord]:
        return self._intents.open_intent_for(scope)

    def reserved_for(self, scope: PositionScope) -> Reservation:
        return self._intents.reserved_for(scope)

    def open_intent_count(self) -> int:
        return self._intents.open_intent_count()

    def is_locked(self, scope: PositionScope) -> bool:
        return self._action.is_locked(scope)
