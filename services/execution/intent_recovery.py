"""Startup recovery for IntentRegistry (Issue #70).

Recovers in-flight intent state from the durable orders DB after a crash.
The in-memory IntentRegistry starts empty on process restart; this module
reads pending (non-terminal) orders from the database and rebuilds the
registry so that:

* Duplicate-intent blocking is restored — a second BUY on the same scope
  is correctly rejected even after a restart.
* Reserved balance / share accounting is consistent — the aggregate
  ``reserved_for(scope)`` matches what was reserved before the crash.

Design choice: recovery is a single-pass load from the DB at startup,
not a real-time sync. Once recovered, the IntentRegistry is the sole
in-memory authority for the process lifetime.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from domain.order_lifecycle import OrderLifecycleStatus
from services.intentRegistry import IntentRegistry, IntentSide, IntentState, Reservation
from services.positionActionCoordinator import PositionScope

if TYPE_CHECKING:
    from storage.sqlite.orders_repo import OrdersRepo

logger = logging.getLogger(__name__)


# Map OrderLifecycleStatus → IntentState for recovery.
_LIFECYCLE_TO_INTENT: dict[OrderLifecycleStatus, IntentState] = {
    OrderLifecycleStatus.RESERVED: IntentState.INTENT_PENDING,
    OrderLifecycleStatus.SUBMITTING: IntentState.SUBMITTED,
    OrderLifecycleStatus.SUBMITTED_UNCONFIRMED: IntentState.SUBMITTED,
    OrderLifecycleStatus.PARTIALLY_FILLED: IntentState.PARTIAL,
    OrderLifecycleStatus.FILLED: IntentState.DONE,
    OrderLifecycleStatus.RETRYABLE_FAILED: IntentState.ERROR,
    OrderLifecycleStatus.RECONCILIATION_REQUIRED: IntentState.CONFIRMING,
    OrderLifecycleStatus.UNKNOWN_UNCONFIRMED: IntentState.CONFIRMING,
    # Terminal statuses are NOT recovered (they are resolved).
}

# Terminal lifecycle statuses that should NOT be recovered.
_TERMINAL_LIFECYCLE = {
    OrderLifecycleStatus.CANCELLED,
    OrderLifecycleStatus.EXPIRED,
    OrderLifecycleStatus.REJECTED,
    OrderLifecycleStatus.TERMINAL_FAILED,
    OrderLifecycleStatus.FILLED,
}


async def recover_intents_from_db(
    registry: IntentRegistry,
    orders_repo: OrdersRepo,
) -> int:
    """Recover in-flight intents from the orders DB into the IntentRegistry.

    Scans non-terminal orders and re-registers them as in-flight intents
    so that duplicate-intent blocking and reservation accounting are
    consistent after a process restart.

    Returns the number of intents recovered.
    """
    pending_orders = orders_repo.list_non_terminal_orders()
    if not pending_orders:
        logger.info("intent.recovery no pending orders to recover")
        return 0

    recovered = 0
    for order in pending_orders:
        order_status = order.status if hasattr(order, "status") else None
        if order_status and str(order_status) in {
            s.value for s in _TERMINAL_LIFECYCLE
        }:
            continue

        intent_state = _LIFECYCLE_TO_INTENT.get(
            OrderLifecycleStatus(order_status) if order_status else None,
            IntentState.INTENT_PENDING,
        )

        # Skip terminal intents — they're already resolved.
        if intent_state in {IntentState.DONE, IntentState.ERROR, IntentState.CANCELLED}:
            continue

        user_id = getattr(order, "user_id", "") or ""
        condition_id = getattr(order, "condition_id", "") or ""
        token_id = getattr(order, "token_id", "") or ""
        side_str = getattr(order, "side", "BUY") or "BUY"
        side = IntentSide.BUY if side_str.upper() == "BUY" else IntentSide.SELL

        reserved_usdc = Decimal(str(getattr(order, "reserved_usdc", "0") or "0"))
        reserved_size = Decimal(str(getattr(order, "reserved_size", "0") or "0"))

        scope = PositionScope(
            user_id=user_id,
            condition_id=condition_id,
            token_id=token_id,
        )

        try:
            await registry.begin_intent(
                scope=scope,
                side=side,
                reservation=Reservation(
                    quote_usdc=reserved_usdc,
                    token_shares=reserved_size,
                ),
                intent_id=getattr(order, "id", None),
            )
            # If the order was further along than PENDING, transition it.
            if intent_state != IntentState.INTENT_PENDING:
                await registry.transition(
                    intent_id=getattr(order, "id", ""),
                    new_state=intent_state,
                )
            recovered += 1
            logger.info(
                "intent.recovery id=%s scope=(%s,%s,%s) side=%s state=%s "
                "reserved_usdc=%s reserved_size=%s",
                getattr(order, "id", "?"),
                user_id, condition_id, token_id,
                side.value, intent_state.value,
                reserved_usdc, reserved_size,
            )
        except Exception as exc:
            logger.warning(
                "intent.recovery.skip id=%s error=%s",
                getattr(order, "id", "?"), exc,
            )

    logger.info("intent.recovery recovered=%d total_pending=%d", recovered, len(pending_orders))
    return recovered