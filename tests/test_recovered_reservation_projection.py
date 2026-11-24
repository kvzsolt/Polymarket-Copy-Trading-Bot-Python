"""Every durable allocation survives restart even when several share one token."""

from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.order_lifecycle import IntentReason, IntentSide, Reservation
from pmcopy.services.execution.intent_recovery import recover_intents_from_db
from pmcopy.services.execution.intent_registry import DuplicateIntentError, IntentRegistry, IntentState
from pmcopy.services.positions.position_action_coordinator import PositionScope


@pytest.mark.asyncio
async def test_recovery_keeps_both_allocations_and_releases_only_the_confirmed_one():
    first = Reservation(
        id="res-a",
        user_id="account",
        intent_key="intent-a",
        condition_id="condition",
        token_id="token",
        side=IntentSide.BUY,
        reason=IntentReason.COPY_BUY,
        reserved_usdc=Decimal(5),
        reserved_size=Decimal(0),
        status="active",
        created_at=datetime(2026, 9, 8, tzinfo=timezone.utc),
        expires_at=datetime(2026, 9, 9, tzinfo=timezone.utc),
        order_id="order-a",
    )
    second = replace(
        first, id="res-b", intent_key="intent-b", order_id="order-b", reserved_usdc=Decimal(7)
    )
    reader = AsyncMock(list_active=AsyncMock(return_value=[first, second]))
    registry = IntentRegistry()
    assert await recover_intents_from_db(registry, reader) == 2
    scope = PositionScope("account", "condition", "token")
    assert registry.reserved_for(scope).quote_usdc == Decimal(12)
    assert await recover_intents_from_db(registry, reader) == 2
    assert registry.reserved_for(scope).quote_usdc == Decimal(12)
    await registry.finalize_intent("order-a", IntentState.DONE, by_reconciler=True)
    assert registry.reserved_for(scope).quote_usdc == Decimal(7)
    assert registry.open_intent_for(scope).intent_id == "order-b"
    with pytest.raises(DuplicateIntentError):
        await registry.begin_intent(
            scope, registry.get("order-b").side, registry.reserved_for(scope), intent_id="new"
        )
    await registry.finalize_intent("order-b", IntentState.DONE, by_reconciler=True)
    assert registry.reserved_for(scope).quote_usdc == 0
    assert registry.open_intent_for(scope) is None


@pytest.mark.asyncio
async def test_invalid_recovery_is_not_a_success_shaped_partial_projection(caplog):
    reader = AsyncMock(list_active=AsyncMock(side_effect=RuntimeError("synthetic-secret")))
    with pytest.raises(RuntimeError, match="ownership recovery unavailable") as error:
        await recover_intents_from_db(IntentRegistry(), reader)
    assert "synthetic-secret" not in str(error.value)
    assert "synthetic-secret" not in caplog.text
