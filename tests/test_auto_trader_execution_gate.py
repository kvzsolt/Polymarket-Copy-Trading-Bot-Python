"""Both copy execution adapters share the same durable membership gate."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pmcopy.services.execution.coordinator import ExecutionCoordinator, ExecutionIntent


async def test_copy_admission_rejects_before_reservation_or_venue_submission():
    guard = SimpleNamespace(admit=AsyncMock(return_value=False))
    coordinator = ExecutionCoordinator(None, None, copy_admission=guard)
    coordinator._submit_with_reservation = AsyncMock()
    intent = ExecutionIntent(
        "account",
        "market",
        "123",
        "BUY",
        Decimal(5),
        Decimal(".5"),
        Decimal(5),
        leader_wallet="0x" + "a" * 40,
    )
    result = await coordinator.submit_copy_trade(intent)
    assert result.accepted is False
    assert result.skip_code == "auto_trader_not_active"
    coordinator._submit_with_reservation.assert_not_called()


async def test_qualified_owned_sell_continues_through_existing_execution():
    guard = SimpleNamespace(admit=AsyncMock(return_value=True))
    coordinator = ExecutionCoordinator(None, None, copy_admission=guard)
    coordinator._submit_with_reservation = AsyncMock(return_value="delegated")
    intent = ExecutionIntent(
        "account",
        "market",
        "123",
        "SELL",
        Decimal(5),
        Decimal(".5"),
        Decimal("2.5"),
        leader_wallet="0x" + "a" * 40,
    )
    assert await coordinator.submit_copy_trade(intent) == "delegated"
    guard.admit.assert_awaited_once_with("account", "0x" + "a" * 40, "SELL")
