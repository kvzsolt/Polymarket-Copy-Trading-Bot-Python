"""Task allocations share the configured account market capacity atomically."""

import asyncio
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from pmcopy.domain.order_lifecycle import IntentSide
from pmcopy.orchestrators.bootstrap.runtime_bootstrap import _build_execution_coordinator
from pmcopy.services.execution.reservation_service import InsufficientFreeQuote
from pmcopy.storage.postgres.positions_repo import PositionsRepo
from tests.config_helpers import load_test_config
from tests.test_reservation_service import _pg_pool, create_mock_intent

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_distinct_tasks_cannot_exceed_combined_market_cap(monkeypatch):
    """Two six-collateral allocations cannot both reserve against a ten-collateral cap."""
    monkeypatch.setattr("pmcopy.utils.wallet_balance.get_my_balance", AsyncMock(return_value=Decimal("100")))
    async with _pg_pool() as pool:
        config = load_test_config(
            execution={"reservations_enabled": True},
            sizing={"max_position_pct": "0.1"},
        )
        coordinator = _build_execution_coordinator(
            pool=pool,
            positions_repo=PositionsRepo(pool),
            execution_service=AsyncMock(),
            funder_address="0x" + "2" * 40,
            config=config,
        )
        intents = tuple(
            replace(
                create_mock_intent(
                    IntentSide.BUY, size=Decimal("12"), price=Decimal("0.5"), source_event_id=task
                ),
                task_id=task,
            )
            for task in ("task-a", "task-b")
        )
        results = await asyncio.gather(
            *(coordinator.reservation_service.reserve_for_intent(intent) for intent in intents),
            return_exceptions=True,
        )
        assert sum(isinstance(result, InsufficientFreeQuote) for result in results) == 1, results
        assert await coordinator.reservation_service.reservations_repo.sum_active_quote_for_user(
            "u1"
        ) == Decimal("6")
