"""Regression tests for one-shot copy-trade FAK execution policy."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.domain.execution import ExecutionOrderType
from pmcopy.integrations.polymarket.models import VenueSubmitResult
from pmcopy.interfaces.clob_gateway import ClobGateway
from tests.test_execution_fak_repost_ambiguity import _build_svc, _req


@pytest.mark.asyncio
async def test_zero_fill_fak_submits_once_and_never_chases_remainder() -> None:
    """A killed zero-fill copy intent produces no second venue POST."""

    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=False,
            order_id="ord-zero",
            status="cancelled",
            error="fak_no_match",
            non_retryable=True,
        )
    )
    gateway.get_order = AsyncMock(return_value=None)
    service = _build_svc(gateway=gateway)

    result = await service.execute(_req(order_type=ExecutionOrderType.FAK))

    assert gateway.submit_marketable_order.await_count == 1
    assert result.accepted is False
    assert result.filled_amount == Decimal("0")
    assert result.status == "cancelled"


@pytest.mark.asyncio
async def test_partial_fak_fill_accounts_only_match_and_never_reposts() -> None:
    """A partial FAK match has no local open remainder or chase POST."""

    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True,
            order_id="ord-partial",
            status="matched",
            raw={"size_matched": "3", "avg_price": "0.50"},
        )
    )
    gateway.get_order = AsyncMock(return_value=None)
    service = _build_svc(gateway=gateway)

    result = await service.execute(_req(amount="10", order_type=ExecutionOrderType.FAK))

    assert gateway.submit_marketable_order.await_count == 1
    assert result.accepted is True
    assert result.status == "partially_filled"
    assert result.filled_amount == Decimal("3")
    assert result.avg_price == Decimal("0.50")


@pytest.mark.asyncio
async def test_complete_fak_fill_accounts_the_confirmed_match_once() -> None:
    """A complete FAK records exactly the venue-confirmed matched amount."""

    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True,
            order_id="ord-complete",
            status="matched",
            raw={"size_matched": "20", "avg_price": "0.50"},
        )
    )
    gateway.get_order = AsyncMock(return_value=None)
    service = _build_svc(gateway=gateway)

    result = await service.execute(_req(amount="10", order_type=ExecutionOrderType.FAK))

    assert gateway.submit_marketable_order.await_count == 1
    assert result.accepted is True
    assert result.status == "filled"
    assert result.filled_amount == Decimal("20")
    assert result.avg_price == Decimal("0.50")


@pytest.mark.asyncio
async def test_exchange_rejection_fak_submits_once() -> None:
    """A terminal venue rejection is structured and never retried."""

    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=False,
            status="rejected",
            error="minimum order size",
            non_retryable=True,
        )
    )
    gateway.get_order = AsyncMock(return_value=None)
    service = _build_svc(gateway=gateway)

    result = await service.execute(_req(order_type=ExecutionOrderType.FAK))

    assert gateway.submit_marketable_order.await_count == 1
    assert result.accepted is False
    assert result.status == "rejected"


@pytest.mark.asyncio
async def test_retryable_fak_failure_never_waits_without_another_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A one-shot FAK does not pay retry backoff after its final response."""

    sleep = AsyncMock()
    jitter = MagicMock(return_value=0.01)
    monkeypatch.setattr("pmcopy.services.executionService.asyncio.sleep", sleep)
    monkeypatch.setattr("pmcopy.services.executionService.random.uniform", jitter)
    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=False,
            status="failed",
            error="temporary venue refusal",
        )
    )
    gateway.get_order = AsyncMock(return_value=None)
    service = _build_svc(gateway=gateway)

    result = await service.execute(_req(order_type=ExecutionOrderType.FAK))

    assert gateway.submit_marketable_order.await_count == 1
    assert result.accepted is False
    sleep.assert_not_awaited()
    jitter.assert_not_called()


@pytest.mark.asyncio
async def test_ambiguous_fak_response_submits_once_and_defers_to_reconciliation() -> None:
    """A timeout after POST never resends an intent whose fate is unknown."""

    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=False,
            status="submitted_unconfirmed",
            error="timeout after POST",
            ambiguous_post_failure=True,
        )
    )
    gateway.get_order = AsyncMock(return_value=None)
    service = _build_svc(gateway=gateway)

    result = await service.execute(_req(order_type=ExecutionOrderType.FAK))

    assert gateway.submit_marketable_order.await_count == 1
    assert result.accepted is True
    assert result.status == "submitted_unconfirmed"
    assert result.filled_amount == Decimal("0")
