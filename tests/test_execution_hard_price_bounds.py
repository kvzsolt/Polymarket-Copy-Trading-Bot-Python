"""The application must preserve caller price bounds before the venue adapter."""

from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.integrations.polymarket.models import VenueSubmitResult
from pmcopy.interfaces.clob_gateway import ClobGateway
from tests.test_execution_fak_repost_ambiguity import _build_svc, _req


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side", "bound", "tick", "expected"),
    [
        ("BUY", "0.413", "0.005", "0.410"),
        ("SELL", "0.413", "0.005", "0.415"),
        ("BUY", "0.413", "0.0025", "0.4125"),
        ("SELL", "0.413", "0.0025", "0.4150"),
        ("BUY", "0.002", "0.005", None),
        ("SELL", "0.998", "0.005", None),
    ],
)
async def test_fak_submission_respects_original_hard_bound(side, bound, tick, expected):
    """Reject a non-executable bound and otherwise snap toward safety."""
    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=False,
            status="canceled",
            error="order_not_fillable",
            non_retryable=True,
        )
    )
    service = _build_svc(gateway=gateway)
    request = replace(
        _req(side=side, amount="1000", price_limit=bound),
        tick_size=Decimal(tick),
    )
    result = await service.execute(request)
    if expected is None:
        gateway.submit_marketable_order.assert_not_awaited()
        assert result.status == "price_limit_unexecutable"
    else:
        gateway.submit_marketable_order.assert_awaited_once()
        submitted = gateway.submit_marketable_order.await_args.args[0]
        assert submitted.price_limit == Decimal(expected)
