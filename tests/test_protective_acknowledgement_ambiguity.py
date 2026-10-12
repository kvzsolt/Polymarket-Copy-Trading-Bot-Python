"""An accepted protective FAK acknowledgement does not prove terminal execution."""

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.integrations.polymarket.models import VenueSubmitResult
from pmcopy.interfaces.clob_gateway import ClobGateway
from tests.test_execution_fak_repost_ambiguity import _build_svc, _req


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["submitted", "live", "delayed", "unknown", "matched", "filled"])
async def test_protective_acknowledgement_cannot_authorize_another_post(status):
    """Retain reconciliation ownership until a terminal FAK result is known."""
    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True,
            order_id="ambiguous-exit",
            status=status,
        )
    )
    service = _build_svc(gateway=gateway)
    request = replace(_req(side="SELL", amount="10", price_limit="0.4"), reason="protective_exit")
    result = await service.execute(request)
    gateway.submit_marketable_order.assert_awaited_once()
    assert result.status == "submitted_unconfirmed"
