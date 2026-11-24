"""Live order acknowledgements are not confirmed trade settlement evidence."""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.integrations.polymarket.models import VenueSubmitResult
from pmcopy.interfaces.clob_gateway import ClobGateway
from pmcopy.services.execution.execution_service import ExecutionService
from tests.config_helpers import load_test_config
from tests.test_execution_fak_repost_ambiguity import _mock_mds, _order_repo, _req


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["matched", "filled"])
async def test_live_fak_acknowledgement_retains_reconciliation_ownership(status):
    """POST matched quantities remain provisional until the trade feed confirms settlement."""
    gateway = MagicMock(spec=ClobGateway)
    gateway.submit_marketable_order = AsyncMock(
        return_value=VenueSubmitResult(
            success=True,
            order_id="venue-order",
            status=status,
            raw={"size_matched": "10", "avg_price": "0.5"},
        )
    )
    service = ExecutionService(
        AsyncMock(),
        AsyncMock(get_fee_bps=AsyncMock(return_value=Decimal("0"))),
        _order_repo(),
        config=load_test_config(
            execution={"trading_mode": "live"},
            wallet={"private_key": "0x" + "1" * 64},
        ),
        clob_gateway=gateway,
        market_data_service=_mock_mds(),
    )
    result = await service.execute(_req())
    gateway.submit_marketable_order.assert_awaited_once()
    assert result.accepted
    assert result.order_id == "venue-order"
    assert result.status == "submitted_unconfirmed"
    assert result.filled_amount == 0
