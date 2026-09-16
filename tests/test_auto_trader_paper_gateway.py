"""Paper evaluations obtain actual market fees exclusively through CCXT reads."""

from decimal import Decimal as D

import pytest

from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from integrations.polymarket.errors import VenueInvalidOrderError
from integrations.polymarket.paper_market_reader import CcxtPaperMarketReader
from tests.test_ccxt_prediction_gateway_contract import FakeExchange, TOKEN


async def test_market_fee_curve_is_read_without_private_methods():
    exchange = FakeExchange()
    exchange.clob_market["fd"] = {"r": ".05", "e": 1, "to": True}
    gateway = CcxtPaperMarketReader(CcxtPredictionGateway(exchange))
    assert await gateway.get_paper_fee_rate(TOKEN, condition_id="condition") == D(".05")
    assert exchange.created == []


@pytest.mark.parametrize(
    "fee", [None, {}, {"r": "0.05", "e": 2}, {"r": "-1", "e": 1}, {"r": "NaN", "e": 1}]
)
async def test_missing_or_unsupported_fee_curves_fail_closed(fee):
    exchange = FakeExchange()
    exchange.clob_market["fd"] = fee
    with pytest.raises(VenueInvalidOrderError):
        await CcxtPaperMarketReader(CcxtPredictionGateway(exchange)).get_paper_fee_rate(
            TOKEN, condition_id="condition"
        )


async def test_depth_simulator_writes_only_to_paper_gateway(monkeypatch):
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock
    from domain.paper_evaluation import PaperBook
    from integrations.polymarket.paper_evaluation_execution import PaperEvaluationExecution

    from integrations.polymarket.paper_execution import PaperExecutionGateway

    original = PaperExecutionGateway.submit_marketable_order
    captured = []

    async def capture(self, request):
        captured.append(request.token_id)
        return await original(self, request)

    monkeypatch.setattr(PaperExecutionGateway, "submit_marketable_order", capture)
    market_gateway = AsyncMock()
    market_gateway.submit_marketable_order.side_effect = AssertionError("private write")
    execution = PaperEvaluationExecution(market_gateway)
    book = PaperBook(((D(".5"), D("2")),), (), D(".005"), D("1"), D(".05"))
    fill = await execution.fill(
        book,
        token_id=TOKEN,
        side="BUY",
        size=D("10"),
        limit=D(".5"),
        event="trade",
        now=datetime(2026, 9, 16, tzinfo=UTC),
    )
    assert fill.shares == D("2") and fill.fee == D(".025")
    market_gateway.submit_marketable_order.assert_not_called()
    assert captured == [TOKEN]
