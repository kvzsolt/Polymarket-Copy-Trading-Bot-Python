"""Fee rates do not prove fee amounts, and unknown fees cannot certify net performance."""

from decimal import Decimal

import pytest

from pmcopy.integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from pmcopy.integrations.polymarket.models import VenueFill
from pmcopy.services.execution.reconciliation_service import ReconciliationService
from pmcopy.services.trading.leader_performance import ClosedEpisodeAccumulator, PerformanceFill
from pmcopy.domain.detection import TradeSide
from tests.test_ccxt_prediction_gateway_contract import FakeExchange
from unittest.mock import AsyncMock
from tests.test_reconciler_idempotent_transitions import _make_service, _record


@pytest.mark.asyncio
async def test_ccxt_fee_rate_only_remains_unknown_fee_amount():
    """The pinned parser supplies rate without cost for private CLOB trades."""
    exchange = FakeExchange()
    exchange.trades = [
        {
            "id": "fill",
            "order": "order",
            "amount": "5",
            "price": ".5",
            "side": "buy",
            "fee": {"rate": ".002", "currency": "USDC"},
            "info": {
                "id": "fill",
                "asset_id": "123",
                "taker_order_id": "order",
                "status": "CONFIRMED",
            },
        }
    ]
    fills = await CcxtPredictionGateway(exchange).get_fills(order_id="order")
    assert len(fills) == 1
    assert fills[0].fee is None


@pytest.mark.parametrize(
    "fee,expected", [(Decimal("0"), "0"), (Decimal(".02"), "0.02"), (None, None)]
)
def test_confirmed_fill_aggregation_preserves_fee_amount_evidence(fee, expected):
    fill = VenueFill(
        fill_id="fill", order_id="order", size=Decimal("5"), price=Decimal(".5"), fee=fee
    )
    result = ReconciliationService._aggregate_fills([fill], status="confirmed", source="test")
    assert result["fee_amount"] == expected


def test_unknown_fees_are_insufficient_evidence_of_net_episode_performance():
    accumulator = ClosedEpisodeAccumulator()
    accumulator.apply(
        PerformanceFill("buy", "token", "task", TradeSide.BUY, Decimal("10"), Decimal(".5"), None)
    )
    accumulator.apply(
        PerformanceFill(
            "sell", "token", "task", TradeSide.SELL, Decimal("10"), Decimal(".51"), Decimal("0")
        )
    )
    assert accumulator.stats.closed_episodes == 0
    assert accumulator.stats.win_rate_fraction is None
    assert accumulator.stats.excluded_scopes == 1


@pytest.mark.asyncio
async def test_reconciliation_persists_an_explicit_fee_instead_of_zero():
    service = _make_service()
    await service.reconcile_trade_record(
        _record(),
        "BUY",
        confirm_fill=AsyncMock(
            return_value={
                "status": "filled",
                "size_matched": "5",
                "avg_price": "0.5",
                "fee_amount": "0.02",
            }
        ),
    )
    assert service.fills_repo.fills[0].fee == Decimal("0.02")


@pytest.mark.asyncio
async def test_trade_wide_fee_is_not_duplicated_across_maker_orders():
    exchange = FakeExchange()
    exchange.trades = [
        {
            "id": "fill",
            "fee": {"cost": ".04"},
            "info": {
                "trader_side": "MAKER",
                "status": "CONFIRMED",
                "maker_orders": [
                    {
                        "order_id": "maker-a",
                        "asset_id": "123",
                        "side": "BUY",
                        "matched_amount": "2",
                        "price": ".5",
                    },
                    {
                        "order_id": "maker-b",
                        "asset_id": "123",
                        "side": "BUY",
                        "matched_amount": "3",
                        "price": ".5",
                        "fee_amount": ".01",
                    },
                ],
            },
        }
    ]
    fills = await CcxtPredictionGateway(exchange).get_fills()
    assert [(fill.order_id, fill.fee) for fill in fills] == [
        ("maker-a", None),
        ("maker-b", Decimal(".01")),
    ]
