from __future__ import annotations

from decimal import Decimal

import pytest

from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from integrations.polymarket.errors import (
    VenueAuthError,
    VenueInsufficientBalanceError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
    VenueOrderbookNotFoundError,
    VenueRateLimitError,
    VenueUnavailableError,
)
from integrations.polymarket.models import LimitOrderRequest, MarketableOrderRequest

TOKEN = "123"


def error_type(name: str, status: int | None = None):
    attrs = {"status": status} if status is not None else {}
    return type(name, (Exception,), attrs)


class Exchange:
    def __init__(self) -> None:
        self.outcome_value = {
            "precision": {"price": "0.01"},
            "limits": {"amount": {"min": "5"}},
            "negRisk": False,
        }
        self.create_error: BaseException | None = None
        self.read_error: BaseException | None = None
        self.cancel_error: BaseException | None = None

    async def load_outcome(self, _token):
        return self.outcome_value

    async def fetch_order_book(self, _token):
        if self.read_error:
            raise self.read_error
        return {
            "bids": [],
            "asks": [],
            "info": {
                "tick_size": "0.01",
                "min_order_size": "5",
                "neg_risk": False,
            },
        }

    async def create_order(self, *_args):
        if self.create_error:
            raise self.create_error
        return {"id": "order", "status": "open"}

    async def fetch_order(self, _id):
        if self.read_error:
            raise self.read_error
        return {"id": "order", "status": "open"}

    async def cancel_order(self, _id):
        if self.cancel_error:
            raise self.cancel_error
        return {"status": "canceled"}


async def market_request(gateway: CcxtPredictionGateway):
    return await gateway.submit_marketable_order(
        MarketableOrderRequest(
            token_id=TOKEN,
            side="BUY",
            size=Decimal("5"),
            price_limit=Decimal("0.42"),
            condition_id="condition",
        )
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["NetworkError", "RequestTimeout", "ExchangeNotAvailable"])
async def test_post_transport_failures_are_ambiguous_and_not_raised(name: str) -> None:
    exchange = Exchange()
    exchange.create_error = error_type(name)("boom")

    result = await market_request(CcxtPredictionGateway(exchange))

    assert result.success is False
    assert result.ambiguous_post_failure is True
    assert result.non_retryable is False


@pytest.mark.asyncio
async def test_fok_not_fillable_is_terminal_cancel_not_retryable() -> None:
    exchange = Exchange()
    exchange.create_error = error_type("OrderNotFillable")("no match")

    result = await market_request(CcxtPredictionGateway(exchange))

    assert result.status == "canceled"
    assert result.non_retryable is True
    assert result.ambiguous_post_failure is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("AuthenticationError", VenueAuthError),
        ("PermissionDenied", VenueAuthError),
        ("InsufficientFunds", VenueInsufficientBalanceError),
        ("InvalidOrder", VenueInvalidOrderError),
        ("BadRequest", VenueInvalidOrderError),
        ("RateLimitExceeded", VenueRateLimitError),
    ],
)
async def test_post_errors_are_classified(name: str, expected: type[Exception]) -> None:
    exchange = Exchange()
    exchange.create_error = error_type(name)("boom")

    with pytest.raises(expected):
        await market_request(CcxtPredictionGateway(exchange))


@pytest.mark.asyncio
async def test_malformed_success_response_is_ambiguous() -> None:
    exchange = Exchange()

    async def malformed(*_args):
        return {"status": "open", "info": {"success": True}}

    exchange.create_order = malformed
    result = await market_request(CcxtPredictionGateway(exchange))
    assert result.ambiguous_post_failure is True


@pytest.mark.asyncio
async def test_explicit_rejection_is_terminal() -> None:
    exchange = Exchange()

    async def rejected(*_args):
        return {
            "status": "rejected",
            "info": {"success": False, "errorMsg": "invalid signature"},
        }

    exchange.create_order = rejected
    result = await market_request(CcxtPredictionGateway(exchange))
    assert result.success is False
    assert result.non_retryable is True
    assert result.error == "invalid signature"


@pytest.mark.asyncio
async def test_orderbook_bad_symbol_maps_to_not_found() -> None:
    exchange = Exchange()
    exchange.read_error = error_type("BadSymbol")("missing")
    with pytest.raises(VenueOrderbookNotFoundError):
        await CcxtPredictionGateway(exchange).get_orderbook(TOKEN)


@pytest.mark.asyncio
async def test_order_not_found_is_distinct_from_venue_outage() -> None:
    exchange = Exchange()
    exchange.read_error = error_type("OrderNotFound")("missing")
    with pytest.raises(VenueOrderNotFoundError):
        await CcxtPredictionGateway(exchange).get_order("missing")


@pytest.mark.asyncio
async def test_read_transport_error_is_venue_unavailable() -> None:
    exchange = Exchange()
    exchange.read_error = error_type("NetworkError")("down")
    with pytest.raises(VenueUnavailableError):
        await CcxtPredictionGateway(exchange).get_orderbook(TOKEN)


@pytest.mark.asyncio
async def test_cancel_404_is_idempotent_success() -> None:
    exchange = Exchange()
    exchange.cancel_error = error_type("OrderNotFound", status=404)("gone")
    result = await CcxtPredictionGateway(exchange).cancel_order("gone")
    assert result.success is True
    assert result.status == "already_gone"


@pytest.mark.asyncio
async def test_limit_missing_condition_never_submits() -> None:
    exchange = Exchange()
    result = await CcxtPredictionGateway(exchange).submit_limit_order(
        LimitOrderRequest(
            token_id=TOKEN,
            side="BUY",
            price=Decimal("0.42"),
            size=Decimal("5"),
            condition_id=None,
        )
    )
    assert result.success is False
    assert result.non_retryable is True

@pytest.mark.asyncio
async def test_unclassified_exception_after_post_is_ambiguous_not_retryable_outage() -> None:
    exchange = Exchange()
    exchange.create_error = error_type("ExchangeError")("malformed successful response")

    result = await market_request(CcxtPredictionGateway(exchange))

    assert result.success is False
    assert result.ambiguous_post_failure is True
    assert result.non_retryable is False

