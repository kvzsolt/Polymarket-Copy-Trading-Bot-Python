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
        # Realistic CCXT outcome shape: precision.price is the tick, negRisk is
        # top-level, and info is the raw gamma market with orderMinSize.
        self.outcome_value = {
            "outcomeId": TOKEN,
            "precision": {"price": "0.01"},
            "negRisk": False,
            "info": {"orderMinSize": "5"},
        }
        self.create_error: BaseException | None = None
        self.read_error: BaseException | None = None
        self.cancel_error: BaseException | None = None

    async def load_outcome(self, _token):
        return self.outcome_value

    async def fetch_order_book(self, _token):
        if self.read_error:
            raise self.read_error
        # Realistic parsed-book shape: no execution metadata, no "info" key.
        return {
            "outcomeId": TOKEN,
            "bids": [],
            "asks": [],
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


@pytest.fixture(scope="module")
def ccxt_errors():
    """Yield the real CCXT error classes, then unload CCXT.

    The gateway classifies by exception *class name*, so the real vendor
    classes — not local stand-ins — are what prove the reason survives.

    ``tests/test_service_config_loader.py`` asserts no venue client is present
    in ``sys.modules`` — a session-global check — so the CCXT modules this
    import introduces are removed again on teardown.
    """
    import sys

    before = set(sys.modules)
    from ccxt.base import errors

    try:
        yield errors
    finally:
        for name in set(sys.modules) - before:
            if name == "ccxt" or name.startswith("ccxt."):
                del sys.modules[name]


@pytest.mark.asyncio
async def test_rejection_carries_the_venue_reason_not_just_the_class_name(ccxt_errors) -> None:
    """The CLOB's own rejection body must reach the operator.

    ``InvalidOrder`` alone is unactionable: the same class covers a bad price,
    a bad size and a bad token. The CCXT message carries the CLOB body
    (``{"error":"invalid amount"}``) that says which.
    """
    exchange = Exchange()
    exchange.create_error = ccxt_errors.InvalidOrder('polymarket {"error":"invalid amount"}')

    with pytest.raises(VenueInvalidOrderError) as excinfo:
        await market_request(CcxtPredictionGateway(exchange))

    message = str(excinfo.value)
    assert "InvalidOrder" in message
    assert "invalid amount" in message


@pytest.mark.asyncio
async def test_read_rejection_carries_the_venue_reason(ccxt_errors) -> None:
    """The read path surfaces the same reason instead of erasing it."""
    exchange = Exchange()
    exchange.read_error = ccxt_errors.InvalidOrder('polymarket {"error":"invalid token id"}')

    with pytest.raises(VenueInvalidOrderError) as excinfo:
        await CcxtPredictionGateway(exchange).get_order("order")

    assert "invalid token id" in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("secret", "body"),
    [
        ("abc123secret", "GET https://clob.example/order?api_key=abc123secret failed"),
        (
            "0x" + "ab" * 32,
            "rejected signature 0x" + "ab" * 32 + ' for {"error":"invalid amount"}',
        ),
    ],
)
async def test_venue_reason_is_scrubbed_of_credentials(ccxt_errors, secret: str, body: str) -> None:
    """A venue message may echo a signed URL or key material; never log it."""
    exchange = Exchange()
    exchange.create_error = ccxt_errors.InvalidOrder(body)

    with pytest.raises(VenueInvalidOrderError) as excinfo:
        await market_request(CcxtPredictionGateway(exchange))

    assert secret not in str(excinfo.value)


@pytest.mark.asyncio
async def test_venue_reason_is_bounded(ccxt_errors) -> None:
    """An unbounded venue body must not flood the log or the order record."""
    exchange = Exchange()
    exchange.create_error = ccxt_errors.InvalidOrder("x" * 5000)

    with pytest.raises(VenueInvalidOrderError) as excinfo:
        await market_request(CcxtPredictionGateway(exchange))

    assert len(str(excinfo.value)) < 400


@pytest.mark.asyncio
async def test_auth_rejection_stays_opaque(ccxt_errors) -> None:
    """Auth failures keep their fixed message: the body can carry credentials."""
    exchange = Exchange()
    exchange.create_error = ccxt_errors.AuthenticationError(
        "POLY_API_KEY deadbeef is not authorized"
    )

    with pytest.raises(VenueAuthError) as excinfo:
        await market_request(CcxtPredictionGateway(exchange))

    assert str(excinfo.value) == "Polymarket CCXT authentication rejected"


@pytest.mark.asyncio
async def test_unclassified_exception_after_post_is_ambiguous_not_retryable_outage() -> None:
    exchange = Exchange()
    exchange.create_error = error_type("ExchangeError")("malformed successful response")

    result = await market_request(CcxtPredictionGateway(exchange))

    assert result.success is False
    assert result.ambiguous_post_failure is True
    assert result.non_retryable is False
