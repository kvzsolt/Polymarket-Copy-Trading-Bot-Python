from __future__ import annotations

from decimal import Decimal

import pytest

from pmcopy.integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from pmcopy.integrations.polymarket.errors import (
    VenueAuthError,
    VenueInsufficientBalanceError,
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
    VenueOrderbookNotFoundError,
    VenueRateLimitError,
    VenueSchemaError,
    VenueUnavailableError,
)
from pmcopy.integrations.polymarket.models import LimitOrderRequest, MarketableOrderRequest

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
        self.order_value: object = {"id": "order", "status": "open"}

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

    async def clobPublicGetClobMarketsConditionId(self, _params):
        """Return the metadata precondition so POST-error tests reach POST."""
        return {"tokens": [TOKEN], "mts": "0.01", "mos": "5"}

    async def clobPublicGetNegRiskTokenId(self, _params):
        """Return explicit token risk metadata without fetching a book."""
        return {"neg_risk": False}

    async def create_order(self, *_args):
        if self.create_error:
            raise self.create_error
        return {"id": "order", "status": "open"}

    async def fetch_order(self, _id):
        if self.read_error:
            raise self.read_error
        return self.order_value

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
async def test_ccxt_null_order_shape_maps_to_not_found() -> None:
    """Pinned CCXT 4.5.75 parses a CLOB JSON null into this empty order."""

    exchange = Exchange()
    exchange.order_value = {
        "id": None,
        "status": None,
        "filled": 0.0,
        "amount": None,
        "price": None,
        "info": "null",
    }

    with pytest.raises(VenueOrderNotFoundError):
        await CcxtPredictionGateway(exchange).get_order("missing")


@pytest.mark.asyncio
async def test_missing_order_id_with_non_null_info_remains_invalid() -> None:
    """Only CCXT's exact null sentinel is absence; malformed data still fails."""

    exchange = Exchange()
    exchange.order_value = {
        "id": None,
        "status": None,
        "info": {"status": "live"},
    }

    with pytest.raises(VenueInvalidOrderError, match="omitted id"):
        await CcxtPredictionGateway(exchange).get_order("malformed")


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


@pytest.mark.asyncio
async def test_not_fillable_preserves_the_venue_reason() -> None:
    """Keep the venue's own words so the two FAK kills stay distinguishable.

    Polymarket answers an uncrossable FAK with "no orders found to match with
    FAK order" and a partially-filled FOK with "order couldn't be fully
    filled". Collapsing both to the bare token ``order_not_fillable`` made it
    impossible to tell from the logs whether the book was empty at our limit or
    merely too thin, which is the difference between mispricing and bad luck.
    """
    exchange = Exchange()
    exchange.create_error = error_type("OrderNotFillable")(
        'polymarket {"error":"no orders found to match with FAK order. FAK orders '
        'are partially filled or killed if no match is found."}'
    )

    result = await market_request(CcxtPredictionGateway(exchange))

    assert result.status == "canceled"
    assert result.non_retryable is True
    # ``error`` stays an exact token: telemetry and the v1 task adapter both
    # classify on equality, so the detail belongs in its own field.
    assert result.error == "order_not_fillable"
    assert result.venue_reason is not None
    assert "no orders found to match with FAK order" in result.venue_reason


@pytest.mark.asyncio
async def test_not_fillable_redacts_secrets_in_the_venue_reason() -> None:
    """The preserved text goes through the same scrubber as other rejections."""
    exchange = Exchange()
    exchange.create_error = error_type("OrderNotFillable")(
        "no match; Authorization: Bearer sk-live-abcdef123456"
    )

    result = await market_request(CcxtPredictionGateway(exchange))

    assert result.venue_reason is not None
    assert "sk-live-abcdef123456" not in result.venue_reason


@pytest.mark.asyncio
async def test_not_fillable_without_detail_stays_stable() -> None:
    """An empty venue message must not produce a ragged error token."""
    exchange = Exchange()
    exchange.create_error = error_type("OrderNotFillable")("")

    result = await market_request(CcxtPredictionGateway(exchange))

    assert result.error == "order_not_fillable"
    assert result.venue_reason == "no venue detail"


# ---------------------------------------------------------------------------
# Unified-fallback envelope validation (A03)
#
# Without the raw cursor endpoints (degraded builds, public/paper doubles) the
# gateway falls back to the unified ``fetch_open_orders``/``fetch_my_trades``.
# A fallback answer whose shape is unrecognized must not be read as an empty
# history: absence proofs are built on these collections.
# ---------------------------------------------------------------------------


class UnifiedFallbackExchange:
    """Exchange with unified readers only, returning an injectable value."""

    def __init__(self, value: object) -> None:
        self._value = value

    async def fetch_open_orders(self, *_args):
        return self._value

    async def fetch_my_trades(self, *_args):
        return self._value


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        # Dict without any recognized collection key.
        {"error": "unavailable"},
        # A null answer is not an empty collection.
        None,
        # A bare string is not a collection.
        "unavailable",
    ],
)
async def test_unrecognized_unified_fallback_shape_is_invalid_history(value: object) -> None:
    with pytest.raises(VenueSchemaError):
        await CcxtPredictionGateway(UnifiedFallbackExchange(value)).list_open_orders()


@pytest.mark.asyncio
async def test_unified_fallback_empty_list_is_valid_empty_history() -> None:
    orders = await CcxtPredictionGateway(UnifiedFallbackExchange([])).list_open_orders()
    assert orders == []


@pytest.mark.asyncio
async def test_unified_fallback_keyed_envelope_is_valid_history() -> None:
    order = {
        "id": "order-1",
        "status": "LIVE",
        "side": "BUY",
        "price": "0.40",
        "original_size": "10",
        "size_matched": "0",
        "created_at": 1_750_000_000,
        "market": "market-1",
        "asset_id": "token-1",
    }

    orders = await CcxtPredictionGateway(
        UnifiedFallbackExchange({"data": [order], "next_cursor": ""})
    ).list_open_orders(market="market-1")

    assert [state.order_id for state in orders] == ["order-1"]
