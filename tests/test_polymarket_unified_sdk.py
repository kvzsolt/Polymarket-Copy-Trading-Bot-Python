"""Contract tests for the official unified Polymarket SDK boundary."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from polymarket import (
    AcceptedOrder,
    ApiKeyCreds,
    OrderBook as SdkOrderBook,
    RejectedOrder,
    RequestRejectedError,
    TransportError,
)
from polymarket.models.clob.cancel import CancelOrdersResponse

from integrations.polymarket.clob_v2_gateway import PolymarketGateway
from integrations.polymarket.errors import (
    VenueInvalidOrderError,
    VenueOrderNotFoundError,
    VenueOrderbookNotFoundError,
)
from integrations.polymarket.models import LimitOrderRequest, MarketableOrderRequest
from integrations.polymarket.sdk_client import (
    PolymarketClient,
    create_polymarket_client,
)
from utils.createClobClient import quantize_to_tick


TOKEN_ID = "123"


def sdk_book(
    *,
    tick_size: str = "0.005",
    min_order_size: str = "5",
    neg_risk: bool = False,
) -> SdkOrderBook:
    return SdkOrderBook.model_validate(
        {
            "market": "0xmarket",
            "asset_id": TOKEN_ID,
            "timestamp": "1750000000000",
            "bids": [{"price": "0.40", "size": "20"}],
            "asks": [{"price": "0.42", "size": "10"}],
            "min_order_size": min_order_size,
            "tick_size": tick_size,
            "neg_risk": neg_risk,
            "last_trade_price": "0.41",
            "hash": "book-hash",
        }
    )


def gateway(
    *,
    book: object | None = None,
    secure: object | None = None,
) -> tuple[PolymarketGateway, object, object | None]:
    public = SimpleNamespace(
        get_order_book=AsyncMock(return_value=book or sdk_book()),
        environment=SimpleNamespace(
            chain_id=137,
            standard_exchange="0xstandard",
            neg_risk_exchange="0xnegative",
            collateral_token="0xcollateral",
            conditional_tokens="0xconditional",
        ),
        close=AsyncMock(),
    )
    client = PolymarketClient(public, secure_client=secure)  # type: ignore[arg-type]
    return PolymarketGateway(client), public, secure


@pytest.mark.parametrize("tick", ["0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001"])
@pytest.mark.asyncio
async def test_orderbook_accepts_every_supported_tick(tick: str) -> None:
    subject, _, _ = gateway(book=sdk_book(tick_size=tick, neg_risk=True))
    book = await subject.get_orderbook(TOKEN_ID)
    assert book.tick_size == Decimal(tick)
    assert book.min_order_size == Decimal("5")
    assert book.neg_risk is True
    assert book.best_bid == Decimal("0.40")
    assert book.best_ask == Decimal("0.42")


@pytest.mark.asyncio
async def test_missing_neg_risk_fails_closed() -> None:
    malformed = SimpleNamespace(
        token_id=TOKEN_ID,
        tick_size=Decimal("0.01"),
        min_order_size=Decimal("5"),
        neg_risk=None,
        bids=(),
        asks=(),
        timestamp=None,
    )
    subject, _, _ = gateway(book=malformed)
    with pytest.raises(VenueInvalidOrderError, match="neg_risk"):
        await subject.get_orderbook(TOKEN_ID)


@pytest.mark.asyncio
async def test_non_positive_min_order_size_fails_closed() -> None:
    subject, _, _ = gateway(book=sdk_book(min_order_size="0"))
    with pytest.raises(VenueInvalidOrderError, match="execution metadata"):
        await subject.get_orderbook(TOKEN_ID)


@pytest.mark.asyncio
async def test_orderbook_404_has_distinct_terminal_error() -> None:
    subject, public, _ = gateway()
    public.get_order_book.side_effect = RequestRejectedError("missing", status=404)
    with pytest.raises(VenueOrderbookNotFoundError):
        await subject.get_orderbook(TOKEN_ID)


@pytest.mark.asyncio
async def test_limit_order_maps_typed_acceptance() -> None:
    accepted = AcceptedOrder(
        order_id="order-1",
        status="live",
        making_amount=Decimal("5"),
        taking_amount=Decimal("2"),
        trade_ids=(),
        transactions_hashes=(),
    )
    secure = SimpleNamespace(place_limit_order=AsyncMock(return_value=accepted))
    subject, _, _ = gateway(secure=secure)
    result = await subject.submit_limit_order(
        LimitOrderRequest(
            token_id=TOKEN_ID,
            side="BUY",
            price=Decimal("0.40"),
            size=Decimal("5"),
            condition_id="condition",
        )
    )
    assert result.success is True
    assert result.order_id == "order-1"
    secure.place_limit_order.assert_awaited_once_with(
        token_id=TOKEN_ID,
        side="BUY",
        price=Decimal("0.40"),
        size=Decimal("5"),
    )


@pytest.mark.asyncio
async def test_limit_order_requires_condition_before_network() -> None:
    secure = SimpleNamespace(place_limit_order=AsyncMock())
    subject, _, _ = gateway(secure=secure)
    result = await subject.submit_limit_order(
        LimitOrderRequest(
            token_id=TOKEN_ID,
            side="BUY",
            price=Decimal("0.40"),
            size=Decimal("5"),
        )
    )
    assert result.success is False
    assert result.non_retryable is True
    secure.place_limit_order.assert_not_awaited()


@pytest.mark.asyncio
async def test_buy_fok_is_forwarded_without_float_conversion() -> None:
    accepted = AcceptedOrder(
        order_id="order-2",
        status="matched",
        making_amount=Decimal("5"),
        taking_amount=Decimal("10"),
        trade_ids=("trade-1",),
        transactions_hashes=(),
    )
    secure = SimpleNamespace(place_market_order=AsyncMock(return_value=accepted))
    subject, _, _ = gateway(secure=secure)
    result = await subject.submit_marketable_order(
        MarketableOrderRequest(
            token_id=TOKEN_ID,
            side="BUY",
            size=Decimal("5.125"),
            price_limit=Decimal("0.42"),
            tick_size=Decimal("0.005"),
            neg_risk=False,
            order_type="FOK",
        )
    )
    assert result.success is True
    secure.place_market_order.assert_awaited_once_with(
        token_id=TOKEN_ID,
        side="BUY",
        amount=Decimal("5.125"),
        max_price=Decimal("0.42"),
        order_type="FOK",
    )


@pytest.mark.asyncio
async def test_sell_fak_uses_shares_and_minimum_price() -> None:
    accepted = AcceptedOrder(
        order_id="order-3",
        status="matched",
        making_amount=Decimal("4"),
        taking_amount=Decimal("2"),
        trade_ids=(),
        transactions_hashes=(),
    )
    secure = SimpleNamespace(place_market_order=AsyncMock(return_value=accepted))
    subject, _, _ = gateway(secure=secure)
    await subject.submit_marketable_order(
        MarketableOrderRequest(
            token_id=TOKEN_ID,
            side="SELL",
            size=Decimal("4"),
            price_limit=Decimal("0.39"),
            tick_size=Decimal("0.01"),
            neg_risk=True,
            order_type="FAK",
        )
    )
    secure.place_market_order.assert_awaited_once_with(
        token_id=TOKEN_ID,
        side="SELL",
        shares=Decimal("4"),
        min_price=Decimal("0.39"),
        order_type="FAK",
    )


@pytest.mark.asyncio
async def test_transport_failure_after_post_is_ambiguous() -> None:
    secure = SimpleNamespace(
        place_market_order=AsyncMock(side_effect=TransportError("connection reset"))
    )
    subject, _, _ = gateway(secure=secure)
    result = await subject.submit_marketable_order(
        MarketableOrderRequest(
            token_id=TOKEN_ID,
            side="BUY",
            size=Decimal("5"),
            price_limit=Decimal("0.42"),
            tick_size=Decimal("0.01"),
            neg_risk=False,
            order_type="FOK",
        )
    )
    assert result.success is False
    assert result.ambiguous_post_failure is True


@pytest.mark.asyncio
async def test_typed_rejection_is_terminal() -> None:
    secure = SimpleNamespace(
        place_market_order=AsyncMock(
            return_value=RejectedOrder(code="fok_not_filled", message="not filled")
        )
    )
    subject, _, _ = gateway(secure=secure)
    result = await subject.submit_marketable_order(
        MarketableOrderRequest(
            token_id=TOKEN_ID,
            side="BUY",
            size=Decimal("5"),
            price_limit=Decimal("0.42"),
            tick_size=Decimal("0.01"),
            neg_risk=False,
            order_type="FOK",
        )
    )
    assert result.status == "cancelled"
    assert result.non_retryable is True
    assert result.ambiguous_post_failure is False


@pytest.mark.asyncio
async def test_cancel_maps_success_and_404_idempotently() -> None:
    secure = SimpleNamespace(
        cancel_order=AsyncMock(
            return_value=CancelOrdersResponse(canceled=("order-1",), not_canceled={})
        )
    )
    subject, _, _ = gateway(secure=secure)
    assert (await subject.cancel_order("order-1")).success is True

    secure.cancel_order.side_effect = RequestRejectedError("gone", status=404)
    gone = await subject.cancel_order("order-1")
    assert gone.success is True
    assert gone.status == "already_gone"


@pytest.mark.asyncio
async def test_get_order_404_is_not_venue_outage() -> None:
    secure = SimpleNamespace(
        get_order=AsyncMock(side_effect=RequestRejectedError("missing", status=404))
    )
    subject, _, _ = gateway(secure=secure)
    with pytest.raises(VenueOrderNotFoundError):
        await subject.get_order("missing")


@pytest.mark.asyncio
async def test_client_closes_both_transports_once() -> None:
    public = SimpleNamespace(close=AsyncMock())
    secure = SimpleNamespace(close=AsyncMock(), credentials=None)
    client = PolymarketClient(public, secure_client=secure)  # type: ignore[arg-type]
    await client.close()
    await client.close()
    public.close.assert_awaited_once()
    secure.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_client_close_attempts_public_when_secure_close_fails() -> None:
    public = SimpleNamespace(close=AsyncMock())
    secure = SimpleNamespace(
        close=AsyncMock(side_effect=RuntimeError("secure close failed")),
        credentials=None,
    )
    client = PolymarketClient(public, secure_client=secure)  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="secure close failed"):
        await client.close()

    secure.close.assert_awaited_once()
    public.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_factory_builds_public_only_without_private_key(monkeypatch: pytest.MonkeyPatch) -> None:
    public = SimpleNamespace(close=AsyncMock())
    public_factory = MagicMock(return_value=public)
    secure_create = AsyncMock()
    monkeypatch.setattr(
        "integrations.polymarket.sdk_client.AsyncPublicClient",
        public_factory,
    )
    monkeypatch.setattr(
        "integrations.polymarket.sdk_client.AsyncSecureClient.create",
        secure_create,
    )
    client = await create_polymarket_client(config=SimpleNamespace())
    assert client.secure_client is None
    secure_create.assert_not_awaited()


@pytest.mark.asyncio
async def test_factory_passes_complete_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    public = SimpleNamespace(close=AsyncMock())
    secure = SimpleNamespace(credentials=ApiKeyCreds(key="k", secret="s", passphrase="p"))
    secure_create = AsyncMock(return_value=secure)
    monkeypatch.setattr(
        "integrations.polymarket.sdk_client.AsyncPublicClient",
        MagicMock(return_value=public),
    )
    monkeypatch.setattr(
        "integrations.polymarket.sdk_client.AsyncSecureClient.create",
        secure_create,
    )
    config = SimpleNamespace(
        wallet=SimpleNamespace(private_key="0x" + "a" * 64, funder_address="0x" + "b" * 40),
        clob=SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p"),
    )
    await create_polymarket_client(config=config)
    kwargs = secure_create.await_args.kwargs
    assert kwargs["wallet"] == "0x" + "b" * 40
    assert kwargs["credentials"].key == "k"


@pytest.mark.parametrize(
    ("side", "expected"),
    [
        ("BUY", Decimal("0.56")),
        ("SELL", Decimal("0.55")),
    ],
)
def test_tick_quantization_is_side_conservative(side: str, expected: Decimal) -> None:
    assert quantize_to_tick(Decimal("0.551"), Decimal("0.01"), side=side) == expected


def test_production_does_not_import_old_or_private_sdk() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    violations: list[str] = []
    for directory in ("integrations", "clients", "services", "orchestrators", "utils"):
        for path in (root / directory).rglob("*.py"):
            content = path.read_text(encoding="utf-8")
            if "py_clob_client_v2" in content or "polymarket._internal" in content:
                violations.append(str(path.relative_to(root)))
    assert violations == []
