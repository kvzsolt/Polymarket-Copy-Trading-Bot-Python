"""Contract tests for the official unified Polymarket SDK boundary."""

from __future__ import annotations

from decimal import Decimal
from datetime import datetime, timezone
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

from config.env import SignatureType
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
_TEST_CREDENTIAL_VALUES = ("k", "s", "p")


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
async def test_list_open_orders_consumes_sdk_paginator() -> None:
    created_at = datetime.now(tz=timezone.utc)

    async def pages():
        yield SimpleNamespace(items=(
            SimpleNamespace(
                id="order-open",
                status="LIVE",
                side="BUY",
                price=Decimal("0.42"),
                original_size=Decimal("10"),
                size_matched=Decimal("2"),
                created_at=created_at,
            ),
        ))

    secure = SimpleNamespace(list_open_orders=MagicMock(return_value=pages()))
    subject, _, _ = gateway(secure=secure)

    orders = await subject.list_open_orders()

    assert len(orders) == 1
    assert orders[0].order_id == "order-open"
    assert orders[0].filled_size == Decimal("2")
    assert orders[0].created_at == created_at
    secure.list_open_orders.assert_called_once_with()


@pytest.mark.asyncio
async def test_list_open_orders_passes_exact_sdk_filters() -> None:
    async def pages():
        if False:
            yield None

    secure = SimpleNamespace(list_open_orders=MagicMock(return_value=pages()))
    subject, _, _ = gateway(secure=secure)

    assert await subject.list_open_orders(order_id="order-1", market="market-1") == []
    secure.list_open_orders.assert_called_once_with(id="order-1", market="market-1")


@pytest.mark.asyncio
async def test_get_fills_attributes_maker_leg_and_deduplicates_pages() -> None:
    matched_at = datetime.now(tz=timezone.utc)
    maker = SimpleNamespace(
        order_id="maker-order",
        token_id="maker-token",
        side="SELL",
        matched_amount=Decimal("4"),
        price=Decimal("0.41"),
    )
    raw_trade = SimpleNamespace(
        id="trade-1",
        taker_order_id="taker-order",
        token_id="taker-token",
        side="BUY",
        size=Decimal("4"),
        price=Decimal("0.41"),
        trader_side="MAKER",
        maker_orders=(maker,),
        status="CONFIRMED",
        bucket_index=7,
        transaction_hash="0xsettled",
        matched_at=matched_at,
    )

    async def pages():
        yield SimpleNamespace(items=(raw_trade,))
        yield SimpleNamespace(items=(raw_trade,))

    secure = SimpleNamespace(list_account_trades=MagicMock(return_value=pages()))
    subject, _, _ = gateway(secure=secure)

    fills = await subject.get_fills(
        market="market-1",
        trade_id="trade-1",
        order_id="maker-order",
    )

    assert len(fills) == 1
    assert fills[0].order_id == "maker-order"
    assert fills[0].token_id == "maker-token"
    assert fills[0].size == Decimal("4")
    assert fills[0].status == "CONFIRMED"
    assert fills[0].trader_side == "MAKER"
    assert fills[0].bucket_index == 7
    secure.list_account_trades.assert_called_once_with(id="trade-1", market="market-1")


@pytest.mark.asyncio
async def test_open_order_pagination_uses_fresh_timeout_per_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    timeout_entries = 0

    class _Timeout:
        async def __aenter__(self):
            nonlocal timeout_entries
            timeout_entries += 1

        async def __aexit__(self, *_exc):
            return False

    async def pages():
        yield SimpleNamespace(items=())
        yield SimpleNamespace(items=())

    secure = SimpleNamespace(list_open_orders=MagicMock(return_value=pages()))
    subject, _, _ = gateway(secure=secure)
    monkeypatch.setattr(
        "integrations.polymarket.clob_v2_gateway.asyncio.timeout",
        lambda _seconds: _Timeout(),
    )

    assert await subject.list_open_orders() == []
    # Two data pages plus the terminal ``StopAsyncIteration`` probe. Each one
    # gets a new deadline instead of sharing a total timeout across pagination.
    assert timeout_entries == 3


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


@pytest.mark.parametrize(
    ("wallet_type", "expected_signature_type"),
    [
        ("EOA", SignatureType.EOA),
        ("POLY_PROXY", SignatureType.POLY_PROXY),
        ("GNOSIS_SAFE", SignatureType.GNOSIS_SAFE),
        ("DEPOSIT_WALLET", SignatureType.DEPOSIT_WALLET),
    ],
)
def test_client_maps_sdk_wallet_type_to_signature_type(
    wallet_type: str,
    expected_signature_type: SignatureType,
) -> None:
    public = SimpleNamespace()
    secure = SimpleNamespace(wallet_type=wallet_type)
    configured_hint = SimpleNamespace(signature_type=SignatureType.EOA)
    client = PolymarketClient(
        public,  # type: ignore[arg-type]
        secure_client=secure,  # type: ignore[arg-type]
        wallet_config=configured_hint,
    )

    assert client.wallet_type == wallet_type
    assert client.signature_type == expected_signature_type


def test_public_only_client_has_no_wallet_diagnostics() -> None:
    client = PolymarketClient(SimpleNamespace())  # type: ignore[arg-type]
    assert client.wallet_type is None
    assert client.signature_type is None


@pytest.mark.asyncio
async def test_gateway_compatibility_reports_deposit_wallet_signature_type() -> None:
    secure = SimpleNamespace(
        wallet="0x" + "b" * 40,
        wallet_type="DEPOSIT_WALLET",
    )
    subject, _, _ = gateway(secure=secure)

    report = await subject.check_compatibility()

    assert report["wallet_type"] == "DEPOSIT_WALLET"
    assert report["signature_type"] == 3


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
async def test_factory_paper_mode_never_bootstraps_secure_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    public = SimpleNamespace(close=AsyncMock())
    secure_create = AsyncMock()
    monkeypatch.setattr(
        "integrations.polymarket.sdk_client.AsyncPublicClient",
        MagicMock(return_value=public),
    )
    monkeypatch.setattr(
        "integrations.polymarket.sdk_client.AsyncSecureClient.create",
        secure_create,
    )
    config = SimpleNamespace(
        wallet=SimpleNamespace(
            private_key="0x" + "a" * 64,
            funder_address="0x" + "b" * 40,
        ),
        clob=SimpleNamespace(api_key="k", api_secret="s", api_passphrase="p"),
    )

    client = await create_polymarket_client(config=config, authenticated=False)

    assert client.secure_client is None
    secure_create.assert_not_awaited()


@pytest.mark.asyncio
async def test_factory_derives_credentials_and_validates_deposit_wallet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_key = "0x" + "a" * 64
    wallet = "0x" + "b" * 40
    public = SimpleNamespace(close=AsyncMock())
    secure = SimpleNamespace(
        credentials=ApiKeyCreds(
            key=_TEST_CREDENTIAL_VALUES[0],
            secret=_TEST_CREDENTIAL_VALUES[1],
            passphrase=_TEST_CREDENTIAL_VALUES[2],
        ),
        wallet=wallet,
        wallet_type="DEPOSIT_WALLET",
        close=AsyncMock(),
    )
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
        wallet=SimpleNamespace(private_key=private_key, funder_address=wallet),
    )
    client = await create_polymarket_client(config=config)

    secure_create.assert_awaited_once_with(private_key=private_key)
    assert client.secure_client is secure
    assert client.wallet_address == wallet
    assert client.wallet_type == "DEPOSIT_WALLET"
    assert client.signature_type == SignatureType.DEPOSIT_WALLET
    assert client.api_creds is secure.credentials


# ---------------------------------------------------------------------------
# Funder cross-check scoping
#
# The wallet_config handed to create_polymarket_client is that client's
# authoritative identity. Reaching past it to the global config.wallet funder
# cross-checked one user's SDK-derived Deposit Wallet against another user's
# configured funder.
# ---------------------------------------------------------------------------


def _sdk_doubles(monkeypatch: pytest.MonkeyPatch, resolved_wallet: str):
    public = SimpleNamespace(close=AsyncMock())
    secure = SimpleNamespace(
        credentials=ApiKeyCreds(
            key=_TEST_CREDENTIAL_VALUES[0],
            secret=_TEST_CREDENTIAL_VALUES[1],
            passphrase=_TEST_CREDENTIAL_VALUES[2],
        ),
        wallet=resolved_wallet,
        wallet_type="DEPOSIT_WALLET",
        close=AsyncMock(),
    )
    monkeypatch.setattr(
        "integrations.polymarket.sdk_client.AsyncPublicClient",
        MagicMock(return_value=public),
    )
    monkeypatch.setattr(
        "integrations.polymarket.sdk_client.AsyncSecureClient.create",
        AsyncMock(return_value=secure),
    )
    return public, secure


@pytest.mark.asyncio
async def test_multi_user_entry_without_a_funder_derives_its_own_deposit_wallet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A per-user entry may omit its funder and let its key derive the wallet.

    ``WalletConfig.funder_address`` defaults to "" for multi-user entries, and
    ``bind_resolved_deposit_wallet`` binds the SDK-resolved address afterwards.
    Falling back to the global funder here compared this user's wallet against
    a *different* user's configured funder and failed startup outright.
    """
    user_key = "0x" + "c" * 64
    user_wallet = "0x" + "d" * 40
    _sdk_doubles(monkeypatch, user_wallet)

    client = await create_polymarket_client(
        wallet_config=SimpleNamespace(private_key=user_key, funder_address=""),
        # A different user's funder — must never be applied to this client.
        config=SimpleNamespace(
            wallet=SimpleNamespace(private_key=user_key, funder_address="0x" + "e" * 40),
        ),
    )

    assert client.wallet_address == user_wallet


@pytest.mark.asyncio
async def test_single_user_configured_funder_mismatch_still_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preservation: a wallet_config carrying a funder is still cross-checked.

    ``load_runtime_users`` copies the configured funder into the single user's
    own WalletConfig, so that path keeps its guard against signing from a key
    that does not own the configured wallet.
    """
    _sdk_doubles(monkeypatch, "0x" + "d" * 40)

    with pytest.raises(ValueError, match="does not match the Deposit"):
        await create_polymarket_client(
            wallet_config=SimpleNamespace(
                private_key="0x" + "c" * 64,
                funder_address="0x" + "f" * 40,
            ),
            config=SimpleNamespace(wallet=SimpleNamespace(funder_address="0x" + "f" * 40)),
        )


@pytest.mark.asyncio
async def test_configured_funder_mismatch_still_fails_closed_without_wallet_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preservation: with no wallet_config, the global funder is authoritative."""
    _sdk_doubles(monkeypatch, "0x" + "d" * 40)

    with pytest.raises(ValueError, match="does not match the Deposit"):
        await create_polymarket_client(
            config=SimpleNamespace(
                wallet=SimpleNamespace(
                    private_key="0x" + "c" * 64,
                    funder_address="0x" + "f" * 40,
                ),
            ),
        )


@pytest.mark.asyncio
async def test_matching_wallet_config_funder_still_authenticates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Preservation: an agreeing configured funder is accepted."""
    wallet = "0x" + "d" * 40
    _sdk_doubles(monkeypatch, wallet)

    client = await create_polymarket_client(
        wallet_config=SimpleNamespace(
            private_key="0x" + "c" * 64,
            funder_address=wallet,
        ),
        config=SimpleNamespace(wallet=SimpleNamespace(funder_address="0x" + "e" * 40)),
    )

    assert client.wallet_address == wallet


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
