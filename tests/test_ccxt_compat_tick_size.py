"""Arbitrary tick-size compatibility contracts for the CCXT-only venue.

These tests pin the venue-neutral tick-size value object, side-aware
quantization by integer tick multiples (never decimal places, never a silent
``0.01`` default), and the fail-closed two-source tick-size resolver.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal

import pytest

from pmcopy.integrations.polymarket.ccxt_compat.tick_size import (
    InvalidTickSize,
    PolymarketTickSizeResolver,
    PublicClobTickSizeSource,
    TickSize,
    TickRounding,
    TickSizeResolver,
    TickSizeUnavailable,
    quantize_price_to_tick,
)


@pytest.mark.parametrize(
    ("side", "price", "tick_size", "expected"),
    [
        ("BUY", Decimal("0.411"), Decimal("0.0025"), Decimal("0.4100")),
        ("SELL", Decimal("0.411"), Decimal("0.0025"), Decimal("0.4125")),
        ("BUY", Decimal("0.413"), Decimal("0.005"), Decimal("0.410")),
        ("SELL", Decimal("0.413"), Decimal("0.005"), Decimal("0.415")),
        ("BUY", Decimal("0.9876"), Decimal("0.0001"), Decimal("0.9876")),
        ("BUY", Decimal("0.37"), Decimal("0.1"), Decimal("0.3")),
        ("SELL", Decimal("0.31"), Decimal("0.1"), Decimal("0.4")),
        ("BUY", Decimal("0.123456"), Decimal("0.001"), Decimal("0.123")),
        ("SELL", Decimal("0.123456"), Decimal("0.001"), Decimal("0.124")),
    ],
)
def test_quantize_price_to_arbitrary_tick(
    side: str,
    price: Decimal,
    tick_size: Decimal,
    expected: Decimal,
) -> None:
    assert (
        quantize_price_to_tick(
            price=price,
            tick_size=tick_size,
            side=side,  # type: ignore[arg-type]
        )
        == expected
    )


@pytest.mark.parametrize(
    "tick_size",
    [Decimal("0"), Decimal("-0.01"), Decimal("NaN"), Decimal("Infinity")],
)
def test_invalid_tick_size_is_rejected(tick_size: Decimal) -> None:
    with pytest.raises(InvalidTickSize):
        quantize_price_to_tick(
            price=Decimal("0.5"),
            tick_size=tick_size,
            side="BUY",
        )


@pytest.mark.parametrize("price", [Decimal("NaN"), Decimal("-Infinity")])
def test_non_finite_price_is_rejected(price: Decimal) -> None:
    with pytest.raises(ValueError):
        quantize_price_to_tick(
            price=price,
            tick_size=Decimal("0.01"),
            side="BUY",
        )


def test_unknown_side_is_rejected() -> None:
    with pytest.raises(ValueError, match="side"):
        quantize_price_to_tick(
            price=Decimal("0.5"),
            tick_size=Decimal("0.01"),
            side="LONG",  # type: ignore[arg-type]
        )


def test_buy_never_exceeds_original_limit() -> None:
    price = Decimal("0.7319")
    for tick in ("0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001"):
        result = quantize_price_to_tick(price=price, tick_size=Decimal(tick), side="BUY")
        assert result <= price


def test_sell_never_falls_below_original_limit() -> None:
    price = Decimal("0.7319")
    for tick in ("0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001"):
        result = quantize_price_to_tick(price=price, tick_size=Decimal(tick), side="SELL")
        assert result >= price


def test_quantization_is_by_tick_multiples_not_decimal_places() -> None:
    # 0.0025 is not expressible as a fixed number of decimal places relative to
    # the price; the result must be an exact integer multiple of the tick.
    tick = Decimal("0.0025")
    result = quantize_price_to_tick(price=Decimal("0.411"), tick_size=tick, side="SELL")
    assert (result / tick) == (result / tick).to_integral_value()
    assert result == Decimal("0.4125")


def test_tick_size_value_object_validates() -> None:
    assert TickSize(Decimal("0.0025")).value == Decimal("0.0025")
    with pytest.raises(ValueError):
        TickSize(Decimal("0"))
    with pytest.raises(ValueError):
        TickSize(Decimal("-0.01"))
    with pytest.raises(ValueError):
        TickSize(Decimal("NaN"))


@dataclass
class _FakeCcxtSource:
    tick: Decimal | None
    calls: int = 0

    async def get_tick_size(self, token_id: str) -> Decimal | None:
        self.calls += 1
        return self.tick


@dataclass
class _FakeMetadataSource:
    tick: Decimal | None
    calls: int = 0

    async def get_tick_size(self, token_id: str) -> Decimal | None:
        self.calls += 1
        return self.tick


def test_resolver_prefers_ccxt_metadata_then_falls_back() -> None:
    ccxt = _FakeCcxtSource(Decimal("0.0025"))
    metadata = _FakeMetadataSource(Decimal("0.01"))

    resolver = PolymarketTickSizeResolver(
        ccxt_client=ccxt,
        market_metadata_client=metadata,
    )

    assert asyncio.run(resolver.resolve("token-a")) == Decimal("0.0025")
    assert metadata.calls == 0


def test_resolver_falls_back_to_market_metadata_client() -> None:
    ccxt = _FakeCcxtSource(None)
    metadata = _FakeMetadataSource(Decimal("0.005"))

    resolver = PolymarketTickSizeResolver(
        ccxt_client=ccxt,
        market_metadata_client=metadata,
    )

    assert asyncio.run(resolver.resolve("token-a")) == Decimal("0.005")


def test_resolver_fails_closed_when_no_source_has_a_tick() -> None:
    resolver = PolymarketTickSizeResolver(
        ccxt_client=_FakeCcxtSource(None),
        market_metadata_client=_FakeMetadataSource(None),
    )

    with pytest.raises(TickSizeUnavailable, match="token-a"):
        asyncio.run(resolver.resolve("token-a"))


def test_resolver_without_any_source_fails_closed() -> None:
    with pytest.raises(TickSizeUnavailable):
        asyncio.run(PolymarketTickSizeResolver().resolve("token-a"))


@pytest.mark.parametrize("tick", [Decimal("0"), Decimal("-0.01"), Decimal("NaN")])
def test_resolver_rejects_invalid_tick_from_any_source(tick: Decimal) -> None:
    with pytest.raises(InvalidTickSize):
        asyncio.run(
            PolymarketTickSizeResolver(
                ccxt_client=_FakeCcxtSource(tick),
                market_metadata_client=_FakeMetadataSource(Decimal("0.01")),
            ).resolve("token-a")
        )

    with pytest.raises(InvalidTickSize):
        asyncio.run(
            PolymarketTickSizeResolver(
                ccxt_client=_FakeCcxtSource(None),
                market_metadata_client=_FakeMetadataSource(tick),
            ).resolve("token-a")
        )


def test_resolver_caches_within_ttl() -> None:
    ccxt = _FakeCcxtSource(Decimal("0.01"))

    resolver = PolymarketTickSizeResolver(ccxt_client=ccxt, ttl_seconds=60.0)

    assert asyncio.run(resolver.resolve("token-a")) == Decimal("0.01")
    assert asyncio.run(resolver.resolve("token-a")) == Decimal("0.01")
    assert ccxt.calls == 1


def test_resolver_cache_expires_after_ttl() -> None:
    ccxt = _FakeCcxtSource(Decimal("0.01"))
    now = {"value": 0.0}

    class Clock:
        def __call__(self) -> float:
            return now["value"]

    resolver = PolymarketTickSizeResolver(ccxt_client=ccxt, ttl_seconds=60.0, clock=Clock())

    asyncio.run(resolver.resolve("token-a"))
    now["value"] = 61.0
    asyncio.run(resolver.resolve("token-a"))
    assert ccxt.calls == 2


def test_resolver_invalidation_forces_a_fresh_resolution() -> None:
    ccxt = _FakeCcxtSource(Decimal("0.01"))
    resolver = PolymarketTickSizeResolver(ccxt_client=ccxt)

    asyncio.run(resolver.resolve("token-a"))
    resolver.invalidate("token-a")
    asyncio.run(resolver.resolve("token-a"))
    assert ccxt.calls == 2


def test_resolver_observes_authoritative_tick_changes() -> None:
    ccxt = _FakeCcxtSource(Decimal("0.01"))
    resolver = PolymarketTickSizeResolver(ccxt_client=ccxt)

    assert asyncio.run(resolver.resolve("token-a")) == Decimal("0.01")

    # A fresh venue book observed a tick-size change while the market is live.
    asyncio.run(resolver.note_observed_tick("token-a", Decimal("0.0025")))
    assert asyncio.run(resolver.resolve("token-a")) == Decimal("0.0025")
    assert ccxt.calls == 1


def test_resolver_observation_validates_the_tick() -> None:
    resolver = PolymarketTickSizeResolver(ccxt_client=_FakeCcxtSource(None))

    with pytest.raises(InvalidTickSize):
        asyncio.run(resolver.note_observed_tick("token-a", Decimal("0")))


def test_resolver_satisfies_the_protocol() -> None:
    resolver: TickSizeResolver = PolymarketTickSizeResolver(
        ccxt_client=_FakeCcxtSource(Decimal("0.01"))
    )
    assert asyncio.run(resolver.resolve("token-a")) == Decimal("0.01")


def test_public_clob_source_reads_book_tick_size() -> None:
    async def fetch_json(url: str) -> object:
        assert "token-a" in url
        return {"tick_size": "0.0025", "min_order_size": "5"}

    source = PublicClobTickSizeSource(fetch_json=fetch_json)

    assert asyncio.run(source.get_tick_size("token-a")) == Decimal("0.0025")


def test_public_clob_source_returns_none_without_a_default() -> None:
    async def fetch_json(url: str) -> object:
        return {"min_order_size": "5"}

    source = PublicClobTickSizeSource(fetch_json=fetch_json)

    assert asyncio.run(source.get_tick_size("token-a")) is None


def test_public_clob_source_swallows_transport_errors_and_returns_none() -> None:
    async def fetch_json(url: str) -> object:
        raise TimeoutError("clob unreachable")

    source = PublicClobTickSizeSource(fetch_json=fetch_json)

    assert asyncio.run(source.get_tick_size("token-a")) is None


class TestMarketableTickRounding:
    """A marketable limit must round toward crossing, not away from it.

    The default ``CONSERVATIVE`` mode protects a caller's cap, which is right
    for a resting limit but wrong for an order whose whole purpose is to cross:
    flooring a 0.6846 BUY to 0.68 shaved the band and left the order below a
    0.70 ask, which the venue killed as unfillable.
    """

    def test_marketable_buy_rounds_up_to_the_tick(self) -> None:
        result = quantize_price_to_tick(
            price=Decimal("0.6846"),
            tick_size=Decimal("0.01"),
            side="BUY",
            rounding=TickRounding.MARKETABLE,
        )
        assert result == Decimal("0.69")

    def test_marketable_sell_rounds_down_to_the_tick(self) -> None:
        result = quantize_price_to_tick(
            price=Decimal("0.5225"),
            tick_size=Decimal("0.01"),
            side="SELL",
            rounding=TickRounding.MARKETABLE,
        )
        assert result == Decimal("0.52")

    def test_conservative_remains_the_default(self) -> None:
        """Existing callers keep cap-preserving behaviour without opting in."""
        assert quantize_price_to_tick(
            price=Decimal("0.6846"), tick_size=Decimal("0.01"), side="BUY"
        ) == Decimal("0.68")
        assert quantize_price_to_tick(
            price=Decimal("0.5225"), tick_size=Decimal("0.01"), side="SELL"
        ) == Decimal("0.53")

    @pytest.mark.parametrize("tick", ["0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001"])
    def test_marketable_buy_never_falls_below_the_requested_price(self, tick: str) -> None:
        price = Decimal("0.7319")
        result = quantize_price_to_tick(
            price=price,
            tick_size=Decimal(tick),
            side="BUY",
            rounding=TickRounding.MARKETABLE,
        )
        assert result >= price

    @pytest.mark.parametrize("tick", ["0.1", "0.01", "0.005", "0.0025", "0.001", "0.0001"])
    def test_marketable_sell_never_rises_above_the_requested_price(self, tick: str) -> None:
        price = Decimal("0.7319")
        result = quantize_price_to_tick(
            price=price,
            tick_size=Decimal(tick),
            side="SELL",
            rounding=TickRounding.MARKETABLE,
        )
        assert result <= price

    def test_marketable_rounding_stays_on_exact_tick_multiples(self) -> None:
        tick = Decimal("0.0025")
        result = quantize_price_to_tick(
            price=Decimal("0.411"),
            tick_size=tick,
            side="BUY",
            rounding=TickRounding.MARKETABLE,
        )
        assert result == Decimal("0.4125")
        assert (result / tick) == (result / tick).to_integral_value()

    def test_an_already_on_tick_price_is_unchanged_in_either_mode(self) -> None:
        """Idempotence matters: the gateway re-quantizes what it was handed."""
        for mode in (TickRounding.CONSERVATIVE, TickRounding.MARKETABLE):
            assert quantize_price_to_tick(
                price=Decimal("0.69"),
                tick_size=Decimal("0.01"),
                side="BUY",
                rounding=mode,
            ) == Decimal("0.69")
