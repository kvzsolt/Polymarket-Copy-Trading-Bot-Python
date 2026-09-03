"""Signed-order metadata is refreshed on a clock, not on every submission.

Every copied FAK previously re-fetched market metadata immediately before
signing, duplicating the fetch the pipeline had just made. Two CLOB round trips
of ~0.4s each sat on the critical path of a race whose competitive window is
1-2 seconds. A short TTL keeps the "validate metadata before signing"
invariant — the data is still recent — while collapsing the duplicate fetch.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from integrations.polymarket.ccxt_prediction_gateway import CcxtPredictionGateway
from integrations.polymarket.models import MarketableOrderRequest

from tests.test_ccxt_prediction_gateway_contract import FakeExchange, TOKEN


class FakeClock:
    """Monotonic clock the test advances explicitly."""

    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _request() -> MarketableOrderRequest:
    return MarketableOrderRequest(
        token_id=TOKEN,
        side="BUY",
        size=Decimal("5.00"),
        price_limit=Decimal("0.41"),
        condition_id="condition",
        order_type="FAK",
    )


@pytest.mark.asyncio
async def test_submission_reuses_metadata_fetched_within_the_ttl() -> None:
    clock = FakeClock()
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(
        exchange, metadata_ttl_seconds=10.0, monotonic=clock
    )

    await gateway.get_execution_metadata(TOKEN, condition_id="condition")
    fetches_after_prefetch = exchange.metadata_market_calls

    clock.advance(2.0)
    result = await gateway.submit_marketable_order(_request())

    assert result.success is True
    assert exchange.metadata_market_calls == fetches_after_prefetch
    assert exchange.neg_risk_calls == fetches_after_prefetch


@pytest.mark.asyncio
async def test_submission_refetches_once_the_ttl_has_expired() -> None:
    clock = FakeClock()
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(
        exchange, metadata_ttl_seconds=10.0, monotonic=clock
    )

    await gateway.get_execution_metadata(TOKEN, condition_id="condition")
    fetches_after_prefetch = exchange.metadata_market_calls

    clock.advance(11.0)
    result = await gateway.submit_marketable_order(_request())

    assert result.success is True
    assert exchange.metadata_market_calls > fetches_after_prefetch


@pytest.mark.asyncio
async def test_a_zero_ttl_preserves_refetch_on_every_submission() -> None:
    """The old always-refresh behaviour stays reachable by configuration."""
    clock = FakeClock()
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(
        exchange, metadata_ttl_seconds=0.0, monotonic=clock
    )

    await gateway.get_execution_metadata(TOKEN, condition_id="condition")
    fetches_after_prefetch = exchange.metadata_market_calls

    await gateway.submit_marketable_order(_request())

    assert exchange.metadata_market_calls > fetches_after_prefetch


@pytest.mark.asyncio
async def test_metadata_is_still_validated_when_served_from_cache() -> None:
    """A cached hit must not bypass the signed-order metadata contract."""
    clock = FakeClock()
    exchange = FakeExchange()
    gateway = CcxtPredictionGateway(
        exchange, metadata_ttl_seconds=10.0, monotonic=clock
    )

    metadata = await gateway.get_execution_metadata(TOKEN, condition_id="condition")
    clock.advance(1.0)
    reused = await gateway.get_execution_metadata(TOKEN, condition_id="condition")

    assert reused == metadata
    assert reused.tick_size == Decimal("0.0025")
    assert reused.min_order_size == Decimal("5")
    assert reused.neg_risk is True


def test_metadata_ttl_defaults_to_refetching_before_every_signed_order() -> None:
    """Fresh metadata before signing is the default; the TTL is opt-in.

    Polymarket moves tick size on a live market, and a stale tick signs a price
    off the grid, so trading latency for staleness has to be a deliberate
    operator choice rather than a silent default.
    """
    gateway = CcxtPredictionGateway(FakeExchange())

    assert gateway._metadata_ttl_seconds == 0.0
    assert gateway._metadata_is_fresh(TOKEN) is False


def test_venue_metadata_ttl_is_configurable_and_defaults_off() -> None:
    from config.app_config import ExecutionConfig

    assert ExecutionConfig().venue_metadata_ttl_seconds == 0.0


@pytest.mark.parametrize("value", [-1.0, -0.001])
def test_negative_venue_metadata_ttl_is_rejected(value: float) -> None:
    from config.app_config import EnvValidationError, load_config

    overrides = {
        "database": {"dsn": "postgresql://test:test@localhost/test"},
        "wallet": {"funder_address": "0x" + "1" * 40},
        "polymarket": {"rpc_url": "https://rpc.invalid"},
        "execution": {"venue_metadata_ttl_seconds": value},
    }

    with pytest.raises(EnvValidationError, match="VENUE_METADATA_TTL_SECONDS"):
        load_config(overrides)
