"""Risk-monitor handling of the Polymarket ``/price`` 404 terminal signal.

Per the Polymarket OpenAPI spec, ``GET /price`` returning ``404 not_found``
means the token has no orderbook — the market resolved / archived and the
token is retired. ``utils.createClobClient.get_price`` surfaces this via the
``PRICE_TOKEN_NOT_FOUND`` sentinel so ``services.risk.risk_monitor`` can:

  1. Mark the token resolved in ``MarketStateStore`` (no further polling
     anywhere — refresh + stop-loss both short-circuit via
     ``_is_token_resolved``).
  2. Leave the circuit breaker untouched (404 is market state, not venue
     health; only 5xx / timeouts / malformed payloads trip the breaker).

Both behaviours together kill the boot-time loop where a retired token
floods ``venue_circuit_breaker_opened`` events.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from cache.market_state_store import MarketStateStore
from services.risk.risk_monitor import (
    RiskPriceCache,
    VenueCircuitBreaker,
    _active_sell_reserved_size,
    _fetch_and_store_price,
    _get_context_cached_price,
    _refresh_context_price_cache,
    _should_log_active_reservation_skip,
)
from utils.createClobClient import PRICE_TOKEN_NOT_FOUND


class _NotFoundPriceClient:
    """Mimics ``createClobClient.get_price`` returning the 404 sentinel."""

    def __init__(self):
        self.calls = 0

    async def get_price(self, _token_id: str, _side: str):
        self.calls += 1
        return PRICE_TOKEN_NOT_FOUND


VALID_TOKEN_ID = "71321045679252212594626385532706912750332728571942532223535764789812338637396"


@pytest.mark.asyncio
async def test_404_sentinel_marks_resolved_and_skips_breaker():
    cache = RiskPriceCache()
    breaker = VenueCircuitBreaker(threshold=2, cooldown=60)
    market_store = MagicMock()
    market_store.mark_resolved = MagicMock()
    client = _NotFoundPriceClient()

    await _fetch_and_store_price(
        client,
        token_id="0xdeadtoken",
        cache=cache,
        semaphore=asyncio.Semaphore(1),
        circuit_breaker=breaker,
        market_store=market_store,
        condition_id="0xcond123",
    )

    # MarketStateStore was instructed to mark the token resolved with the
    # condition_id the caller threaded through.
    market_store.mark_resolved.assert_called_once_with("0xdeadtoken", "0xcond123")

    # Breaker untouched — sentinel is a market signal, not venue health.
    assert breaker._consecutive_failures == 0
    assert breaker._opened_at is None
    assert breaker.state == "closed"

    # Cache is NOT populated — there's no quote to remember.
    assert cache.get_fresh("0xdeadtoken", stale_after=10) is None


@pytest.mark.asyncio
async def test_404_sentinel_without_market_store_still_skips_breaker():
    """When no market_store is wired (test/paper fixture), the 404 sentinel
    must still bypass the breaker so dead tokens don't flap it."""
    cache = RiskPriceCache()
    breaker = VenueCircuitBreaker(threshold=1, cooldown=60)
    client = _NotFoundPriceClient()

    await _fetch_and_store_price(
        client,
        token_id="0xdeadtoken",
        cache=cache,
        semaphore=asyncio.Semaphore(1),
        circuit_breaker=breaker,
        market_store=None,
        condition_id=None,
    )

    assert breaker._consecutive_failures == 0
    assert breaker.state == "closed"


@pytest.mark.asyncio
async def test_none_response_still_trips_breaker():
    """Sanity guard: a generic ``None`` (5xx / timeout) must continue to
    increment failures so the breaker correctly opens on real venue issues."""
    cache = RiskPriceCache()
    breaker = VenueCircuitBreaker(threshold=2, cooldown=60)

    class _NullPriceClient:
        async def get_price(self, *_a):
            return None

    for _ in range(2):
        await _fetch_and_store_price(
            _NullPriceClient(),
            token_id="0xunhealthy",
            cache=cache,
            semaphore=asyncio.Semaphore(1),
            circuit_breaker=breaker,
        )
    assert breaker.state == "open"


@pytest.mark.asyncio
async def test_refresh_context_price_cache_skips_fresh_quotes():
    cache = RiskPriceCache()
    cache.set(VALID_TOKEN_ID, price=Decimal("0.31"))

    client = MagicMock()
    client.get_price = AsyncMock(return_value={"price": "0.31"})
    positions_repo = MagicMock()
    positions_repo.list_open_positions = AsyncMock(
        return_value=[
            SimpleNamespace(
                token_id=VALID_TOKEN_ID,
                condition_id="0xcond",
            )
        ]
    )
    context = SimpleNamespace(
        user_id="u1",
        client=client,
        positions_repo=positions_repo,
        extra={"risk_price_cache": cache},
        market_data_runtime=None,
    )

    await _refresh_context_price_cache(context, refresh_interval=0.5)

    client.get_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_context_price_cache_prefers_subscribed_market_wss_quote():
    cache = RiskPriceCache()
    store = MarketStateStore()
    store.set_quote(
        VALID_TOKEN_ID,
        best_bid=Decimal("0.31"),
        best_ask=Decimal("0.33"),
        source="wss",
    )

    client = MagicMock()
    client.get_price = AsyncMock(return_value={"price": "0.31"})
    positions_repo = MagicMock()
    positions_repo.list_open_positions = AsyncMock(
        return_value=[
            SimpleNamespace(
                token_id=VALID_TOKEN_ID,
                condition_id="0xcond",
            )
        ]
    )
    market_runtime = SimpleNamespace(
        market_store=store,
        _subscribed_tokens={VALID_TOKEN_ID},
        is_wss_down=MagicMock(return_value=False),
    )
    context = SimpleNamespace(
        user_id="u1",
        client=client,
        positions_repo=positions_repo,
        extra={"risk_price_cache": cache},
        market_data_runtime=market_runtime,
        config=SimpleNamespace(
            market_data=SimpleNamespace(wss_connection_max_silence_seconds=20.0)
        ),
    )

    await _refresh_context_price_cache(context, refresh_interval=0.5)

    client.get_price.assert_not_awaited()
    entry = cache.get_fresh(VALID_TOKEN_ID, stale_after=1.5)
    assert entry is not None
    assert entry.price == Decimal("0.31")


@pytest.mark.asyncio
async def test_get_context_cached_price_prefers_subscribed_market_wss_quote():
    store = MarketStateStore()
    store.set_quote(
        VALID_TOKEN_ID,
        best_bid=Decimal("0.30"),
        best_ask=Decimal("0.32"),
        source="wss",
    )

    client = MagicMock()
    client.get_price = AsyncMock(return_value={"price": "0.31"})
    positions_repo = MagicMock()
    positions_repo.list_open_positions = AsyncMock(return_value=[])
    market_runtime = SimpleNamespace(
        market_store=store,
        _subscribed_tokens={VALID_TOKEN_ID},
        is_wss_down=MagicMock(return_value=False),
    )
    context = SimpleNamespace(
        user_id="u1",
        client=client,
        positions_repo=positions_repo,
        extra={},
        market_data_runtime=market_runtime,
        config=SimpleNamespace(
            market_data=SimpleNamespace(wss_connection_max_silence_seconds=20.0)
        ),
    )

    price = await _get_context_cached_price(
        context,
        VALID_TOKEN_ID,
        refresh_interval=0.5,
    )

    assert price == Decimal("0.30")
    client.get_price.assert_not_awaited()


@pytest.mark.asyncio
async def test_active_sell_reserved_size_sums_only_active_sell_scope():
    repo = MagicMock()
    repo.list_active_for_scope = AsyncMock(
        return_value=[
            SimpleNamespace(side="SELL", status="active", reserved_size=Decimal("5")),
            SimpleNamespace(side="BUY", status="active", reserved_size=Decimal("99")),
            SimpleNamespace(side="SELL", status="released", reserved_size=Decimal("7")),
        ]
    )
    context = SimpleNamespace(
        execution_coordinator=SimpleNamespace(reservations_repo=repo),
        extra={},
    )

    reserved = await _active_sell_reserved_size(
        context,
        user_id="u1",
        condition_id="0xcond",
        token_id=VALID_TOKEN_ID,
    )

    assert reserved == Decimal("5")
    repo.list_active_for_scope.assert_awaited_once_with(
        "u1",
        "0xcond",
        VALID_TOKEN_ID,
    )


@pytest.mark.asyncio
async def test_active_sell_reserved_size_releases_expired_orphan_before_sum():
    # A14: the orphan release needs POSITIVE proof — the orders lookup must
    # succeed and find no persisted submission for the reservation. An
    # unresolved no-ID submission keeps its shares reserved (covered in
    # tests/test_reservation_expiry_no_id_release.py).
    expired = datetime.fromtimestamp(time.time() - 10, tz=timezone.utc)
    repo = MagicMock()
    repo.list_active_for_scope = AsyncMock(
        return_value=[
            SimpleNamespace(
                id="res-orphan",
                side="SELL",
                status="active",
                reserved_size=Decimal("5"),
                order_id=None,
                venue_order_id=None,
                expires_at=expired,
            )
        ]
    )
    orders_repo = MagicMock()
    orders_repo.get_by_reservation_id = AsyncMock(return_value=None)
    reservation_service = SimpleNamespace(
        reservations_repo=repo,
        release_terminal=AsyncMock(),
    )
    context = SimpleNamespace(
        execution_coordinator=SimpleNamespace(
            reservations_repo=repo,
            reservation_service=reservation_service,
        ),
        orders_repo=orders_repo,
        extra={},
    )

    reserved = await _active_sell_reserved_size(
        context,
        user_id="u1",
        condition_id="0xcond",
        token_id=VALID_TOKEN_ID,
    )

    assert reserved == Decimal("0")
    orders_repo.get_by_reservation_id.assert_awaited_once_with("res-orphan")
    reservation_service.release_terminal.assert_awaited_once_with(
        "res-orphan",
        reason="expired_orphan_before_risk_exit",
    )


@pytest.mark.asyncio
async def test_active_sell_reserved_size_keeps_linked_order_reservation():
    expired = datetime.fromtimestamp(time.time() - 10, tz=timezone.utc)
    repo = MagicMock()
    repo.list_active_for_scope = AsyncMock(
        return_value=[
            SimpleNamespace(
                id="res-linked",
                side="SELL",
                status="active",
                reserved_size=Decimal("5"),
                order_id="ord-1",
                venue_order_id=None,
                expires_at=expired,
            )
        ]
    )
    reservation_service = SimpleNamespace(
        reservations_repo=repo,
        release_terminal=AsyncMock(),
    )
    context = SimpleNamespace(
        execution_coordinator=SimpleNamespace(
            reservations_repo=repo,
            reservation_service=reservation_service,
        ),
        extra={},
    )

    reserved = await _active_sell_reserved_size(
        context,
        user_id="u1",
        condition_id="0xcond",
        token_id=VALID_TOKEN_ID,
    )

    assert reserved == Decimal("5")
    reservation_service.release_terminal.assert_not_awaited()


def test_active_reservation_skip_logging_is_throttled():
    context = SimpleNamespace(extra={})

    first = _should_log_active_reservation_skip(
        context,
        condition_id="0xcond",
        token_id=VALID_TOKEN_ID,
        reason="stop_loss",
        now=100.0,
    )
    second = _should_log_active_reservation_skip(
        context,
        condition_id="0xcond",
        token_id=VALID_TOKEN_ID,
        reason="stop_loss",
        now=101.0,
    )
    third = _should_log_active_reservation_skip(
        context,
        condition_id="0xcond",
        token_id=VALID_TOKEN_ID,
        reason="stop_loss",
        now=131.0,
    )

    assert first is True
    assert second is False
    assert third is True
