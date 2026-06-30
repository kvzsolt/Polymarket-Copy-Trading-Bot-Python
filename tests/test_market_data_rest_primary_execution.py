"""REST-primary execution must not depend on WSS health (deep-research P1).

When ``MARKET_DATA_PRIMARY_SOURCE=rest_snapshot`` a copy-trade execution call
(``require_fresh_data(token, side=...)``) routes straight to
``require_execution_snapshot`` — a fresh REST ``/book`` fetch that is gated only
on age / spread / liquidity. It must succeed with the market WSS reported DOWN
when the REST snapshot is fresh and tight, and must fail closed when the snapshot
is wide or empty. These tests pin that contract so the (operator-tunable, and
gitignored) WSS-bootstrap flags can never silently re-gate the execution path.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from cache.market_state_store import MarketStateStore
from services.market_data_service import (
    MarketDataPolicy,
    MarketDataService,
    MarketDataSource,
    StaleDataError,
)

TEST_TOKEN_ID = "0x" + ("r" * 16) + "_rest_primary_exec"  # degenerate test-only token id


def _book(bid: str, ask: str) -> SimpleNamespace:
    return SimpleNamespace(
        bids=[SimpleNamespace(price=bid, size="100")],
        asks=[SimpleNamespace(price=ask, size="100")],
        tick_size="0.01",
        min_order_size="1",
        neg_risk=False,
    )


def _rest_primary_service(book_payload: SimpleNamespace) -> MarketDataService:
    """REST-primary MDS with the market WSS reported permanently DOWN."""
    cache = MarketStateStore()
    gateway = AsyncMock()
    gateway.get_orderbook = AsyncMock(return_value=book_payload)
    policy = MarketDataPolicy(primary_source="rest_snapshot")
    return MarketDataService(
        cache,
        gateway,
        policy=policy,
        is_wss_down_fn=lambda: True,
    )


@pytest.mark.asyncio
async def test_rest_primary_execution_succeeds_with_wss_down_and_fresh_rest() -> None:
    # ~196 bps spread, well inside the 300 bps cap.
    svc = _rest_primary_service(_book("0.50", "0.51"))
    bundle = await svc.require_fresh_data(TEST_TOKEN_ID, side="BUY", condition_id="0xcond")
    assert bundle.source == MarketDataSource.REST_SNAPSHOT


@pytest.mark.asyncio
async def test_rest_primary_execution_blocked_when_rest_snapshot_spread_wide() -> None:
    # ~3333 bps spread, far past the 300 bps cap -> fail closed.
    svc = _rest_primary_service(_book("0.40", "0.60"))
    with pytest.raises(StaleDataError):
        await svc.require_fresh_data(TEST_TOKEN_ID, side="BUY", condition_id="0xcond")


@pytest.mark.asyncio
async def test_rest_primary_execution_blocked_when_book_empty() -> None:
    empty = SimpleNamespace(
        bids=[], asks=[], tick_size="0.01", min_order_size="1", neg_risk=False
    )
    svc = _rest_primary_service(empty)
    with pytest.raises(StaleDataError):
        await svc.require_fresh_data(TEST_TOKEN_ID, side="BUY", condition_id="0xcond")
