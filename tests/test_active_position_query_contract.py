"""Active inventory queries must exclude settled holdings before limits and pagination."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from domain.order_lifecycle import IntentReason, IntentSide, OrderIntent, OrderIntentKey
from services.control_plane.contracts import PageRequest
from storage.postgres.control_plane_query_repository import PostgresControlPlaneQueryRepository
from storage.postgres.task_buy_capacity import PostgresTaskBuyCapacity


class FixedClock:
    """Provide deterministic observation time for the query repository."""

    def now(self) -> datetime:
        """Return a fixed aware timestamp."""
        return datetime(2026, 9, 16, tzinfo=UTC)


class QueryBoundary:
    """Capture SQL at the database boundary without supplying filtered test rows."""

    def __init__(self) -> None:
        self.pool = MagicMock()
        self.pool.fetch = AsyncMock(return_value=[])
        self.pool.fetchrow = AsyncMock(return_value=None)
        self.repository = PostgresControlPlaneQueryRepository(self.pool, clock=FixedClock())

    def assert_active_filter(self, query: str) -> None:
        """Require exclusion at the query boundary while retaining unknown lifecycle."""
        normalized = " ".join(query.lower().split())
        assert "lifecycle <> 'settled'" in normalized
        assert "lifecycle = 'active'" not in normalized
        assert "size_tokens::numeric > 0" in normalized


@pytest.mark.asyncio
async def test_task_market_capacity_excludes_settled_holdings_but_keeps_pending_orders() -> None:
    """Settled holdings leave market capacity; pending BUY reservations retain it."""
    boundary = QueryBoundary()
    intent = OrderIntent(
        key=OrderIntentKey(
            user_id="follower",
            leader_wallet="leader",
            condition_id="condition",
            token_id="token",
            side=IntentSide.BUY,
            reason=IntentReason.COPY_BUY,
        ),
        size=Decimal("1"),
        price=Decimal("0.5"),
        max_slippage_bps=100,
        created_at=FixedClock().now(),
        max_market_notional=Decimal("10"),
        max_open_markets=1,
    )
    assert await PostgresTaskBuyCapacity.permits(boundary.pool, intent, Decimal("1"))
    holdings, reservations = boundary.pool.fetch.await_args_list
    boundary.assert_active_filter(holdings.args[0])
    assert holdings.args[1:] == ("follower",)
    assert "order_reservations" in reservations.args[0]
    assert "status = 'active'" in reservations.args[0]
    assert "lifecycle" not in reservations.args[0]


@pytest.mark.asyncio
async def test_active_position_page_filters_settled_before_pagination() -> None:
    """Settled rows cannot consume page slots or appear as active positions."""
    boundary = QueryBoundary()
    await boundary.repository.list_positions("follower", PageRequest(limit=10))
    query = boundary.pool.fetch.await_args.args[0]
    boundary.assert_active_filter(query)
    assert query.index("lifecycle") < query.index("LIMIT")
    assert boundary.pool.fetch.await_args.args[1] == "follower"


@pytest.mark.asyncio
async def test_single_active_position_query_excludes_settled_holdings() -> None:
    """Looking up a raw token cannot bypass the active portfolio filter."""
    boundary = QueryBoundary()
    assert await boundary.repository.get_position("follower", "token") is None
    boundary.assert_active_filter(boundary.pool.fetchrow.await_args.args[0])
    assert boundary.pool.fetchrow.await_args.args[1:] == ("follower", "token")


@pytest.mark.asyncio
async def test_leader_open_position_count_excludes_settled_before_aggregation() -> None:
    """Leader counts aggregate only active or unclassified held inventory."""
    boundary = QueryBoundary()
    assert await boundary.repository.leader_open_position_counts() == {}
    query = boundary.pool.fetch.await_args.args[0]
    boundary.assert_active_filter(query)
    assert query.index("lifecycle") < query.index("GROUP BY")
