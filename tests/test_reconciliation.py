"""Tests for exchange-side reconciliation service."""

from __future__ import annotations

import contextlib
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.reconciliation import (
    ReconciliationResult,
    ReconciliationService,
    _order_age_seconds,
    _safe_int,
)
from storage.postgres.orders_repo import OrderSubmissionRecord, OrdersRepo


@contextlib.asynccontextmanager
async def _pg_pool():
    """Spin a Postgres testcontainer + return a pool with the full
    schema applied. Auto-skips when Docker is unavailable."""
    try:
        from testcontainers.postgres import PostgresContainer
        import asyncpg
        from storage.postgres.migrations import apply_migrations as pg_apply
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        ctr = PostgresContainer("postgres:16-alpine")
        ctr.start()
    except Exception as exc:
        pytest.skip(f"Docker / postgres image unavailable: {exc!r}")
    try:
        dsn = ctr.get_connection_url(driver="asyncpg").replace(
            "postgresql+asyncpg://", "postgresql://"
        )
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)
        async with pool.acquire() as conn:
            await pg_apply(conn)
        try:
            yield pool
        finally:
            await pool.close()
    finally:
        ctr.stop()


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_safe_int_normal(self):
        assert _safe_int(42) == 42
        assert _safe_int("7") == 7

    def test_safe_int_bad(self):
        assert _safe_int(None) == 0
        assert _safe_int("abc", 99) == 99

    def test_order_age_seconds_epoch(self):
        now = 1700000000.0
        order = {"timestamp": 1699999000}
        assert _order_age_seconds(order, now) == pytest.approx(1000.0, abs=1)

    def test_order_age_millis(self):
        now = 1700000000.0
        order = {"timestamp": 1699999000000}  # millis
        assert _order_age_seconds(order, now) == pytest.approx(1000.0, abs=1)

    def test_order_age_no_ts(self):
        assert _order_age_seconds({}, time.time()) == 0.0


# ---------------------------------------------------------------------------
# Mock CLOB client
# ---------------------------------------------------------------------------

def _make_mock_clob(orders: list[dict] | None = None, cancel_side_effect=None):
    """Build a mock clob_client with get_orders and cancel_orders."""
    mock = MagicMock()
    mock.get_orders.return_value = orders or []
    if cancel_side_effect:
        mock.cancel_orders.side_effect = cancel_side_effect
    return mock


# ---------------------------------------------------------------------------
# ReconciliationService tests
# ---------------------------------------------------------------------------

class TestReconciliationService:

    @pytest.mark.asyncio
    async def test_empty_exchange(self):
        clob = _make_mock_clob(orders=[])
        svc = ReconciliationService(clob_client=clob)
        result = await svc.reconcile()

        assert isinstance(result, ReconciliationResult)
        assert result.open_orders_on_exchange == 0
        assert result.stale_orders_cancelled == 0
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_fresh_orders_not_stale(self):
        """Orders younger than threshold should not be cancelled."""
        now = time.time()
        orders = [
            {"id": "o1", "timestamp": now - 60},   # 1 min old
            {"id": "o2", "timestamp": now - 120},   # 2 min old
        ]
        clob = _make_mock_clob(orders=orders)
        svc = ReconciliationService(clob_client=clob, stale_threshold_seconds=3600)
        result = await svc.reconcile()

        assert result.open_orders_on_exchange == 2
        assert result.stale_orders_cancelled == 0

    @pytest.mark.asyncio
    async def test_stale_orders_detected(self):
        """Orders older than threshold should be flagged."""
        now = time.time()
        orders = [
            {"id": "o1", "timestamp": now - 7200},   # 2h old → stale
            {"id": "o2", "timestamp": now - 60},      # 1 min → fresh
        ]
        clob = _make_mock_clob(orders=orders)
        svc = ReconciliationService(
            clob_client=clob,
            stale_threshold_seconds=3600,
            auto_cancel_stale=False,
        )
        result = await svc.reconcile()

        assert result.open_orders_on_exchange == 2
        assert result.stale_orders_cancelled == 0  # auto_cancel is off

    @pytest.mark.asyncio
    async def test_stale_orders_cancelled(self):
        """With auto_cancel=True, stale orders get cancelled."""
        now = time.time()
        orders = [
            {"id": "stale1", "timestamp": now - 7200},
            {"id": "stale2", "timestamp": now - 5000},
        ]
        clob = _make_mock_clob(orders=orders)
        svc = ReconciliationService(
            clob_client=clob,
            stale_threshold_seconds=3600,
            auto_cancel_stale=True,
        )
        result = await svc.reconcile()

        assert result.stale_orders_cancelled == 2
        assert clob.cancel_orders.call_count == 2

    @pytest.mark.asyncio
    async def test_cancel_failure_recorded(self):
        """If cancel fails, error is recorded but we keep going."""
        now = time.time()
        orders = [{"id": "bad1", "timestamp": now - 9999}]
        clob = _make_mock_clob(
            orders=orders,
            cancel_side_effect=RuntimeError("exchange down"),
        )
        svc = ReconciliationService(
            clob_client=clob,
            stale_threshold_seconds=3600,
            auto_cancel_stale=True,
        )
        result = await svc.reconcile()

        assert result.stale_orders_cancelled == 0
        assert len(result.errors) == 1
        assert "exchange down" in result.errors[0]

    @pytest.mark.asyncio
    async def test_fetch_failure_returns_error(self):
        """If fetching open orders fails, result has error."""
        clob = MagicMock()
        clob.get_orders.side_effect = ConnectionError("timeout")
        svc = ReconciliationService(clob_client=clob)
        result = await svc.reconcile()

        assert result.open_orders_on_exchange == 0
        assert len(result.errors) == 1
        assert "timeout" in result.errors[0]

    @pytest.mark.asyncio
    async def test_locally_unknown_orders(self):
        """Orders on exchange but not in local repo are counted."""
        now = time.time()
        orders = [{"id": "x1", "timestamp": now}]
        clob = _make_mock_clob(orders=orders)
        repo_mock = MagicMock()
        repo_mock.get_by_order_id = AsyncMock(return_value=None)

        svc = ReconciliationService(
            clob_client=clob,
            orders_repo=repo_mock,
            stale_threshold_seconds=9999,
        )
        result = await svc.reconcile()

        repo_mock.get_by_order_id.assert_called_once_with("x1")
        assert result.locally_unknown_orders == 1

    @pytest.mark.asyncio
    async def test_known_order_not_counted_unknown(self):
        """Orders found locally by venue order_id should not be counted as unknown."""
        now = time.time()
        orders = [{"id": "k1", "timestamp": now}]
        clob = _make_mock_clob(orders=orders)
        repo_mock = MagicMock()
        repo_mock.get_by_order_id = AsyncMock(return_value={"some": "record"})

        svc = ReconciliationService(
            clob_client=clob,
            orders_repo=repo_mock,
            stale_threshold_seconds=9999,
        )
        result = await svc.reconcile()

        repo_mock.get_by_order_id.assert_called_once_with("k1")
        assert result.locally_unknown_orders == 0

    @pytest.mark.asyncio
    async def test_dict_response_format(self):
        """Handle paginated dict response from get_orders."""
        clob = MagicMock()
        clob.get_orders.return_value = {"data": [{"id": "d1"}, {"id": "d2"}]}
        svc = ReconciliationService(clob_client=clob, stale_threshold_seconds=9999)
        result = await svc.reconcile()

        assert result.open_orders_on_exchange == 2

    @pytest.mark.asyncio
    async def test_orders_client_prefers_top_level_order_methods(self):
        """If top-level client exposes order methods, use it instead of nested .client."""
        clob = MagicMock()
        clob.get_orders.return_value = [{"id": "top-level"}]
        clob.client.get_orders.return_value = [{"id": "nested"}]

        svc = ReconciliationService(clob_client=clob, stale_threshold_seconds=9999)
        result = await svc.reconcile()

        assert result.open_orders_on_exchange == 1
        clob.get_orders.assert_called_once()
        clob.client.get_orders.assert_not_called()

    @pytest.mark.asyncio
    async def test_orders_client_falls_back_to_nested_client(self):
        """If top-level client lacks order methods, use nested .client implementation."""

        class _Wrapper:
            def __init__(self):
                self.client = MagicMock()

        wrapper = _Wrapper()
        wrapper.client.get_orders.return_value = [{"id": "nested-1"}, {"id": "nested-2"}]

        svc = ReconciliationService(clob_client=wrapper, stale_threshold_seconds=9999)
        result = await svc.reconcile()

        assert result.open_orders_on_exchange == 2
        wrapper.client.get_orders.assert_called_once()

    @pytest.mark.asyncio
    async def test_appconfig_defaults(self):
        """Service reads from AppConfig.reconciliation when no explicit values."""
        from types import SimpleNamespace
        clob = _make_mock_clob()
        cfg = SimpleNamespace(
            reconciliation=SimpleNamespace(
                stale_threshold_seconds=1800.0,
                auto_cancel_stale=True,
                max_closed_probes=10,
            ),
        )
        svc = ReconciliationService(clob_client=clob, config=cfg)
        assert svc.stale_threshold == 1800.0
        assert svc.auto_cancel is True


class TestOrderIdentityResolution:
    @pytest.mark.asyncio
    async def test_orders_repo_can_lookup_by_venue_order_id(self):
        async with _pg_pool() as pool:
            repo = OrdersRepo(pool)
            await repo.save_submission(
                OrderSubmissionRecord(
                    idempotency_key="idem-1",
                    user_id="u1",
                    leader_id="leader1",
                    order_id="venue-123",
                    status="submitted_unconfirmed",
                    request_payload={"foo": "bar"},
                    response_payload={"orderID": "venue-123"},
                )
            )

            by_order = await repo.get_by_order_id("venue-123")
        assert by_order is not None
        assert by_order["order_id"] == "venue-123"
        assert by_order["idempotency_key"] == "idem-1"

    @pytest.mark.asyncio
    async def test_reconciliation_prefers_venue_order_id_lookup(self):
        now = time.time()
        orders = [{"id": "venue-999", "timestamp": now}]
        clob = _make_mock_clob(orders=orders)
        repo_mock = MagicMock()
        repo_mock.get_by_order_id = AsyncMock(
            return_value={"idempotency_key": "idem-999", "order_id": "venue-999"}
        )
        repo_mock.get_by_idempotency_key = AsyncMock(side_effect=AssertionError(
            "reconciliation must not treat venue order_id as idempotency key"
        ))

        svc = ReconciliationService(
            clob_client=clob,
            orders_repo=repo_mock,
            stale_threshold_seconds=9999,
        )
        result = await svc.reconcile()

        repo_mock.get_by_order_id.assert_called_once_with("venue-999")
        assert result.locally_unknown_orders == 0
