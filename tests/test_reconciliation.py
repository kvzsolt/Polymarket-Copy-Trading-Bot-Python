"""Tests for exchange-side reconciliation service."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from services.reconciliation import (
    ReconciliationResult,
    ReconciliationService,
    _order_age_seconds,
    _safe_int,
)


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
        repo_mock.get_by_idempotency_key.return_value = None

        svc = ReconciliationService(
            clob_client=clob,
            orders_repo=repo_mock,
            stale_threshold_seconds=9999,
        )
        result = await svc.reconcile()

        assert result.locally_unknown_orders == 1

    @pytest.mark.asyncio
    async def test_known_order_not_counted_unknown(self):
        """Orders found locally should not be counted as unknown."""
        now = time.time()
        orders = [{"id": "k1", "timestamp": now}]
        clob = _make_mock_clob(orders=orders)
        repo_mock = MagicMock()
        repo_mock.get_by_idempotency_key.return_value = {"some": "record"}

        svc = ReconciliationService(
            clob_client=clob,
            orders_repo=repo_mock,
            stale_threshold_seconds=9999,
        )
        result = await svc.reconcile()

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
    async def test_env_defaults(self):
        """Service reads from env when no explicit values."""
        clob = _make_mock_clob()
        with patch.dict("os.environ", {
            "RECONCILE_STALE_SECONDS": "1800",
            "RECONCILE_AUTO_CANCEL": "true",
        }):
            svc = ReconciliationService(clob_client=clob)
            assert svc.stale_threshold == 1800.0
            assert svc.auto_cancel is True
