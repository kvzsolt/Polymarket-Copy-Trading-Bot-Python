"""Phase 4 — exchange-side reconciliation extensions.

Validates that the periodic reconciler now:

* detects partial fills (venue cumulative matched > local matched_total) and
  persists the new matched_total
* flags terminal venue fills that we have no local record of
* flags terminal venue cancellations as ``unexpected_cancellation`` when our
  local row was still believed open
"""
from __future__ import annotations

import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from services.reconciliation import ReconciliationService


def _make_clob(orders):
    clob = MagicMock()
    clob.get_orders.return_value = orders
    return clob


class TestPartialFillDetection:
    @pytest.mark.asyncio
    async def test_partial_fill_recorded(self):
        now = time.time()
        venue_orders = [{
            "id": "o-partial",
            "timestamp": now,
            "status": "live",
            "size_matched": "12",
        }]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-partial",
            "status": "submitted_unconfirmed",
            "matched_total": "5",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.partial_fills_observed == 1
        assert result.partial_fill_deltas == (("o-partial", "7"),)
        repo.upsert_matched_total.assert_called_once_with(
            order_id="o-partial", matched="12"
        )

    @pytest.mark.asyncio
    async def test_no_partial_fill_when_unchanged(self):
        now = time.time()
        venue_orders = [{
            "id": "o-noop",
            "timestamp": now,
            "status": "live",
            "size_matched": "5",
        }]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-noop",
            "status": "submitted_unconfirmed",
            "matched_total": "5",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.partial_fills_observed == 0
        repo.upsert_matched_total.assert_not_called()

    @pytest.mark.asyncio
    async def test_filled_field_supported(self):
        """Some venue schemas use ``filled`` instead of ``size_matched``."""
        now = time.time()
        venue_orders = [{
            "id": "o-alt",
            "timestamp": now,
            "status": "live",
            "filled": "8",
        }]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-alt",
            "status": "submitted_unconfirmed",
            "matched_total": "3",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.partial_fills_observed == 1


class TestUnexpectedFillDetection:
    @pytest.mark.asyncio
    async def test_unexpected_fill_with_no_local_record(self):
        now = time.time()
        venue_orders = [{
            "id": "o-orphan",
            "timestamp": now,
            "status": "matched",
            "size_matched": "10",
        }]
        repo = MagicMock()
        repo.get_by_order_id.return_value = None

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.unexpected_fills_observed == 1
        assert result.locally_unknown_orders == 1

    @pytest.mark.asyncio
    async def test_terminal_but_zero_matched_is_not_a_fill(self):
        now = time.time()
        venue_orders = [{
            "id": "o-zero",
            "timestamp": now,
            "status": "matched",
            "size_matched": "0",
        }]
        repo = MagicMock()
        repo.get_by_order_id.return_value = None

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.unexpected_fills_observed == 0


class TestUnexpectedCancellationDetection:
    @pytest.mark.asyncio
    async def test_unexpected_cancellation_with_local_open(self):
        now = time.time()
        venue_orders = [{
            "id": "o-cancel",
            "timestamp": now,
            "status": "cancelled",
            "size_matched": "0",
        }]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-cancel",
            "status": "submitted_unconfirmed",
            "matched_total": "0",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.unexpected_cancellations == 1

    @pytest.mark.asyncio
    async def test_locally_cancelled_not_flagged(self):
        now = time.time()
        venue_orders = [{
            "id": "o-known-cancel",
            "timestamp": now,
            "status": "canceled",
            "size_matched": "0",
        }]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-known-cancel",
            "status": "cancelled",
            "matched_total": "0",
        }

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        assert result.unexpected_cancellations == 0


class TestPersistFailureIsSwallowed:
    @pytest.mark.asyncio
    async def test_persist_failure_does_not_break_loop(self):
        now = time.time()
        venue_orders = [{
            "id": "o-bad-persist",
            "timestamp": now,
            "status": "live",
            "size_matched": "9",
        }]
        repo = MagicMock()
        repo.get_by_order_id.return_value = {
            "order_id": "o-bad-persist",
            "status": "submitted_unconfirmed",
            "matched_total": "1",
        }
        repo.upsert_matched_total.side_effect = RuntimeError("sqlite locked")

        svc = ReconciliationService(
            clob_client=_make_clob(venue_orders),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        result = await svc.reconcile()

        # The partial fill is still observed even though persistence failed —
        # the next pass will retry the upsert with the same delta.
        assert result.partial_fills_observed == 1
        assert result.errors == []
