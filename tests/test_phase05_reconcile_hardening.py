"""Phase 5 — reconciliation loop/probe hardening.

Covers two small non-architectural fixes:

* H2/N5 — the per-pass closed-order probe is bounded (``max_closed_probes``)
  so an unbounded local in-flight backlog cannot become an N+1 venue-call
  storm against rate-limited order endpoints.
* H3/#11 — ``reconciliation_loop`` sleeps interruptibly: a shutdown request
  wakes it immediately instead of blocking for the full interval (up to 300s).
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from services.reconciliation import ReconciliationService, reconciliation_loop


def _make_clob(open_orders, probed_order=None):
    clob = MagicMock()
    clob.get_orders.return_value = open_orders
    clob.cancel_orders.return_value = None
    clob.get_order.return_value = probed_order
    return clob


class TestClosedOrderProbeCap:
    @pytest.mark.asyncio
    async def test_probes_are_capped_per_pass(self):
        # 25 locally-in-flight orders, none on the open list → every one is a
        # probe candidate. With the cap at 5 only 5 venue calls may fire.
        now = time.time()
        in_flight = [
            {"order_id": f"o-{i}", "status": "submitted_unconfirmed",
             "matched_total": "0", "side": "BUY", "request": {"amount": "10"}}
            for i in range(25)
        ]
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = in_flight
        repo.get_by_order_id.return_value = None

        # Every probe returns a still-live order → classification yields no
        # delta, so the only thing under test is the call count.
        probed = {"status": "live", "size_matched": "0", "timestamp": now}
        svc = ReconciliationService(
            clob_client=_make_clob([], probed_order=probed),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        svc.max_closed_probes = 5  # exercise the cap deterministically

        await svc.reconcile()

        assert svc.clob_client.get_order.call_count == 5

    @pytest.mark.asyncio
    async def test_default_cap_is_ten(self):
        svc = ReconciliationService(
            clob_client=_make_clob([]),
            orders_repo=MagicMock(),
            stale_threshold_seconds=9_999,
        )
        assert svc.max_closed_probes == 10


class TestLoopShutdownResponsiveness:
    @pytest.mark.asyncio
    async def test_loop_wakes_on_shutdown_instead_of_sleeping_full_interval(self):
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = []
        svc = ReconciliationService(
            clob_client=_make_clob([]),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        shutdown = asyncio.Event()

        async def _trip():
            await asyncio.sleep(0.05)
            shutdown.set()

        asyncio.create_task(_trip())
        # interval is huge; a blind sleep would hang here. wait_for proves the
        # loop returns promptly once shutdown is requested.
        await asyncio.wait_for(
            reconciliation_loop(svc, interval_seconds=300, shutdown_event=shutdown),
            timeout=2.0,
        )

    @pytest.mark.asyncio
    async def test_loop_exits_immediately_if_already_shutdown(self):
        repo = MagicMock()
        repo.get_unconfirmed_order_submissions.return_value = []
        svc = ReconciliationService(
            clob_client=_make_clob([]),
            orders_repo=repo,
            stale_threshold_seconds=9_999,
        )
        shutdown = asyncio.Event()
        shutdown.set()
        await asyncio.wait_for(
            reconciliation_loop(svc, interval_seconds=300, shutdown_event=shutdown),
            timeout=2.0,
        )
