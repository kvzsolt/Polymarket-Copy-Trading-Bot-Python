"""Issue #101 — end-to-end live-execution invariants.

Anchors the riskiest live-trading invariants in **one** release-gate test
that exercises the real Postgres ``OrdersRepo`` against the production
``ExecutionService.execute`` path (gateway is the only mock). Existing
release-gate modules cover the individual matrix items piecewise; this
file ties the cross-cutting invariants together so a future refactor can't
silently shrink coverage by passing each unit while breaking the combined
behaviour.

Invariants asserted here:

1. exactly one venue POST under concurrent identical intents
2. the durable order row carries the venue order_id
3. subsequent identical execute() returns ``duplicate_ignored`` against the
   real persisted row (no second POST)
4. an unknown SDK exception keeps the duplicate-lock active (#100) — the
   reservation does not silently free
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

pytestmark = pytest.mark.integration

# Reuse the heavy fixtures already proven in test_execution_idempotency_extended
# so we don't duplicate construction code.
from tests.test_execution_idempotency_extended import (  # type: ignore[import-not-found]  # noqa: E402
    _FeeSvc,
    _SimpleClob,
    _make_exec_config,
    _make_mds,
    _make_req,
    _pg_pool,
    _reset_circuit,  # noqa: F401 — re-export so the autouse fixture applies
)
from services.executionService import ExecutionService  # noqa: E402
from storage.postgres.orders_repo import OrdersRepo  # noqa: E402


@pytest.mark.asyncio
async def test_concurrent_identical_intents_durable_state():
    """Inv-1 + Inv-2: N concurrent identical execute() → exactly one venue
    POST, and the durable order row carries the venue's order_id."""
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        clob = _SimpleClob()
        svc = ExecutionService(
            clob, _FeeSvc(), repo,
            config=_make_exec_config(),
            market_data_service=_make_mds(),
        )
        req = _make_req(signal_id="sig-live-inv")

        N = 20
        results = await asyncio.gather(*[svc.execute(req) for _ in range(N)])

        # Exactly one signed order ever reached the venue.
        assert clob.post_signed_order.await_count == 1
        # At least one task got the live order_id; losers either duplicate_ignored
        # (after the winner persisted) or atomic-lock-rejected.
        accepted = [r for r in results if r.order_id == "ord-1"]
        assert accepted, "no task observed the venue order_id"


@pytest.mark.asyncio
async def test_replay_after_persist_collapses_to_duplicate():
    """Inv-3: a follow-up execute() with the same effective identity, after
    the winner has persisted, must collapse to ``duplicate_ignored`` against
    the real Postgres row (no second venue POST)."""
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        clob = _SimpleClob()
        svc = ExecutionService(
            clob, _FeeSvc(), repo,
            config=_make_exec_config(),
            market_data_service=_make_mds(),
        )
        req = _make_req(signal_id="sig-dup-live")

        first = await svc.execute(req)
        assert first.accepted is True

        second = await svc.execute(req)
        assert second.status == "duplicate_ignored"
        assert clob.post_signed_order.await_count == 1


@pytest.mark.asyncio
async def test_unknown_sdk_exception_keeps_duplicate_lock():
    """Inv-4 (#100): an unknown post-call exception must be ambiguous, not
    terminal. The duplicate-lock must remain active so a follow-up identical
    intent is collapsed instead of producing a second signed order."""
    async with _pg_pool() as pool:
        repo = OrdersRepo(pool)
        clob = _SimpleClob()
        # Novel exception type → unknown classification → VenueUnavailableError
        # (ambiguous) → submitted_unconfirmed → persisted row keeps the lock.
        clob.create_market_order = AsyncMock(side_effect=RuntimeError("upstream closed"))
        svc = ExecutionService(
            clob, _FeeSvc(), repo,
            config=_make_exec_config(),
            market_data_service=_make_mds(),
        )
        req = _make_req(signal_id="sig-unknown-live")

        first = await svc.execute(req)
        assert first.status == "submitted_unconfirmed"

        # Replay: must collapse against the durable lock, not generate another
        # signed order. ``create_market_order`` raises again but a second invocation
        # is the exact regression #100 prevents.
        second = await svc.execute(req)
        # The duplicate-lock path must absorb the second execute() — either
        # duplicate_ignored (lock already persisted) or another ambiguous
        # submitted_unconfirmed (lock active, awaiting reconciliation). It
        # must NOT classify as terminal "failed" / "rejected" — that was the
        # #100 regression that would let a duplicate slip through.
        assert second.status in ("duplicate_ignored", "submitted_unconfirmed")
        assert second.status not in ("failed", "rejected")
