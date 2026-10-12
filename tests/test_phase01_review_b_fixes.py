"""Review B fixes (C1, C3, C6 contract, C7) for phase 01.

Pins the post-Review-B behavior so regressions get caught:

  * C1 — reconcile_submitted_unconfirmed must not silently drop rows whose
    identity is missing; it emits a hard metric and falls back to a sentinel
    scope so the row still reconciles.
  * C3 — (superseded) the former V2 ``execute_intent`` path failed closed in
    live mode when the PositionScope import failed; that dead path has been
    removed, and live-mode serialization is enforced by the production
    ``execution_coordinator_missing`` guards plus per-scope locks around the
    ``submit_*`` surface.
  * C6 contract — two scopes that share (condition_id, token_id) but differ
    on user_id must not collide in the coordinator's lock registry.
  * C7 — (superseded by A04) the execution coordinator is now BORN with the
    account-owned position_coordinator via ``_build_execution_coordinator``;
    the old post-construction attach (and its failure branch) no longer
    exists. The posture test below pins the construction-time wiring.

These tests do not duplicate the broader coordinator integration suite; they
only nail down the specific posture asked for in Review B.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pmcopy.services.positionActionCoordinator import (
    PositionActionCoordinator,
    PositionScope,
)


# ---------------------------------------------------------------------------
# C1 — reconcile_submitted_unconfirmed legacy-row fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reconcile_emits_metric_and_fallback_for_empty_identity(monkeypatch):
    """Legacy rows with NULL user_id/condition_id/asset must reconcile via
    sentinel scope and emit ``reconcile_missing_scope_total``."""

    from pmcopy.orchestrators import trade_pipeline as tp

    metric_calls: list[tuple[str, dict | None]] = []

    def _fake_metric_inc(name, value=1.0, labels=None):
        metric_calls.append((name, labels))

    monkeypatch.setattr(tp.metrics, "inc", _fake_metric_inc)

    coord = PositionActionCoordinator()
    acquired_scopes: list[PositionScope] = []
    original_acquire = coord.acquire

    def _spy_acquire(scope):
        acquired_scopes.append(scope)
        return original_acquire(scope)

    coord.acquire = _spy_acquire  # type: ignore[assignment]

    legacy_record = {
        "order_id": "ord-legacy-1",
        "trade_key": "tk-legacy-1",
        "user_id": None,
        "condition_id": "",
        "asset": None,
    }

    reconcile_count = 0

    async def _fake_reconcile_one(_ctx, _rec, _side, *, startup_recovery=False):
        nonlocal reconcile_count
        assert startup_recovery is False
        reconcile_count += 1
        return True

    monkeypatch.setattr(tp, "_reconcile_one", _fake_reconcile_one)

    orders_repo = MagicMock()
    orders_repo.get_unconfirmed_buys = AsyncMock(return_value=[legacy_record])
    orders_repo.get_unconfirmed_sells = AsyncMock(return_value=[])

    ctx = SimpleNamespace(
        execution_service=MagicMock(),
        orders_repo=orders_repo,
        position_coordinator=coord,
        extra={},
        config=None,
    )

    n = await tp.reconcile_submitted_unconfirmed(ctx)
    assert n == 1
    assert reconcile_count == 1

    # Hard metric was emitted with the side label.
    assert any(
        name == "reconcile_missing_scope_total" and (labels or {}).get("side") == "BUY"
        for name, labels in metric_calls
    ), f"missing reconcile_missing_scope_total metric in {metric_calls!r}"

    # Sentinel scope was used so the lock could still serialise. Each legacy
    # row must get its OWN scope (salted by order_id / trade_key) so two
    # unrelated rows do not collapse into the same coordinator slot and
    # corrupt per-position attribution.
    assert len(acquired_scopes) == 1
    scope = acquired_scopes[0]
    assert scope.user_id.startswith("__legacy_user__")
    assert "ord-legacy-1" in scope.user_id
    assert scope.condition_id.startswith("__legacy_cond__")
    assert "ord-legacy-1" in scope.condition_id
    assert scope.token_id.startswith("__legacy_token__")
    assert "ord-legacy-1" in scope.token_id


@pytest.mark.asyncio
async def test_reconcile_submitted_unconfirmed_scopes_queries_to_context_user():
    """Runtime contexts must not reconcile every user's pending rows.

    A risk-loop pre-reconcile runs once per user context. If each context fetches
    every submitted_unconfirmed row globally, one stuck venue order can multiply
    work across all users and repeatedly hit the pre-reconcile timeout.
    """
    from pmcopy.orchestrators import trade_pipeline as tp

    orders_repo = MagicMock()
    orders_repo.get_unconfirmed_buys = AsyncMock(return_value=[])
    orders_repo.get_unconfirmed_sells = AsyncMock(return_value=[])

    ctx = SimpleNamespace(
        user_id="0xUser",
        execution_service=MagicMock(),
        orders_repo=orders_repo,
        position_coordinator=PositionActionCoordinator(),
        extra={},
        config=None,
    )

    assert await tp.reconcile_submitted_unconfirmed(ctx) == 0
    orders_repo.get_unconfirmed_buys.assert_awaited_once_with(
        max_age_seconds=300.0,
        user_id="0xUser",
    )
    orders_repo.get_unconfirmed_sells.assert_awaited_once_with(
        max_age_seconds=300.0,
        user_id="0xUser",
    )


@pytest.mark.asyncio
async def test_reconcile_includes_orders_table_only_v1_submissions(monkeypatch):
    """Reconcile v1 orders even when processed_trades has no matching row."""

    from pmcopy.orchestrators import trade_pipeline as tp

    class OrdersWithV1Rows:
        """Expose both legacy scans and the bounded v1 orders scan."""

        async def get_unconfirmed_buys(self, **_kwargs):
            """Return no legacy processed-trade BUY rows."""

            return []

        async def get_unconfirmed_sells(self, **_kwargs):
            """Return no legacy processed-trade SELL rows."""

            return []

        async def get_unconfirmed_order_submissions(self, **kwargs):
            """Return one v1 order persisted without processed_trades."""

            assert kwargs == {"user_id": "user-a", "limit": 200}
            return [
                {
                    "idempotency_key": "copy-task:abc",
                    "user_id": "user-a",
                    "leader_id": "leader-a",
                    "order_id": "order-a",
                    "status": "submitted_unconfirmed",
                    "request": {
                        "side": "BUY",
                        "signal_id": "copy-task:abc",
                        "token_id": "token-a",
                        "condition_id": "condition-a",
                    },
                }
            ]

    reconciled: list[tuple[str, str]] = []

    async def _fake_reconcile_one(_ctx, record, side, *, startup_recovery=False):
        assert startup_recovery is False
        reconciled.append((record["order_id"], side))
        return True

    monkeypatch.setattr(tp, "_reconcile_one", _fake_reconcile_one)
    ctx = SimpleNamespace(
        user_id="user-a",
        execution_service=MagicMock(),
        orders_repo=OrdersWithV1Rows(),
        position_coordinator=PositionActionCoordinator(),
        extra={},
        config=None,
    )

    assert await tp.reconcile_submitted_unconfirmed(ctx) == 1
    assert reconciled == [("order-a", "BUY")]


# ---------------------------------------------------------------------------
# C3 — fail-closed lock bypass in live mode
# ---------------------------------------------------------------------------


def _live_config() -> SimpleNamespace:
    return SimpleNamespace(
        dry_run=False,
        execution=SimpleNamespace(paper_trade_only=False),
    )


def _paper_config() -> SimpleNamespace:
    return SimpleNamespace(
        dry_run=False,
        execution=SimpleNamespace(paper_trade_only=True),
    )


def _make_coordinator(*, config):
    from pmcopy.services.execution.coordinator import ExecutionCoordinator

    execution_service = SimpleNamespace(_config=config)
    return ExecutionCoordinator(
        reservation_service=MagicMock(),
        positions_repo=MagicMock(),
        execution_service=execution_service,
        position_coordinator=PositionActionCoordinator(),
    )


# ---------------------------------------------------------------------------
# C6 contract — different users on same (cond, token) must not collide
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_coordinator_per_user_isolation_on_same_cond_token():
    coord = PositionActionCoordinator()

    scope_user_a = PositionScope(
        user_id="user-A",
        condition_id="cond-shared",
        token_id="tok-shared",
    )
    scope_user_b = PositionScope(
        user_id="user-B",
        condition_id="cond-shared",
        token_id="tok-shared",
    )

    lock_a = await coord.get_lock(scope_user_a)
    lock_b = await coord.get_lock(scope_user_b)

    assert lock_a is not lock_b, (
        "coordinator must key locks by (user_id, condition_id, token_id) — "
        "two users on the same (cond, token) must not share a lock"
    )

    await lock_a.acquire()
    try:
        # Acquiring user B must succeed immediately — independent lock.
        await asyncio.wait_for(lock_b.acquire(), timeout=0.5)
        lock_b.release()
    finally:
        lock_a.release()


# ---------------------------------------------------------------------------
# C7 (A04 posture) — the execution coordinator is BORN with the account-owned
# position_coordinator; the old post-construction attach was removed.
# ---------------------------------------------------------------------------


def test_runtime_bootstrap_helper_wires_account_coordinator_at_construction():
    """``_build_execution_coordinator`` must inject the account-owned
    position_coordinator at construction so the execution funnel serializes
    on the same lock map as every per-binding TradingContext — with no
    post-construction rewiring left to silently drop.
    """
    from pmcopy.orchestrators.runtime_bootstrap import _build_execution_coordinator
    from pmcopy.services.positionCoordinator import PositionCoordinator

    pos_coord = PositionCoordinator()
    coord = _build_execution_coordinator(
        pool=MagicMock(),
        positions_repo=MagicMock(),
        execution_service=MagicMock(),
        funder_address="0x" + "9" * 40,
        position_coordinator=pos_coord,
    )

    assert coord is not None
    assert coord.position_coordinator is pos_coord
    assert coord.position_coordinator.intent_registry is pos_coord.intent_registry
