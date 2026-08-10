"""Regression: startup_reconcile_unconfirmed must surface per-user failures.

Pre-fix: any exception inside the per-user reconcile loop was caught and
logged at WARNING level, then dropped on the floor. The outer dict only
counted successful users; the bot then unconditionally set
``application.readiness.update(reconciler_ok=True)`` and started monitors
on a stale position snapshot.

This test pins:
  * startup_reconcile_unconfirmed accepts a ``failures_out`` list kwarg
    that the function appends each failed user_id to.
  * Existing callers (no failures_out) get unchanged behaviour.
  * A failing reconcile call populates the list AND returns a dict that
    does not include the failed user.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from orchestrators.runtime_bootstrap import startup_reconcile_unconfirmed


def _ctx(user_id: str):
    """Minimal TradingContext stand-in for the reconcile path."""
    return SimpleNamespace(
        user_id=user_id,
        execution_service=object(),  # truthy — passes the `is None` skip
    )


@pytest.mark.asyncio
async def test_failures_out_populated_on_exception():
    contexts = [_ctx("u_ok"), _ctx("u_bad")]
    side_effects = {"u_ok": 1, "u_bad": RuntimeError("boom")}

    async def fake_trade(ctx):
        outcome = side_effects[ctx.user_id]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def fake_risk(ctx):
        return 0

    failures: list[str] = []
    with (
        patch(
            "orchestrators.trade_pipeline.reconcile_submitted_unconfirmed",
            side_effect=fake_trade,
        ),
        patch(
            "orchestrators.runtime_bootstrap.reconcile_unconfirmed_risk_orders",
            side_effect=fake_risk,
        ),
        patch(
            "orchestrators.runtime_bootstrap.rehydrate_position_manager",
        ),
    ):
        result = await startup_reconcile_unconfirmed(contexts, failures_out=failures)

    assert failures == ["u_bad"], (
        f"failures_out must list every user whose reconcile raised; got {failures}"
    )
    assert "u_bad" not in result, "failed user must not appear in reconciled dict"
    assert result.get("u_ok") == 1, f"clean user must still appear; got {result}"


@pytest.mark.asyncio
async def test_backward_compat_no_failures_out_kwarg():
    """Existing callers that omit failures_out must keep working."""
    contexts = [_ctx("u_ok")]

    async def fake_trade(ctx):
        return 2

    async def fake_risk(ctx):
        return 1

    with (
        patch(
            "orchestrators.trade_pipeline.reconcile_submitted_unconfirmed",
            side_effect=fake_trade,
        ),
        patch(
            "orchestrators.runtime_bootstrap.reconcile_unconfirmed_risk_orders",
            side_effect=fake_risk,
        ),
        patch(
            "orchestrators.runtime_bootstrap.rehydrate_position_manager",
        ),
    ):
        result = await startup_reconcile_unconfirmed(contexts)

    assert result == {"u_ok": 3}
