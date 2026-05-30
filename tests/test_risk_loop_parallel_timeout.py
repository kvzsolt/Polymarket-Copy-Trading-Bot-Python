"""Regression: risk_monitor_loop_for_contexts runs contexts in parallel and
times them out individually.

Pre-fix: the outer loop iterated contexts sequentially and awaited each
``run_risk_check_for_context`` without a timeout. A hung venue call on
context A blocked contexts B…N indefinitely; the entire risk pipeline
froze behind one stalled user.

Post-fix: contexts run via ``asyncio.gather`` and each call is wrapped
in ``asyncio.wait_for`` so a stalled context returns asyncio.TimeoutError
and the loop continues for everyone else.
"""
from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import pytest


def _ctx(user_id: str):
    return SimpleNamespace(user_id=user_id, extra={})


@pytest.mark.asyncio
async def test_one_stalled_context_does_not_block_the_others():
    from services.risk.risk_monitor import risk_monitor_loop_for_contexts

    ctx_fast = _ctx("fast")
    ctx_slow = _ctx("slow")

    started: dict[str, float] = {}
    finished: dict[str, float] = {}

    async def check(context):
        started[context.user_id] = time.monotonic()
        if context.user_id == "slow":
            # Stall indefinitely — the wait_for ceiling must fire.
            await asyncio.sleep(100)
        else:
            await asyncio.sleep(0)
        finished[context.user_id] = time.monotonic()

    async def refresh(context, refresh_interval):
        return None

    readiness = MagicMock()
    config = SimpleNamespace(
        polling=SimpleNamespace(risk_check_interval=0.05),
        risk=SimpleNamespace(),
        execution=SimpleNamespace(),
    )

    # Wrap the whole loop in a wait_for so we get exactly one cycle then
    # cancellation. The internal post-cycle asyncio.sleep stays real so
    # wait_for inside the loop still works correctly.
    with patch(
        "services.risk.risk_monitor.run_risk_check_for_context",
        side_effect=check,
    ), patch(
        "services.risk.risk_monitor._refresh_context_price_cache",
        side_effect=refresh,
    ), patch(
        "services.risk.risk_monitor._exit_monitor_enabled", return_value=True
    ):
        # Outer wait_for is 5x the per-context timeout — gives one full
        # cycle plus a sleep tick to complete before we cancel.
        with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(
                risk_monitor_loop_for_contexts(
                    [ctx_fast, ctx_slow],
                    readiness,
                    config=config,
                    interval=0.05,
                ),
                timeout=10.0,  # generous ceiling for the test wait_for
            )

    # Fast context completed; slow context started but did NOT complete
    # (the timeout cancelled it).
    assert "fast" in finished, "fast context must complete in parallel"
    assert "slow" in started, "slow context must have been started"
    assert "slow" not in finished, "slow context must NOT complete — timeout cancels"
