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


def test_pre_reconcile_default_timeout_exceeds_venue_get_timeout():
    from integrations.polymarket.clob_v2_gateway import _GET_ORDER_HTTP_TIMEOUT_SECONDS
    from services.risk.risk_monitor import _risk_pre_reconcile_timeout_seconds

    timeout = _risk_pre_reconcile_timeout_seconds(0.05)

    # Worst case for ONE healthy-but-slow venue GET: the first attempt burns
    # the full aiohttp total timeout (10s), then _async_signed_get retries
    # once after 0.15s and that retry may burn another full 10s before the
    # result lands. 12.0s ("timeout plus retry delay") undercounted the retry
    # as 0.15s instead of a second full request — the outer guard cancelled
    # every in-flight reconcile whenever the venue was slow, live-spamming
    # "Risk pre-reconcile TIMEOUT ... after 12.0s".
    assert timeout > 2 * _GET_ORDER_HTTP_TIMEOUT_SECONDS + 0.15
    assert _risk_pre_reconcile_timeout_seconds(30.0) == 30.0


@pytest.mark.asyncio
async def test_one_stalled_context_does_not_block_the_others():
    from services.risk.risk_monitor import risk_monitor_loop_for_contexts

    ctx_fast = _ctx("fast")
    ctx_slow = _ctx("slow")

    started: dict[str, float] = {}
    finished: dict[str, float] = {}

    async def check(context, *, pre_reconcile: bool = True):
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
    with (
        patch(
            "services.risk.risk_monitor.run_risk_check_for_context",
            side_effect=check,
        ),
        patch(
            "services.risk.risk_monitor._refresh_context_price_cache",
            side_effect=refresh,
        ),
        patch("services.risk.risk_monitor._exit_monitor_enabled", return_value=True),
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


@pytest.mark.asyncio
async def test_pre_reconcile_runs_outside_protective_exit_timeout():
    """Pre-reconcile (venue order-status round-trips) runs in its OWN untimed
    phase, and the timed protective-exit check is invoked with
    ``pre_reconcile=False`` so slow reconciliation cannot starve the stop-loss
    evaluation every cycle (sym 1)."""
    from services.risk.risk_monitor import risk_monitor_loop_for_contexts

    ctx = _ctx("u1")
    check_calls: list[bool] = []

    async def slow_pre_reconcile(context):
        await asyncio.sleep(0.2)  # untimed — does not count against the budget
        return 0

    async def check(context, *, pre_reconcile: bool = True):
        check_calls.append(pre_reconcile)
        await asyncio.sleep(0)

    async def refresh(context, refresh_interval):
        return None

    readiness = MagicMock()
    config = SimpleNamespace(
        polling=SimpleNamespace(risk_check_interval=0.05),
        risk=SimpleNamespace(),
        execution=SimpleNamespace(),
    )

    with (
        patch(
            "services.risk.risk_monitor._pre_reconcile_for_context",
            side_effect=slow_pre_reconcile,
        ) as prerec,
        patch("services.risk.risk_monitor.run_risk_check_for_context", side_effect=check),
        patch("services.risk.risk_monitor._refresh_context_price_cache", side_effect=refresh),
        patch("services.risk.risk_monitor._exit_monitor_enabled", return_value=True),
    ):
        with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(
                risk_monitor_loop_for_contexts([ctx], readiness, config=config, interval=0.05),
                timeout=2.0,
            )

    # Pre-reconcile ran in its own phase...
    prerec.assert_awaited()
    # ...and every timed check was told NOT to repeat it.
    assert check_calls and all(flag is False for flag in check_calls)


@pytest.mark.asyncio
async def test_pre_reconcile_timeout_enters_backoff_without_stalling_risk_checks():
    """A stuck pre-reconcile must not burn the timeout every risk tick.

    The live failure mode was a log every ~6s:
    ``Risk pre-reconcile TIMEOUT ... after 5.0s``. That means each cycle waited
    for the same stuck venue reconciliation before evaluating protective exits.
    After the first timeout we back off that user's pre-reconcile attempts while
    still running the risk check phase every tick.
    """
    from services.risk.risk_monitor import risk_monitor_loop_for_contexts

    ctx = _ctx("u-timeout")
    check_calls: list[bool] = []

    async def stuck_pre_reconcile(context):
        await asyncio.sleep(100)
        return 0

    async def check(context, *, pre_reconcile: bool = True):
        check_calls.append(pre_reconcile)
        await asyncio.sleep(0)

    async def refresh(context, refresh_interval):
        return None

    readiness = MagicMock()
    config = SimpleNamespace(
        polling=SimpleNamespace(risk_check_interval=0.01),
        risk=SimpleNamespace(
            risk_pre_reconcile_timeout_seconds=0.05,
            risk_pre_reconcile_backoff_seconds=1.0,
        ),
        execution=SimpleNamespace(),
    )

    with (
        patch(
            "services.risk.risk_monitor._pre_reconcile_for_context",
            side_effect=stuck_pre_reconcile,
        ) as prerec,
        patch("services.risk.risk_monitor.run_risk_check_for_context", side_effect=check),
        patch("services.risk.risk_monitor._refresh_context_price_cache", side_effect=refresh),
        patch("services.risk.risk_monitor._exit_monitor_enabled", return_value=True),
    ):
        with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(
                risk_monitor_loop_for_contexts([ctx], readiness, config=config, interval=0.01),
                timeout=0.18,
            )

    assert prerec.await_count == 1
    assert len(check_calls) >= 2
    assert all(flag is False for flag in check_calls)
