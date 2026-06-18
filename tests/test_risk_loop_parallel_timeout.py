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

    with patch(
        "services.risk.risk_monitor._pre_reconcile_for_context",
        side_effect=slow_pre_reconcile,
    ) as prerec, patch(
        "services.risk.risk_monitor.run_risk_check_for_context", side_effect=check
    ), patch(
        "services.risk.risk_monitor._refresh_context_price_cache", side_effect=refresh
    ), patch(
        "services.risk.risk_monitor._exit_monitor_enabled", return_value=True
    ):
        with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(
                risk_monitor_loop_for_contexts(
                    [ctx], readiness, config=config, interval=0.05
                ),
                timeout=2.0,
            )

    # Pre-reconcile ran in its own phase...
    prerec.assert_awaited()
    # ...and every timed check was told NOT to repeat it.
    assert check_calls and all(flag is False for flag in check_calls)


@pytest.mark.asyncio
async def test_pre_reconcile_timeout_enters_backoff_without_stalling_risk_checks(monkeypatch):
    """A stuck pre-reconcile must not burn the timeout every risk tick.

    The live failure mode was a log every ~6s:
    ``Risk pre-reconcile TIMEOUT ... after 5.0s``. That means each cycle waited
    for the same stuck venue reconciliation before evaluating protective exits.
    After the first timeout we back off that user's pre-reconcile attempts while
    still running the risk check phase every tick.
    """
    from services.risk.risk_monitor import risk_monitor_loop_for_contexts

    monkeypatch.setenv("RISK_PRE_RECONCILE_TIMEOUT_SECONDS", "0.05")
    monkeypatch.setenv("RISK_PRE_RECONCILE_TIMEOUT_BACKOFF_SECONDS", "1.0")

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
        risk=SimpleNamespace(),
        execution=SimpleNamespace(),
    )

    with patch(
        "services.risk.risk_monitor._pre_reconcile_for_context",
        side_effect=stuck_pre_reconcile,
    ) as prerec, patch(
        "services.risk.risk_monitor.run_risk_check_for_context", side_effect=check
    ), patch(
        "services.risk.risk_monitor._refresh_context_price_cache", side_effect=refresh
    ), patch(
        "services.risk.risk_monitor._exit_monitor_enabled", return_value=True
    ):
        with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(
                risk_monitor_loop_for_contexts(
                    [ctx], readiness, config=config, interval=0.01
                ),
                timeout=0.18,
            )

    assert prerec.await_count == 1
    assert len(check_calls) >= 2
    assert all(flag is False for flag in check_calls)


def test_pre_reconcile_ceiling_defaults_above_sdk_get_timeout(monkeypatch):
    """The default pre-reconcile ceiling must cover one healthy-but-slow venue GET.

    A single ``get_order`` is bounded by ``clob_v2_gateway._SDK_GET_TIMEOUT_SECONDS``
    (12.0s) + a transport retry. If the ceiling sat below that, one slow GET would
    trip the timeout before it even returned — the root cause of the live spam. With
    no env override and a small poll interval, the default must be >= the SDK GET wall.
    """
    from services.risk.risk_monitor import _risk_pre_reconcile_timeout_seconds
    from integrations.polymarket.clob_v2_gateway import _SDK_GET_TIMEOUT_SECONDS

    monkeypatch.delenv("RISK_PRE_RECONCILE_TIMEOUT_SECONDS", raising=False)

    ceiling = _risk_pre_reconcile_timeout_seconds(0.25)
    assert ceiling >= _SDK_GET_TIMEOUT_SECONDS, (
        f"default ceiling {ceiling}s must be >= SDK GET timeout "
        f"{_SDK_GET_TIMEOUT_SECONDS}s so one slow get_order cannot trip it"
    )
    assert ceiling >= 15.0


@pytest.mark.asyncio
async def test_pre_reconcile_timeout_logs_at_warning_not_error(monkeypatch):
    """The handled pre-reconcile timeout logs at WARNING (expected degradation),
    not ERROR — the backoff + standalone reconciler are the fail-safe.

    The module logger sets ``propagate=False`` and owns its handler, so caplog
    can't see it; patch the module ``logger`` and assert which level was used.
    """
    from services.risk import risk_monitor as rm

    monkeypatch.setenv("RISK_PRE_RECONCILE_TIMEOUT_SECONDS", "0.05")
    monkeypatch.setenv("RISK_PRE_RECONCILE_TIMEOUT_BACKOFF_SECONDS", "1.0")

    ctx = _ctx("u-warn")

    async def stuck_pre_reconcile(context):
        await asyncio.sleep(100)
        return 0

    async def check(context, *, pre_reconcile: bool = True):
        await asyncio.sleep(0)

    async def refresh(context, refresh_interval):
        return None

    readiness = MagicMock()
    config = SimpleNamespace(
        polling=SimpleNamespace(risk_check_interval=0.01),
        risk=SimpleNamespace(),
        execution=SimpleNamespace(),
    )

    mock_logger = MagicMock()
    with patch.object(rm, "logger", mock_logger), patch.object(
        rm, "_pre_reconcile_for_context", side_effect=stuck_pre_reconcile
    ), patch.object(
        rm, "run_risk_check_for_context", side_effect=check
    ), patch.object(
        rm, "_refresh_context_price_cache", side_effect=refresh
    ), patch.object(
        rm, "_exit_monitor_enabled", return_value=True
    ):
        with pytest.raises((asyncio.TimeoutError, asyncio.CancelledError)):
            await asyncio.wait_for(
                rm.risk_monitor_loop_for_contexts(
                    [ctx], readiness, config=config, interval=0.01
                ),
                timeout=0.18,
            )

    def _emitted(mock_call_list) -> list[str]:
        return [c.args[0] for c in mock_call_list if c.args]

    warning_msgs = _emitted(mock_logger.warning.call_args_list)
    error_msgs = _emitted(mock_logger.error.call_args_list)
    assert any("Risk pre-reconcile TIMEOUT" in m for m in warning_msgs), (
        "TIMEOUT must be logged at WARNING"
    )
    assert not any("Risk pre-reconcile TIMEOUT" in m for m in error_msgs), (
        "TIMEOUT must NOT be logged at ERROR — it is a handled degradation"
    )
