"""Bounded-restart policy for supervised aux loops (runtime-hardening).

``_on_supervised_crash`` is the pure decision function the main supervisor uses
when a risk / discovery / near-one / redemption loop crashes: restart up to
``max_restarts`` within a rolling window, then fail-closed.
"""
from __future__ import annotations

import asyncio

import app


def _spec(**kw):
    return app._Supervised(name="t", factory=lambda: asyncio.sleep(0), **kw)


def test_fast_repeated_crashes_fail_closed_at_budget():
    spec = _spec()
    spec.started = 1000.0  # crashes happen "now == started" → no recovery reset

    decisions = [app._on_supervised_crash(spec, 1000.0) for _ in range(5)]

    assert decisions == ["restart", "restart", "restart", "restart", "fail_closed"]
    assert spec.restarts == 5


def test_survival_past_recovery_window_resets_counter():
    spec = _spec()
    spec.started = 1000.0
    spec.restarts = 4  # nearly exhausted

    # Crash AFTER surviving >= recovery_seconds → counter resets, then counts 1.
    decision = app._on_supervised_crash(spec, 1000.0 + spec.recovery_seconds)

    assert decision == "restart"
    assert spec.restarts == 1


def test_custom_budget_is_respected():
    spec = _spec(max_restarts=2)
    spec.started = 0.0

    assert app._on_supervised_crash(spec, 0.0) == "restart"
    assert app._on_supervised_crash(spec, 0.0) == "fail_closed"


# --- P2-h: crashed leader monitor must be excluded from the live-count gauge ---


class _FakeTask:
    def __init__(self, done: bool):
        self._done = done

    def done(self) -> bool:
        return self._done


def test_live_monitor_count_excludes_done_tasks():
    tasks = [_FakeTask(done=False), _FakeTask(done=True), _FakeTask(done=False)]
    # One crashed/finished monitor (done=True) must not be counted.
    assert app._live_monitor_count(tasks) == 2


def test_live_monitor_count_zero_when_all_done():
    tasks = [_FakeTask(done=True), _FakeTask(done=True)]
    assert app._live_monitor_count(tasks) == 0


def test_live_monitor_count_empty_list():
    assert app._live_monitor_count([]) == 0
