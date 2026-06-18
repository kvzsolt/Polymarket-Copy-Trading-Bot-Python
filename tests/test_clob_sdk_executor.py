"""Regression: venue SDK calls must run on a bounded, isolated thread pool.

Root cause of the live "Risk pre-reconcile TIMEOUT" spam: every blocking py-clob
SDK call (``get_order``, ``get_fills``, book/tick lookups) ran via
``run_in_executor(None, ...)`` on Python's default pool (``min(32, cpu+4)`` ≈ 6
threads on a 2-core box), shared across all users. A few wedged 12s calls starved
the risk pre-reconcile ``get_order`` below its timeout ceiling.

``app.install_sdk_executor`` installs a sized, named pool as the loop's default
executor so those calls stop queueing behind unrelated SDK work.
"""
from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest


def test_sdk_executor_workers_defaults(monkeypatch):
    from app import _sdk_executor_workers, _DEFAULT_CLOB_SDK_EXECUTOR_WORKERS

    monkeypatch.delenv("CLOB_SDK_EXECUTOR_WORKERS", raising=False)
    assert _sdk_executor_workers() == _DEFAULT_CLOB_SDK_EXECUTOR_WORKERS


def test_sdk_executor_workers_reads_env(monkeypatch):
    from app import _sdk_executor_workers

    monkeypatch.setenv("CLOB_SDK_EXECUTOR_WORKERS", "20")
    assert _sdk_executor_workers() == 20


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1", 4),       # below floor -> clamped up
        ("4", 4),       # floor
        ("64", 64),     # ceiling
        ("999", 64),    # above ceiling -> clamped down
        ("nonsense", 12),  # invalid -> default
        ("", 12),       # blank -> default
    ],
)
def test_sdk_executor_workers_clamped(monkeypatch, raw, expected):
    from app import _sdk_executor_workers

    monkeypatch.setenv("CLOB_SDK_EXECUTOR_WORKERS", raw)
    assert _sdk_executor_workers() == expected


@pytest.mark.asyncio
async def test_install_sdk_executor_sets_named_default_executor():
    from app import install_sdk_executor

    loop = asyncio.get_running_loop()
    executor = install_sdk_executor(loop, 9)
    try:
        assert isinstance(executor, ThreadPoolExecutor)
        assert executor._max_workers == 9
        # run_in_executor(None, ...) must now resolve to OUR named pool — the
        # callable runs on a "clob-sdk" thread, proving the default was replaced.
        name = await loop.run_in_executor(None, lambda: threading.current_thread().name)
        assert name.startswith("clob-sdk"), name
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


@pytest.mark.asyncio
async def test_sized_pool_does_not_starve_a_fast_call():
    """With a sized pool, slow calls occupying N-1 threads must not block a fast
    call from running on the remaining thread (the anti-starvation property)."""
    from app import install_sdk_executor

    loop = asyncio.get_running_loop()
    workers = 4
    executor = install_sdk_executor(loop, workers)
    release = threading.Event()
    try:
        # Occupy workers-1 threads with blocking calls that wait on the event.
        slow = [
            loop.run_in_executor(None, release.wait) for _ in range(workers - 1)
        ]

        # A fast call submitted while the slow ones block must still complete
        # promptly on the one free worker.
        start = time.monotonic()
        result = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: "fast-done"), timeout=1.0
        )
        elapsed = time.monotonic() - start

        assert result == "fast-done"
        assert elapsed < 0.5, f"fast call was starved ({elapsed:.3f}s)"
        assert not all(f.done() for f in slow), "slow calls should still be blocked"
    finally:
        release.set()
        await asyncio.gather(*slow, return_exceptions=True)
        executor.shutdown(wait=False, cancel_futures=True)
