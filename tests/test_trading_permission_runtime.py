"""Deterministic orchestration tests for cached trading control."""

from __future__ import annotations

from collections.abc import Awaitable

import pytest

from orchestration.trading_permission_runtime import TradingPermissionRuntime
from services.trading_permission import TradingPausedError


class FakePermission:
    """Record off-path refresh attempts."""

    def __init__(self, *, error: Exception | None = None) -> None:
        self.calls = 0
        self._error = error

    async def refresh(self) -> object:
        """Record or fail one refresh."""

        self.calls += 1
        if self._error is not None:
            raise self._error
        return object()


class FakeSleeper:
    """Stop the periodic loop after observing its configured interval."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def sleep(self, delay_seconds: float) -> Awaitable[None]:
        """Record the delay and return an awaitable that cancels the loop."""

        self.delays.append(delay_seconds)

        async def stop() -> None:
            raise RuntimeError("stop-test-loop")

        return stop()


@pytest.mark.asyncio
async def test_initialize_refreshes_only_reconciled_users() -> None:
    """Users with failed startup reconciliation remain fail-closed."""

    allowed = FakePermission()
    blocked = FakePermission()
    runtime = TradingPermissionRuntime(
        permissions={"allowed": allowed, "blocked": blocked},
        sleeper=FakeSleeper(),
    )

    failed = await runtime.initialize(blocked_user_ids={"blocked"})

    assert failed == ()
    assert allowed.calls == 1
    assert blocked.calls == 0


@pytest.mark.asyncio
async def test_initialize_reports_control_refresh_failure() -> None:
    """An unavailable durable control state cannot enable live execution."""

    permission = FakePermission(error=TradingPausedError("unavailable"))
    runtime = TradingPermissionRuntime(
        permissions={"user": permission},
        sleeper=FakeSleeper(),
    )

    failed = await runtime.initialize(blocked_user_ids=set())

    assert failed == ("user",)


@pytest.mark.asyncio
async def test_periodic_refresh_runs_off_the_order_path() -> None:
    """The background runtime refreshes cached controls before waiting."""

    permission = FakePermission()
    sleeper = FakeSleeper()
    runtime = TradingPermissionRuntime(
        permissions={"user": permission},
        sleeper=sleeper,
    )

    with pytest.raises(RuntimeError, match="stop-test-loop"):
        await runtime.run(interval_seconds=10.0)

    assert permission.calls == 1
    assert sleeper.delays == [10.0]
