"""WP-B regression tests: ``app_lifecycle`` OOP owners and partial-startup teardown."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import pmcopy.app as app_module
from pmcopy.orchestrators.lifecycle.app_lifecycle import (
    CallableCloseResource,
    HealthServerResource,
    ManagedResource,
    RuntimeResourceStack,
    RuntimeStreamCoordinator,
    SupervisedTaskSpec,
    TaskSupervisor,
)
from pmcopy.services.execution.circuit_breaker import CircuitBreaker
from pmcopy.services.observability.health_server import ReadinessTracker
from pmcopy.services.observability.metrics import MetricsRegistry


@pytest.mark.asyncio
async def test_runtime_resource_stack_closes_lifo_once_and_isolates_errors() -> None:
    """A failing resource close must not prevent earlier resources from closing once."""
    events: list[str] = []

    class _CustomResource(ManagedResource):
        resource_name = "first"

        async def aclose(self) -> None:
            events.append("first")

    async def _explode() -> None:
        events.append("second_boom")
        raise RuntimeError("boom")

    runner = SimpleNamespace(cleanup=AsyncMock(side_effect=lambda: events.append("health")))
    health = HealthServerResource(runner=runner)

    stack = RuntimeResourceStack()
    first = stack.push(_CustomResource())
    stack.push(first)  # duplicate registration ignored
    stack.push_client(SimpleNamespace(close=_explode), name="exploder")
    stack.push(health)

    await stack.aclose()
    await stack.aclose()  # idempotent second drain

    assert events == ["health", "second_boom", "first"]
    assert isinstance(CallableCloseResource(lambda: None), ManagedResource)


@pytest.mark.asyncio
async def test_partial_wss_startup_populates_runtimes_out_for_shutdown(monkeypatch) -> None:
    """When user 2 fails WSS startup, user 1's started runtime is recorded and stopped."""
    monkeypatch.setattr("pmcopy.storage.postgres.connection.shutdown_pool", AsyncMock())
    monkeypatch.setattr("pmcopy.utils.fetch_data.close_session", AsyncMock())

    stopped: list[str] = []

    class _FakeMarketRuntime:
        def __init__(self, *, readiness, **_kwargs) -> None:
            self._user_id = readiness._account

        async def start(self, _tokens) -> None:
            if self._user_id == "user-b":
                raise RuntimeError("user-b market ws failed")

        async def stop(self) -> None:
            stopped.append(self._user_id)

    users = [SimpleNamespace(user_id="user-a"), SimpleNamespace(user_id="user-b")]
    states = {
        "user-a": SimpleNamespace(market_store=object(), clob_gateway=object()),
        "user-b": SimpleNamespace(market_store=object(), clob_gateway=object()),
    }
    market_runtimes: dict[str, object] = {}
    coord = RuntimeStreamCoordinator()

    try:
        with pytest.raises(RuntimeError, match="user-b market ws failed"):
            await coord.start_market_data_runtimes(
                runtime_users=users,  # type: ignore[arg-type]
                user_states=states,  # type: ignore[arg-type]
                live_positions_by_user={},
                contexts_by_leader={},
                readiness=ReadinessTracker(),
                config=SimpleNamespace(),
                market_runtime_cls=_FakeMarketRuntime,
                runtimes_out=market_runtimes,  # type: ignore[arg-type]
            )
    finally:
        await app_module._shutdown_runtime(
            monitor_tasks=[],
            supervised=[],
            market_data_runtimes=market_runtimes,  # type: ignore[arg-type]
            user_data_runtimes=[],
            metrics_runner=None,
        )

    assert stopped == ["user-a"]


@pytest.mark.asyncio
async def test_task_supervisor_restarts_then_fails_closed_when_budget_exhausted() -> None:
    """Crashed supervised loop restarts with backoff until ``max_restarts`` triggers exit 1."""
    attempts = 0

    async def _crashing_loop() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError(f"crash-{attempts}")

    spec = SupervisedTaskSpec(
        name="test-loop",
        factory=_crashing_loop,
        max_restarts=2,
        recovery_seconds=60.0,
    )
    readiness = ReadinessTracker()
    supervisor = TaskSupervisor(
        supervised=[spec],
        monitor_tasks=[],
        monitor_wallet_by_task={},
        readiness=readiness,
        refresh_interval=0.05,
        metrics_registry=MetricsRegistry(),
        sleep_fn=AsyncMock(),
    )
    supervisor.spawn_all()

    exit_code = await supervisor.run_until_stopped()

    assert exit_code == 1
    assert attempts == 2
    assert readiness.snapshot()["supervisor_ok"] is False


def test_venue_circuit_breaker_pushes_state_into_readiness() -> None:
    """State transitions on the shared venue breaker propagate to ``ReadinessTracker``."""
    readiness = ReadinessTracker()
    readiness.update(circuit_breaker_state="closed")
    cfg = SimpleNamespace(execution=SimpleNamespace(circuit_failure_threshold=2))
    breaker = CircuitBreaker.for_venue_config(
        cfg,
        cooldown_seconds=0.0,
        on_state_change=lambda st: readiness.update(circuit_breaker_state=st.value),
    )

    breaker.record_failure()
    assert readiness.snapshot()["circuit_breaker_state"] == "closed"
    breaker.record_failure()
    assert readiness.snapshot()["circuit_breaker_state"] == "open"
    breaker.allow_request()  # 0s cooldown transitions OPEN -> HALF_OPEN
    assert readiness.snapshot()["circuit_breaker_state"] == "half_open"
    breaker.record_success()
    assert readiness.snapshot()["circuit_breaker_state"] == "closed"
