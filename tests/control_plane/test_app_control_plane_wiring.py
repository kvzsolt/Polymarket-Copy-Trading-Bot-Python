"""Application composition tests for the optional control-plane runtime."""

from __future__ import annotations

from types import SimpleNamespace


class FakeRuntime:
    """Record start and close lifecycle without opening sockets."""

    def __init__(self, *, fail_start: bool = False) -> None:
        self.fail_start = fail_start
        self.start_count = 0
        self.close_count = 0

    async def start(self) -> None:
        """Start or raise the configured transport failure."""

        self.start_count += 1
        if self.fail_start:
            raise OSError("sensitive bind detail")

    async def close(self) -> None:
        """Record deterministic outer-registry shutdown."""

        self.close_count += 1


class FakeFactory:
    """Capture application dependencies passed by the composition root."""

    def __init__(self, runtime: FakeRuntime | None) -> None:
        self.runtime = runtime
        self.calls: list[dict[str, object]] = []

    def build(self, **kwargs: object) -> FakeRuntime | None:
        """Capture dependencies and return the configured runtime."""

        self.calls.append(dict(kwargs))
        return self.runtime


async def test_control_plane_helper_registers_before_start_and_returns_runtime() -> None:
    """A started optional server is owned by the existing outer lifecycle."""

    from app import _start_control_plane_runtime

    runtime = FakeRuntime()
    factory = FakeFactory(runtime)
    registry: list[object] = []
    application = SimpleNamespace(config=object(), readiness=object())
    telemetry = SimpleNamespace(query=object(), hub=object(), service=object())
    pool = object()
    worker_state = object()
    runtime_gateways = (object(),)
    task_activation_guard = object()
    discovery_command_port = object()
    portfolio_cache = object()

    result = await _start_control_plane_runtime(
        application=application,
        pool=pool,
        telemetry=telemetry,
        worker_state=worker_state,  # type: ignore[arg-type]
        runtime_gateways=runtime_gateways,  # type: ignore[arg-type]
        task_activation_guard=task_activation_guard,  # type: ignore[arg-type]
        discovery_command_port=discovery_command_port,  # type: ignore[arg-type]
        portfolio_cache=portfolio_cache,  # type: ignore[arg-type]
        client_registry=registry,
        factory=factory,  # type: ignore[arg-type]
    )

    assert result is runtime
    assert registry == [runtime]
    assert runtime.start_count == 1
    assert factory.calls == [
        {
            "config": application.config,
            "pool": pool,
            "readiness": application.readiness,
            "telemetry_query": telemetry.query,
            "telemetry_hub": telemetry.hub,
            "telemetry_service": telemetry.service,
            "worker_state": worker_state,
            "runtime_gateways": runtime_gateways,
            "task_activation_guard": task_activation_guard,
            "discovery_command_port": discovery_command_port,
            "portfolio_cache": portfolio_cache,
        }
    ]


async def test_control_plane_helper_isolates_optional_transport_start_failure() -> None:
    """A failed privileged adapter remains closeable and cannot stop trading."""

    from app import _start_control_plane_runtime

    runtime = FakeRuntime(fail_start=True)
    factory = FakeFactory(runtime)
    registry: list[object] = []

    result = await _start_control_plane_runtime(
        application=SimpleNamespace(config=object(), readiness=object()),
        pool=object(),
        telemetry=SimpleNamespace(query=object(), hub=object(), service=object()),
        worker_state=object(),  # type: ignore[arg-type]
        runtime_gateways=(),
        task_activation_guard=None,
        discovery_command_port=None,
        portfolio_cache=None,
        client_registry=registry,
        factory=factory,  # type: ignore[arg-type]
    )

    assert result is runtime
    assert registry == [runtime]
    assert runtime.start_count == 1


async def test_control_plane_helper_has_no_resource_when_disabled() -> None:
    """A factory-declared disabled adapter performs no lifecycle mutation."""

    from app import _start_control_plane_runtime

    factory = FakeFactory(None)
    registry: list[object] = []

    result = await _start_control_plane_runtime(
        application=SimpleNamespace(config=object(), readiness=object()),
        pool=object(),
        telemetry=SimpleNamespace(query=object(), hub=object(), service=object()),
        worker_state=object(),  # type: ignore[arg-type]
        runtime_gateways=(),
        task_activation_guard=None,
        discovery_command_port=None,
        portfolio_cache=None,
        client_registry=registry,
        factory=factory,  # type: ignore[arg-type]
    )

    assert result is None
    assert registry == []
