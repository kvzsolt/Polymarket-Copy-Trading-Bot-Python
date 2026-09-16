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
    reads = object()

    result = await _start_control_plane_runtime(
        application=application,
        pool=pool,
        telemetry=telemetry,
        worker_state=worker_state,  # type: ignore[arg-type]
        runtime_gateways=runtime_gateways,  # type: ignore[arg-type]
        task_activation_guard=task_activation_guard,  # type: ignore[arg-type]
        discovery_command_port=discovery_command_port,  # type: ignore[arg-type]
        portfolio_cache=portfolio_cache,  # type: ignore[arg-type]
        reads=reads,  # type: ignore[arg-type]
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
            "reads": reads,
            "auto_traders": None,
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
        reads=None,
        client_registry=registry,
        factory=factory,  # type: ignore[arg-type]
    )

    assert result is runtime
    assert registry == [runtime]
    assert runtime.start_count == 1
    assert factory.calls[0]["reads"] is None


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


class FakeContext:
    """Minimal trading-context stand-in for read composition."""

    def __init__(self, user_id: str, wallet: str) -> None:
        self.user_id = user_id
        self.wallet_address = wallet
        self.pnl_tracker = object()
        self.positions_repo = object()


class FakeUserState:
    """Minimal user-runtime stand-in exposing one market store."""

    def __init__(self, market_store: object | None) -> None:
        self.market_store = market_store


async def test_reads_composition_maps_accounts_ledgers_and_market_stores() -> None:
    """Composition publishes exactly the follower state the process owns."""

    from app import _build_control_plane_reads
    from services.healthServer import ReadinessTracker
    from services.metrics import MetricsRegistry
    from services.telemetry.emitters import DetectionTelemetryEmitter

    context_a = FakeContext("account-a", "0xaaa")
    context_b = FakeContext("account-b", "0xbbb")
    store_a = object()
    monitored: list[str] = ["0xleader1"]
    contexts_by_leader: dict[str, list[FakeContext]] = {
        "0xLEADER1": [context_a, context_b],
        "0xleader2": [context_a],
        "0xleader3": [],
    }

    reads = _build_control_plane_reads(
        readiness=ReadinessTracker(),
        metrics_registry=MetricsRegistry(),
        contexts_by_leader=contexts_by_leader,
        risk_contexts_by_user={"account-a": context_a, "account-b": context_b},
        user_states={"account-a": FakeUserState(store_a), "account-b": FakeUserState(None)},
        configured_leaders=("0xLEADER1",),
        monitored_leaders=lambda: tuple(monitored),
        detection_counters=DetectionTelemetryEmitter(None),
        pending_stats=lambda: None,
        portfolio_cache=None,
    )

    assert reads.accounts is not None
    accounts = {view.follower_account: view for view in reads.accounts()}
    assert accounts["account-a"].leader_count == 2
    assert accounts["account-a"].wallet_address == "0xaaa"
    assert accounts["account-b"].leader_count == 1
    assert set(reads.pnl_trackers) == {"account-a", "account-b"}
    assert set(reads.market_stores) == {"account-a"}
    assert reads.configured_leaders == ("0xLEADER1",)
    assert list(reads.monitored_leaders()) == ["0xleader1"]  # type: ignore[misc]
    assert reads.detection_supported is True
    assert reads.pnl_supported is True
    assert reads.leaders_supported is True
    assert reads.positions is not None
    assert await reads.positions.list_open_positions("account-unknown") == ()

    monitored.append("0xleader2")
    assert list(reads.monitored_leaders()) == ["0xleader1", "0xleader2"]  # type: ignore[misc]

    # A leader bound after composition must change the next account read.
    contexts_by_leader["0xleader4"] = [context_b]
    refreshed = {view.follower_account: view for view in reads.accounts()}
    assert refreshed["account-b"].leader_count == 2
    assert refreshed["account-a"].leader_count == 2


def test_detection_connection_probe_reports_unknown_without_runtime_state() -> None:
    """A runtime with no connection state never reads as disconnected."""

    from app import _detection_connection_probe

    class _Probed:
        connected = False

    assert _detection_connection_probe(_Probed())() is False
    assert _detection_connection_probe(object())() is None
    assert _detection_connection_probe(None)() is None


async def test_automatic_runtime_exposes_its_saved_evidence_and_promotion_service():
    """The production HTTP composition receives the running account-scoped service."""
    from app import _start_control_plane_runtime
    from orchestrators.auto_trader_runtime import AutoTraderRuntime
    from services.control_plane.auto_traders import AutoTraderControlService

    factory = FakeFactory(None)
    application = SimpleNamespace(
        config=object(), readiness=object(), auto_traders=AutoTraderRuntime(services={})
    )
    await _start_control_plane_runtime(
        application=application,
        pool=object(),
        telemetry=SimpleNamespace(query=object(), hub=object(), service=object()),
        worker_state=object(),
        runtime_gateways=(),
        task_activation_guard=None,
        discovery_command_port=None,
        client_registry=[],
        factory=factory,
    )
    assert isinstance(factory.calls[0]["auto_traders"], AutoTraderControlService)
