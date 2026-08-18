"""Production control-plane composition and lifecycle tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

from config.app_config import (
    AppConfig,
    AppConfigSemanticValidator,
    ControlPlaneAuthConfig,
    ControlPlaneConfig,
    DatabaseConfig,
    DetectionConfig,
    ExecutionConfig,
    OnchainDetectionConfig,
    PolymarketConfig,
    WalletConfig,
)
from integrations.control_plane import (
    ControlPlaneAuthenticator,
    ControlPlaneDependencies,
    ControlPlaneHttpConfig,
)
import pytest
from pydantic import SecretStr
from services.healthServer import ReadinessTracker
from services.metrics import MetricsRegistry
from services.copy_tasks.domain import CopyTask
from services.control_plane.contracts import (
    DiscoveryRunCommand,
    DiscoveryRunReceipt,
    PortfolioView,
)
from services.telemetry import TelemetryHub, TelemetryQueryService, TelemetryService


NOW = datetime(2026, 8, 12, 14, 0, tzinfo=UTC)


class FixedClock:
    """Return one deterministic aware timestamp."""

    def now(self) -> datetime:
        """Return the fixed instant."""

        return NOW


class FixedMonotonicClock:
    """Return deterministic monotonic seconds."""

    def monotonic(self) -> float:
        """Return the fixed monotonic instant."""

        return 150.0


class FixedWorkerState:
    """Expose deterministic low-cardinality worker state."""

    @property
    def worker_count(self) -> int:
        """Return the fixed worker count."""

        return 2

    @property
    def queue_depth(self) -> int:
        """Return the fixed aggregate queue depth."""

        return 5


class AllowingTaskActivation:
    """Permit task activation for production-composition inspection."""

    def ensure_available(self, task: CopyTask) -> None:
        """Accept the supplied task."""

        del task


class AcceptingDiscoveryQueue:
    """Represent an already-started bounded discovery command port."""

    async def enqueue(self, command: DiscoveryRunCommand) -> DiscoveryRunReceipt:
        """Return one deterministic accepted receipt."""

        del command
        return DiscoveryRunReceipt(
            run_id="run-a",
            accepted=True,
            reason_code="accepted",
        )


class FixedPortfolioCache:
    """Return one already-refreshed local portfolio projection."""

    async def get_cached_portfolio(self, follower_account: str) -> PortfolioView | None:
        """Return a deterministic exact-decimal snapshot for the follower."""

        return PortfolioView(
            follower_account=follower_account,
            cash_usdc="10",
            reserved_usdc="1",
            positions_value_usdc="2",
            equity_usdc="12",
            observed_at=NOW,
            stale=False,
            copyable_balance_usdc="9",
            position_count=1,
        )


class FakePool:
    """Database marker whose methods must not run during composition."""

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> Sequence[Mapping[str, object]]:
        """Fail if composition unexpectedly performs a database query."""

        del query, args
        raise AssertionError("composition must not query PostgreSQL")

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Fail if composition unexpectedly performs a database query."""

        del query, args
        raise AssertionError("composition must not query PostgreSQL")

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Fail if composition unexpectedly performs a database query."""

        del query, args
        raise AssertionError("composition must not query PostgreSQL")


class FakeServer:
    """Record lifecycle calls without opening a network socket."""

    def __init__(self) -> None:
        self.start_count = 0
        self.close_count = 0

    async def start(self) -> None:
        """Record one start call."""

        self.start_count += 1

    async def close(self) -> None:
        """Record one close call."""

        self.close_count += 1


class RecordingServerFactory:
    """Capture concrete transport configuration and injected capabilities."""

    def __init__(self) -> None:
        self.config: ControlPlaneHttpConfig | None = None
        self.authenticator: ControlPlaneAuthenticator | None = None
        self.dependencies: ControlPlaneDependencies | None = None
        self.server = FakeServer()

    def create(
        self,
        *,
        config: ControlPlaneHttpConfig,
        authenticator: ControlPlaneAuthenticator,
        dependencies: ControlPlaneDependencies,
    ) -> FakeServer:
        """Capture composition and return the fake lifecycle owner."""

        self.config = config
        self.authenticator = authenticator
        self.dependencies = dependencies
        return self.server


def build_config(
    *,
    enabled: bool = True,
    trading_mode: str = "paper",
    detection: DetectionConfig | None = None,
    viewer_token: str = "",
) -> AppConfig:
    """Return a valid authenticated loopback control-plane configuration."""

    rpc_url = "https://polygon-rpc.example"
    return AppConfig(
        database=DatabaseConfig(dsn="postgresql://localhost/pmcopy"),
        detection=detection if detection is not None else DetectionConfig(),
        wallet=WalletConfig(
            private_key=SecretStr(""),
            chain_id=137,
            funder_address="0x1111111111111111111111111111111111111111",
            polygon_rpc_url=rpc_url,
        ),
        polymarket=PolymarketConfig(rpc_url=rpc_url),
        execution=ExecutionConfig(
            trading_mode=trading_mode,  # type: ignore[arg-type]
        ),
        control_plane=ControlPlaneConfig(
            enabled=enabled,
            host="127.0.0.1",
            port=9103,
            require_authentication=True,
            auth=ControlPlaneAuthConfig(
                token=SecretStr("operator-test-token"),
                viewer_token=SecretStr(viewer_token),
            ),
        ),
        environment=trading_mode,
        dry_run=trading_mode == "paper",
    )


def build_telemetry() -> tuple[TelemetryService, TelemetryQueryService, TelemetryHub]:
    """Build one in-memory telemetry truth for composition tests."""

    metrics = MetricsRegistry()
    hub = TelemetryHub(metrics=metrics)
    service = TelemetryService(hub=hub, metrics=metrics, clock=FixedClock())
    query = TelemetryQueryService(service=service, metrics=metrics)
    return service, query, hub


async def test_factory_composes_only_canonical_safe_capabilities(tmp_path: Path) -> None:
    """The initial production adapter omits every disconnected mutation path."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    _telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    readiness = ReadinessTracker()
    factory = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    )

    runtime = factory.build(
        config=build_config(),
        pool=FakePool(),
        readiness=readiness,
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=_telemetry_service,
        worker_state=FixedWorkerState(),
    )

    assert runtime is not None
    assert server_factory.config is not None
    assert server_factory.config.host == "127.0.0.1"
    assert server_factory.authenticator is not None
    assert server_factory.authenticator.has_credentials is True
    assert "operator-test-token" not in repr(server_factory.authenticator.credentials)
    dependencies = server_factory.dependencies
    assert dependencies is not None
    assert dependencies.system is not None
    assert dependencies.tasks is not None
    assert dependencies.positions is not None
    assert dependencies.orders is not None
    assert dependencies.activity is not None
    assert dependencies.discovery is not None
    assert dependencies.locks is not None
    assert dependencies.lock_commands is not None
    assert dependencies.reservations is not None
    assert dependencies.settings is not None
    assert dependencies.settings_commands is not None
    assert dependencies.telemetry_query is telemetry_query
    assert dependencies.telemetry_hub is telemetry_hub
    assert dependencies.task_commands is None
    assert dependencies.task_bulk_commands is None
    assert dependencies.discovery_commands is None
    assert dependencies.portfolio is None
    assert dependencies.runtime is not None
    assert dependencies.runtime_commands is None
    capabilities = await dependencies.system.get_capabilities()
    assert capabilities.capabilities == (
        "activity_read",
        "ccxt_backend",
        "discovery_read",
        "live_stream",
        "order_read",
        "position_read",
        "reentry_lock_read",
        "reentry_lock_reset",
        "reservation_read",
        "runtime_read",
        "settings_read",
        "settings_write",
        "task_read",
        "telemetry_snapshot",
    )


async def test_runtime_lifecycle_updates_optional_readiness_idempotently(tmp_path: Path) -> None:
    """Control-plane failure state stays separate from trading readiness."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    _telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    readiness = ReadinessTracker()
    runtime = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(),
        pool=FakePool(),
        readiness=readiness,
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=_telemetry_service,
        worker_state=FixedWorkerState(),
    )
    assert runtime is not None
    assert readiness.snapshot()["checks"]["control_plane_ok"] == {
        "ready": False,
        "required": False,
        "reason": "not_started",
    }

    await runtime.start()
    await runtime.start()
    assert server_factory.server.start_count == 1
    assert readiness.snapshot()["checks"]["control_plane_ok"]["ready"] is True

    await runtime.close()
    await runtime.close()
    assert server_factory.server.close_count == 1
    assert readiness.snapshot()["checks"]["control_plane_ok"] == {
        "ready": False,
        "required": False,
        "reason": "stopped",
    }


async def test_factory_advertises_portfolio_only_with_local_cache(tmp_path: Path) -> None:
    """Portfolio routes require an injected cache and perform no provider I/O."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    runtime = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(),
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=telemetry_service,
        worker_state=FixedWorkerState(),
        portfolio_cache=FixedPortfolioCache(),
    )

    assert runtime is not None
    dependencies = server_factory.dependencies
    assert dependencies is not None and dependencies.portfolio is not None
    snapshot = await dependencies.portfolio.get("user-a")
    assert snapshot.copyable_balance_usdc == "9"
    capabilities = await dependencies.system.get_capabilities()  # type: ignore[union-attr]
    assert "portfolio_read" in capabilities.capabilities


def test_disabled_control_plane_constructs_no_server_or_readiness_check(tmp_path: Path) -> None:
    """Disabled configuration has no socket, credential, or readiness side effect."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    server_factory = RecordingServerFactory()
    _telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    readiness = ReadinessTracker()
    runtime = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=tmp_path / "config",
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(enabled=False),
        pool=FakePool(),
        readiness=readiness,
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=_telemetry_service,
        worker_state=FixedWorkerState(),
    )

    assert runtime is None
    assert server_factory.dependencies is None
    assert "control_plane_ok" not in readiness.snapshot()["checks"]


async def test_live_factory_composes_canonical_runtime_controls(tmp_path: Path) -> None:
    """Advertise live safety commands only when execution consults the gate."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "live").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    runtime = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(trading_mode="live"),
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=telemetry_service,
        worker_state=FixedWorkerState(),
        runtime_gateways=(),
    )

    assert runtime is not None
    dependencies = server_factory.dependencies
    assert dependencies is not None
    assert dependencies.runtime is not None
    assert dependencies.runtime_commands is not None
    capabilities = await dependencies.system.get_capabilities()  # type: ignore[union-attr]
    assert "runtime_read" in capabilities.capabilities
    assert "runtime_pause_entries" in capabilities.capabilities
    assert "runtime_full_stop" in capabilities.capabilities


async def test_factory_composes_task_commands_only_with_runtime_activation_guard(
    tmp_path: Path,
) -> None:
    """Task writes exist only when resume can prove a live event money path."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    runtime = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(),
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=telemetry_service,
        worker_state=FixedWorkerState(),
        task_activation_guard=AllowingTaskActivation(),
    )

    assert runtime is not None
    dependencies = server_factory.dependencies
    assert dependencies is not None
    assert dependencies.task_commands is not None
    assert dependencies.task_decoder is not None
    assert dependencies.task_bulk_commands is not None
    capabilities = await dependencies.system.get_capabilities()  # type: ignore[union-attr]
    assert "task_create" in capabilities.capabilities
    assert "task_update" in capabilities.capabilities
    assert "task_pause" in capabilities.capabilities
    assert "task_resume" in capabilities.capabilities
    assert "task_duplicate" in capabilities.capabilities
    assert "task_delete" in capabilities.capabilities
    assert "task_bulk" in capabilities.capabilities


async def test_factory_composes_manual_discovery_only_with_started_queue(
    tmp_path: Path,
) -> None:
    """Discovery command capability reflects one injected asynchronous queue."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    runtime = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(),
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=telemetry_service,
        worker_state=FixedWorkerState(),
        discovery_command_port=AcceptingDiscoveryQueue(),
    )

    assert runtime is not None
    dependencies = server_factory.dependencies
    assert dependencies is not None
    assert dependencies.discovery_commands is not None
    capabilities = await dependencies.system.get_capabilities()  # type: ignore[union-attr]
    assert "discovery_manual_run" in capabilities.capabilities


@pytest.mark.parametrize(
    ("mode", "legacy_pending", "legacy_mined", "expected"),
    [
        ("polling", True, True, ()),
        ("on_chain_pending", False, False, ("on_chain_pending",)),
        ("on_chain_confirmed", False, False, ("on_chain_confirmed",)),
    ],
)
async def test_on_chain_capabilities_follow_detection_mode_not_legacy_flags(
    tmp_path: Path,
    mode: str,
    legacy_pending: bool,
    legacy_mined: bool,
    expected: tuple[str, ...],
) -> None:
    """Capabilities mirror the selected ``detection.mode``, never the booleans."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    _telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    factory = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    )
    detection = DetectionConfig(
        mode=mode,
        onchain=OnchainDetectionConfig(
            enable_onchain_pending_detection=legacy_pending,
            enable_onchain_mined_detection=legacy_mined,
            pending_tx_ws_url="wss://polygon.example.invalid/v2/redacted",
        ),
    )

    factory.build(
        config=build_config(detection=detection),
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=_telemetry_service,
        worker_state=FixedWorkerState(),
    )

    dependencies = server_factory.dependencies
    assert dependencies is not None
    capabilities = await dependencies.system.get_capabilities()  # type: ignore[union-attr]
    on_chain = tuple(
        capability
        for capability in capabilities.capabilities
        if capability in {"on_chain_pending", "on_chain_confirmed"}
    )
    assert on_chain == expected


class FixedReadinessSource:
    """Return one deterministic readiness snapshot."""

    def snapshot(self) -> dict[str, object]:
        """Return a minimal readiness projection."""

        return {"wss_market_ok": True, "wss_user_ok": True, "monitored_leader_count": 1}


async def test_factory_composes_extended_reads_only_when_injected(tmp_path: Path) -> None:
    """Detection, fill, PnL, risk, leader, and metrics reads are opt-in."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory
    from orchestrators.control_plane_reads import ControlPlaneRuntimeReads
    from services.control_plane.contracts_reads import AccountView
    from services.telemetry.emitters import DetectionTelemetryEmitter

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    metrics = MetricsRegistry()
    config = build_config()
    reads = ControlPlaneRuntimeReads(
        accounts=lambda: (
            AccountView(
                follower_account="account-a",
                wallet_address="0x1111111111111111111111111111111111111111",
                leader_count=1,
            ),
        ),
        detection_counters=DetectionTelemetryEmitter(None),
        readiness=FixedReadinessSource(),
        metrics=metrics,
    )
    runtime = ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=config,
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=telemetry_service,
        worker_state=FixedWorkerState(),
        reads=reads,
    )

    assert runtime is not None
    dependencies = server_factory.dependencies
    assert dependencies is not None
    assert dependencies.accounts is not None
    assert dependencies.detection is not None
    assert dependencies.fills is not None
    assert dependencies.leaders is not None
    assert dependencies.metrics is not None
    assert dependencies.risk is not None
    assert dependencies.pnl is None
    capabilities = await dependencies.system.get_capabilities()  # type: ignore[union-attr]
    assert {
        "account_read",
        "detection_read",
        "fill_read",
        "leader_read",
        "metrics_snapshot",
        "risk_read",
    } <= set(capabilities.capabilities)
    assert "pnl_read" not in capabilities.capabilities
    accounts = await dependencies.accounts.list()
    assert accounts[0].follower_account == "account-a"


async def test_factory_without_reads_keeps_the_existing_capability_surface(
    tmp_path: Path,
) -> None:
    """An unwired composition advertises no extended read capability."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(),
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=telemetry_service,
        worker_state=FixedWorkerState(),
    )

    dependencies = server_factory.dependencies
    assert dependencies is not None
    assert dependencies.accounts is None
    assert dependencies.detection is None
    assert dependencies.fills is None
    assert dependencies.leaders is None
    assert dependencies.metrics is None
    assert dependencies.pnl is None
    assert dependencies.risk is None
    capabilities = await dependencies.system.get_capabilities()  # type: ignore[union-attr]
    assert not {
        "account_read",
        "detection_read",
        "fill_read",
        "leader_read",
        "metrics_snapshot",
        "pnl_read",
        "risk_read",
    } & set(capabilities.capabilities)


async def test_viewer_credential_is_composed_only_when_its_secret_is_set(
    tmp_path: Path,
) -> None:
    """An unset viewer secret leaves exactly the single operator credential."""

    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(),
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=telemetry_service,
        worker_state=FixedWorkerState(),
    )

    assert server_factory.authenticator is not None
    credentials = server_factory.authenticator.credentials
    assert tuple(credential.subject for credential in credentials) == ("operator",)


async def test_viewer_credential_holds_every_read_and_no_write(tmp_path: Path) -> None:
    """A configured viewer secret adds a second, strictly read-only principal."""

    from integrations.control_plane import (
        ALL_CONTROL_PLANE_PERMISSIONS,
        ControlPlanePermission,
        READ_ONLY_CONTROL_PLANE_PERMISSIONS,
    )
    from orchestrators.control_plane_bootstrap import ControlPlaneRuntimeFactory

    config_root = tmp_path / "config"
    (config_root / "environments" / "paper").mkdir(parents=True)
    server_factory = RecordingServerFactory()
    telemetry_service, telemetry_query, telemetry_hub = build_telemetry()
    ControlPlaneRuntimeFactory(
        server_factory=server_factory,
        config_root=config_root,
        version="0.1.0",
        clock=FixedClock(),
        monotonic_clock=FixedMonotonicClock(),
        semantic_validator=AppConfigSemanticValidator(),
        started_monotonic=100.0,
    ).build(
        config=build_config(viewer_token="viewer-test-token"),
        pool=FakePool(),
        readiness=ReadinessTracker(),
        telemetry_query=telemetry_query,
        telemetry_hub=telemetry_hub,
        telemetry_service=telemetry_service,
        worker_state=FixedWorkerState(),
    )

    assert server_factory.authenticator is not None
    credentials = server_factory.authenticator.credentials
    by_subject = {credential.subject: credential for credential in credentials}
    assert set(by_subject) == {"operator", "viewer"}
    assert by_subject["operator"].permissions == ALL_CONTROL_PLANE_PERMISSIONS
    assert by_subject["viewer"].permissions == READ_ONLY_CONTROL_PLANE_PERMISSIONS
    writes = {
        permission
        for permission in ControlPlanePermission
        if not permission.value.endswith(".read")
        and permission is not ControlPlanePermission.TELEMETRY_STREAM
    }
    assert writes and not (writes & by_subject["viewer"].permissions)
    assert "viewer-test-token" not in repr(credentials)
