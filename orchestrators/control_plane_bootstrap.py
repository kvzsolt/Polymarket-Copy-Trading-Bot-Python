"""Compose and own the optional production operator control plane.

Only application services backed by canonical runtime or PostgreSQL truth are
injected. Disconnected mutations remain absent, so aiohttp does not register
their routes and capability discovery cannot overstate what the process can
safely perform.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Protocol, cast

from config.app_config import AppConfig
from config.env import secret_value
from domain.detection import DetectionMode
from integrations.control_plane import (
    ALL_CONTROL_PLANE_PERMISSIONS,
    BearerCredential,
    ControlPlaneAuthenticator,
    ControlPlaneDependencies,
    ControlPlaneHttpConfig,
    ControlPlaneHttpServer,
    CopyTaskPayloadDecoder,
    READ_ONLY_CONTROL_PLANE_PERMISSIONS,
    TelemetryHubPort,
    TelemetryQueryPort,
)
from orchestrators.control_plane_reads import ControlPlaneRuntimeReads
from orchestrators.control_plane_runtime import (
    Clock,
    MonotonicClock,
    RuntimeStateQueryAdapter,
    RuntimeSystemQueryAdapter,
    WorkerStateReader,
)
from orchestrators.control_plane_settings import (
    AppConfigSettingsValidator,
    ConfigSemanticValidator,
)
from services.control_plane.commands import (
    DiscoveryCommandPort,
    DiscoveryCommandService,
    ReentryLockCommandService,
    RuntimeCommandService,
)
from services.control_plane.queries import (
    ActivityQueryService,
    CachedPortfolioQueryPort,
    DiscoveryQueryService,
    OrderQueryService,
    PositionQueryService,
    PortfolioQueryService,
    ReentryLockQueryService,
    ReservationQueryService,
    RuntimeQueryService,
    SystemQueryService,
    TaskQueryService,
)
from services.control_plane.queries_monitoring import (
    AccountQueryService,
    AccountScope,
    DetectionQueryService,
    LeaderQueryService,
    MetricsQueryService,
)
from services.control_plane.queries_trading import (
    FillQueryService,
    PnlQueryService,
    RiskQueryService,
)
from services.control_plane.runtime import (
    OpenOrderControlGateway,
    RuntimeTelemetry,
    TradingControlRuntimeSignal,
)
from services.control_plane.settings import (
    SettingsCommandService,
    SettingsQueryService,
    SettingsRepository,
)
from services.control_plane.tasks import (
    ActivationGuardedTaskBulkPort,
    TaskActivationGuard,
    TaskBulkCommandService,
    TaskCommandService,
)
from services.copy_tasks.service import CopyTaskService
from services.healthServer import ReadinessTracker
from storage.postgres.control_plane_audit_repository import (
    PostgresControlPlaneAuditRepository,
)
from storage.postgres.control_plane_entry_control_repository import (
    PostgresEntryControlRepository,
)
from storage.postgres.control_plane_query_repository import (
    PostgresControlPlaneQueryRepository,
)
from storage.postgres.control_plane_reentry_lock_repository import (
    PostgresControlPlaneReentryLockRepository,
)
from storage.postgres.control_plane_task_bulk_repository import (
    DatabasePool as TaskBulkDatabasePool,
    PostgresControlPlaneTaskBulkRepository,
)
from storage.postgres.copy_task_repository import PostgresCopyTaskRepository


class ControlPlaneServer(Protocol):
    """Own the lifecycle of one privileged network adapter."""

    async def start(self) -> None:
        """Start accepting control-plane requests."""

        ...

    async def close(self) -> None:
        """Stop accepting requests and close live streams."""

        ...


class ControlPlaneDatabasePool(Protocol):
    """Small shared PostgreSQL surface required by composed query adapters."""

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> Sequence[Mapping[str, object]]:
        """Return query rows."""

        ...

    async def fetchrow(
        self,
        query: str,
        *args: object,
    ) -> Mapping[str, object] | None:
        """Return one optional query row."""

        ...

    async def fetchval(self, query: str, *args: object) -> object | None:
        """Return the first scalar from an optional row."""

        ...


class ControlPlaneServerFactory(Protocol):
    """Build a transport server from already-composed application services."""

    def create(
        self,
        *,
        config: ControlPlaneHttpConfig,
        authenticator: ControlPlaneAuthenticator,
        dependencies: ControlPlaneDependencies,
    ) -> ControlPlaneServer:
        """Return one unstarted server lifecycle owner."""

        ...


class AiohttpControlPlaneServerFactory:
    """Create the repository-standard aiohttp control-plane server."""

    def create(
        self,
        *,
        config: ControlPlaneHttpConfig,
        authenticator: ControlPlaneAuthenticator,
        dependencies: ControlPlaneDependencies,
    ) -> ControlPlaneServer:
        """Return one unstarted aiohttp server.

        Args:
            config: Validated network and resource bounds.
            authenticator: Constant-time bearer authentication boundary.
            dependencies: Explicitly supported application capabilities.

        Returns:
            An idempotent aiohttp lifecycle owner.
        """

        return ControlPlaneHttpServer(
            config=config,
            authenticator=authenticator,
            dependencies=dependencies,
        )


class ControlPlaneRuntime:
    """Own server lifecycle and optional control-plane health evidence."""

    def __init__(
        self,
        *,
        server: ControlPlaneServer,
        readiness: ReadinessTracker,
    ) -> None:
        """Register an optional, initially-false readiness producer.

        Args:
            server: Privileged HTTP/WebSocket lifecycle owner.
            readiness: Existing process-wide readiness truth source.
        """

        self._server = server
        self._readiness = readiness
        self._started = False
        self._closed = False
        self._readiness.register_check(
            "control_plane_ok",
            required=False,
            ready=False,
            reason="not_started",
        )

    async def start(self) -> None:
        """Start the server once and publish optional readiness evidence.

        Raises:
            RuntimeError: If a closed runtime is restarted.
            Exception: Propagates a transport startup failure after recording
                an optional fail-closed health reason.
        """

        if self._closed:
            raise RuntimeError("closed control-plane runtime cannot be restarted")
        if self._started:
            return
        try:
            await self._server.start()
        except Exception:
            self._readiness.set_check(
                "control_plane_ok",
                ready=False,
                reason="startup_failed",
            )
            raise
        self._readiness.set_check("control_plane_ok", ready=True)
        self._started = True

    async def close(self) -> None:
        """Close the server once without changing trading readiness."""

        if self._closed:
            return
        self._closed = True
        try:
            await self._server.close()
        finally:
            self._readiness.set_check(
                "control_plane_ok",
                ready=False,
                reason="stopped",
            )
            self._started = False


class ControlPlaneRuntimeFactory:
    """Build the secure control plane from canonical application adapters."""

    def __init__(
        self,
        *,
        server_factory: ControlPlaneServerFactory,
        config_root: Path,
        version: str,
        clock: Clock,
        monotonic_clock: MonotonicClock,
        semantic_validator: ConfigSemanticValidator,
        started_monotonic: float,
    ) -> None:
        """Store infrastructure factories and deterministic runtime metadata.

        Args:
            server_factory: Privileged transport construction boundary.
            config_root: Trusted repository service-config directory.
            version: Current backend build version.
            clock: Aware UTC wall clock.
            monotonic_clock: Monotonic uptime clock.
            semantic_validator: Full application invariant validator.
            started_monotonic: Process composition start in monotonic seconds.
        """

        self._server_factory = server_factory
        self._config_root = config_root
        self._version = str(version)
        self._clock = clock
        self._monotonic_clock = monotonic_clock
        self._semantic_validator = semantic_validator
        self._started_monotonic = float(started_monotonic)

    def build(
        self,
        *,
        config: AppConfig,
        pool: ControlPlaneDatabasePool,
        readiness: ReadinessTracker,
        telemetry_query: TelemetryQueryPort,
        telemetry_hub: TelemetryHubPort,
        telemetry_service: RuntimeTelemetry,
        worker_state: WorkerStateReader,
        runtime_gateways: tuple[OpenOrderControlGateway, ...] = (),
        task_activation_guard: TaskActivationGuard | None = None,
        discovery_command_port: DiscoveryCommandPort | None = None,
        portfolio_cache: CachedPortfolioQueryPort | None = None,
        reads: ControlPlaneRuntimeReads | None = None,
    ) -> ControlPlaneRuntime | None:
        """Build an unstarted runtime or return ``None`` when disabled.

        Args:
            config: Immutable effective application configuration.
            pool: Shared asyncpg-compatible PostgreSQL pool.
            readiness: Existing process-wide readiness truth source.
            telemetry_query: Existing GUI-safe telemetry query service.
            telemetry_hub: Existing bounded telemetry event hub.
            telemetry_service: Existing non-blocking typed telemetry service.
            worker_state: Local low-cardinality copy-worker state reader.
            runtime_gateways: One canonical live gateway per follower account.
            task_activation_guard: Optional fail-closed task resume policy.
            discovery_command_port: Optional started asynchronous cycle queue.
            portfolio_cache: Optional local-only follower portfolio cache.
            reads: Optional local runtime state backing the extended reads.

        Returns:
            An unstarted runtime when enabled, otherwise ``None``.
        """

        if not config.control_plane.enabled:
            return None
        account_scope = (
            AccountScope(reads.accounts)
            if reads is not None and reads.accounts is not None
            else None
        )
        authenticator = self._authenticator(config)
        http_config = self._http_config(config)
        query_repository = PostgresControlPlaneQueryRepository(
            pool,
            clock=self._clock,
        )
        audit_repository = PostgresControlPlaneAuditRepository(pool)
        entry_control_repository = PostgresEntryControlRepository(pool)
        settings_validator = AppConfigSettingsValidator(
            config=config,
            environment=config.environment,
            semantic_validator=self._semantic_validator,
        )
        settings_repository = SettingsRepository(
            config_root=self._config_root,
            known_paths=settings_validator.known_paths(),
            validator=settings_validator,
            clock=self._clock,
        )
        system_adapter = RuntimeSystemQueryAdapter(
            readiness=readiness,
            service_name="pmcopy",
            version=self._version,
            environment=config.environment,
            mode=config.execution.trading_mode,
            backend="ccxt",
            capabilities=self._capabilities(
                config,
                task_commands_enabled=task_activation_guard is not None,
                discovery_commands_enabled=discovery_command_port is not None,
                portfolio_enabled=portfolio_cache is not None,
                reads=reads,
            ),
            active_task_counter=query_repository,
            clock=self._clock,
            monotonic_clock=self._monotonic_clock,
            started_monotonic=self._started_monotonic,
        )
        runtime_commands = None
        if config.execution.trading_mode == "live":
            runtime_commands = RuntimeCommandService(
                entry_control=entry_control_repository,
                runtime_signal=TradingControlRuntimeSignal(
                    gateways=runtime_gateways,
                    telemetry=telemetry_service,
                ),
                audit_sink=audit_repository,
                clock=self._clock,
            )
        task_commands = None
        task_bulk_commands = None
        task_decoder = None
        if task_activation_guard is not None:
            task_repository = PostgresCopyTaskRepository(pool)
            task_commands = TaskCommandService(
                task_service=CopyTaskService(task_repository),
                task_reader=task_repository,
                activation_guard=task_activation_guard,
                audit_sink=audit_repository,
                clock=self._clock,
            )
            task_bulk_commands = TaskBulkCommandService(
                port=ActivationGuardedTaskBulkPort(
                    port=PostgresControlPlaneTaskBulkRepository(cast(TaskBulkDatabasePool, pool)),
                    task_reader=task_repository,
                    activation_guard=task_activation_guard,
                ),
                audit_sink=audit_repository,
                clock=self._clock,
            )
            task_decoder = CopyTaskPayloadDecoder()
        dependencies = ControlPlaneDependencies(
            system=SystemQueryService(system_adapter),
            accounts=(AccountQueryService(account_scope) if account_scope is not None else None),
            detection=(
                DetectionQueryService(
                    counters=reads.detection_counters,  # type: ignore[arg-type]
                    readiness=reads.readiness,  # type: ignore[arg-type]
                    metrics=reads.metrics,  # type: ignore[arg-type]
                    detection_mode=config.detection.detection_mode.value,
                    pending_stats=reads.pending_stats,
                    clock=self._clock,
                )
                if reads is not None and reads.detection_supported
                else None
            ),
            fills=(
                FillQueryService(query_repository, scope=account_scope)
                if reads is not None
                else None
            ),
            pnl=(
                PnlQueryService(
                    pnl_trackers=reads.pnl_trackers,
                    positions=reads.positions,  # type: ignore[arg-type]
                    market_stores=reads.market_stores,
                    quote_max_age_seconds=(config.market_data.executable_quote_max_age_seconds),
                    clock=self._clock,
                )
                if reads is not None and reads.pnl_supported
                else None
            ),
            risk=(
                RiskQueryService(
                    port=query_repository,
                    portfolio=reads.portfolio,
                    config=config,
                    clock=self._clock,
                    day_start_equity=reads.day_start_equity,
                    scope=account_scope,
                )
                if reads is not None
                else None
            ),
            leaders=(
                LeaderQueryService(
                    roster=query_repository,
                    counts=query_repository,
                    configured=reads.configured_leaders,
                    monitored=reads.monitored_leaders or (lambda: ()),
                    counters=reads.detection_counters,  # type: ignore[arg-type]
                )
                if reads is not None and reads.leaders_supported
                else None
            ),
            metrics=(MetricsQueryService(telemetry_query) if reads is not None else None),
            tasks=TaskQueryService(query_repository),
            task_commands=task_commands,
            task_bulk_commands=task_bulk_commands,
            task_decoder=task_decoder,
            positions=PositionQueryService(query_repository),
            orders=OrderQueryService(query_repository),
            activity=ActivityQueryService(query_repository),
            discovery=DiscoveryQueryService(query_repository),
            discovery_commands=(
                DiscoveryCommandService(
                    port=discovery_command_port,
                    audit_sink=audit_repository,
                    clock=self._clock,
                )
                if discovery_command_port is not None
                else None
            ),
            locks=ReentryLockQueryService(query_repository),
            lock_commands=ReentryLockCommandService(
                port=PostgresControlPlaneReentryLockRepository(
                    pool,
                    clock=self._clock,
                ),
                audit_sink=audit_repository,
                clock=self._clock,
            ),
            reservations=ReservationQueryService(query_repository),
            portfolio=(
                PortfolioQueryService(portfolio_cache) if portfolio_cache is not None else None
            ),
            settings=SettingsQueryService(
                settings_repository,
                paths=tuple(settings_validator.known_paths()),
            ),
            settings_commands=SettingsCommandService(
                repository=settings_repository,
                audit_sink=audit_repository,
                clock=self._clock,
            ),
            runtime=RuntimeQueryService(
                RuntimeStateQueryAdapter(
                    entry_control=entry_control_repository,
                    worker_state=worker_state,
                    clock=self._clock,
                )
            ),
            runtime_commands=runtime_commands,
            telemetry_query=telemetry_query,
            telemetry_hub=telemetry_hub,
            settings_environment=config.environment,
        )
        server = self._server_factory.create(
            config=http_config,
            authenticator=authenticator,
            dependencies=dependencies,
        )
        return ControlPlaneRuntime(server=server, readiness=readiness)

    @staticmethod
    def _authenticator(config: AppConfig) -> ControlPlaneAuthenticator:
        """Build bearer or explicitly local unauthenticated policy.

        The operator credential always holds every permission. A second
        ``viewer`` credential is composed only when its own secret is set, and
        holds :data:`READ_ONLY_CONTROL_PLANE_PERMISSIONS` — every read plus the
        telemetry stream, and no mutation — so a dashboard can be given a
        credential that cannot pause, resume, or write anything. Configuration
        already rejects a viewer secret equal to the operator secret, so the
        two credentials can never collapse onto one identity here.
        """

        if config.control_plane.require_authentication:
            auth = config.control_plane.auth
            credentials = (
                BearerCredential(
                    token=secret_value(auth.token),
                    subject="operator",
                    permissions=ALL_CONTROL_PLANE_PERMISSIONS,
                ),
            )
            viewer_token = secret_value(auth.viewer_token)
            if viewer_token:
                credentials += (
                    BearerCredential(
                        token=viewer_token,
                        subject="viewer",
                        permissions=READ_ONLY_CONTROL_PLANE_PERMISSIONS,
                    ),
                )
            return ControlPlaneAuthenticator(credentials=credentials)
        return ControlPlaneAuthenticator(
            allow_unauthenticated_loopback=True,
            loopback_permissions=ALL_CONTROL_PLANE_PERMISSIONS,
        )

    @staticmethod
    def _http_config(config: AppConfig) -> ControlPlaneHttpConfig:
        """Project typed application config into the HTTP adapter contract."""

        source = config.control_plane
        return ControlPlaneHttpConfig(
            host=source.host,
            port=source.port,
            require_authentication=source.require_authentication,
            request_max_bytes=source.request_max_bytes,
            websocket_queue_capacity=source.websocket_queue_capacity,
            telemetry_snapshot_limit=source.telemetry_snapshot_limit,
            command_rate_limit=source.command_rate_limit,
            command_rate_window_seconds=source.command_rate_window_seconds,
            rate_limit_max_keys=source.rate_limit_max_keys,
            shutdown_timeout_seconds=source.shutdown_timeout_seconds,
            cors_allowed_origins=source.cors_allowed_origins,
        )

    @staticmethod
    def _capabilities(
        config: AppConfig,
        *,
        task_commands_enabled: bool,
        discovery_commands_enabled: bool,
        portfolio_enabled: bool,
        reads: ControlPlaneRuntimeReads | None = None,
    ) -> tuple[str, ...]:
        """Return sorted capabilities backed by this exact composition."""

        capabilities = {
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
        }
        if config.execution.trading_mode == "live":
            capabilities.update(
                {
                    "runtime_full_stop",
                    "runtime_pause_entries",
                    "runtime_resume_entries",
                }
            )
        if task_commands_enabled:
            capabilities.update(
                {
                    "task_create",
                    "task_bulk",
                    "task_delete",
                    "task_duplicate",
                    "task_pause",
                    "task_resume",
                    "task_update",
                }
            )
        if discovery_commands_enabled:
            capabilities.add("discovery_manual_run")
        if portfolio_enabled:
            capabilities.add("portfolio_read")
        if reads is not None:
            capabilities.update({"fill_read", "metrics_snapshot", "risk_read"})
            if reads.accounts is not None:
                capabilities.add("account_read")
            if reads.detection_supported:
                capabilities.add("detection_read")
            if reads.pnl_supported:
                capabilities.add("pnl_read")
            if reads.leaders_supported:
                capabilities.add("leader_read")
        detection_mode = config.detection.detection_mode
        if detection_mode is DetectionMode.ON_CHAIN_CONFIRMED:
            capabilities.add("on_chain_confirmed")
        if detection_mode is DetectionMode.ON_CHAIN_PENDING:
            capabilities.add("on_chain_pending")
        return tuple(sorted(capabilities))


__all__ = [
    "AiohttpControlPlaneServerFactory",
    "ControlPlaneDatabasePool",
    "ControlPlaneRuntime",
    "ControlPlaneRuntimeFactory",
    "ControlPlaneServer",
    "ControlPlaneServerFactory",
]
