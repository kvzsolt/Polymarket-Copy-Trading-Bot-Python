"""Local aiohttp contract tests for the control-plane route adapter."""

from __future__ import annotations

from datetime import datetime, timezone

from aiohttp.test_utils import TestClient, TestServer

from integrations.control_plane import (
    BearerCredential,
    ControlPlaneAuthenticator,
    CorrelationIdFactory,
    ControlPlaneDependencies,
    ControlPlaneHttpConfig,
    ControlPlaneHttpServer,
    ControlPlanePermission,
    ControlPlanePermissionPolicy,
    InProcessActionRateLimiter,
    create_control_plane_app,
)
from services.control_plane.contracts import (
    CapabilitiesView,
    ControlPlaneError,
    ControlPlaneReason,
    LockResetResult,
    LockResetOutcome,
    Page,
    PageRequest,
    ReentryLockView,
    ReadinessView,
    SettingsPath,
    SystemStatusView,
    SystemView,
    TaskVersionCommand,
    TaskView,
)

_NOW = datetime(2026, 8, 11, 14, 0, tzinfo=timezone.utc)
_TOKEN = "local-test-control-plane-token"


class _CorrelationIds:
    def new(self) -> str:
        return "correlation-0001"


class _UnsafeCorrelationIds:
    def new(self) -> str:
        return "unsafe\r\nheader"


class _MonotonicClock:
    def __init__(self) -> None:
        self.value = 10.0

    def monotonic(self) -> float:
        return self.value


class _SystemService:
    async def get(self) -> SystemView:
        return SystemView(
            service="pmcopy",
            version="1.0",
            environment="test",
            observed_at=_NOW,
        )

    async def get_status(self) -> SystemStatusView:
        return SystemStatusView(
            system=await self.get(),
            readiness=ReadinessView(
                live=True,
                ready=False,
                failed_checks=("wss_user",),
                observed_at=_NOW,
            ),
        )

    async def get_capabilities(self) -> CapabilitiesView:
        return CapabilitiesView(
            capabilities=("task.read", "task.write"),
            observed_at=_NOW,
        )


class _TaskQuery:
    def __init__(self) -> None:
        self.pages: list[tuple[str, PageRequest]] = []

    async def list(self, follower_account: str, page: PageRequest) -> Page[TaskView]:
        self.pages.append((follower_account, page))
        return Page(items=(_task(),), next_cursor=None)

    async def get(self, task_id: str) -> object:
        assert task_id == "task-a"
        return _task()


class _TaskCommands:
    def __init__(self) -> None:
        self.pauses: list[TaskVersionCommand] = []

    async def pause(self, command: TaskVersionCommand) -> TaskView:
        self.pauses.append(command)
        return _task(version=command.expected_version + 1, status="paused")


class _FailingTaskQuery(_TaskQuery):
    async def list(self, follower_account: str, page: PageRequest) -> Page[TaskView]:
        del follower_account, page
        raise ControlPlaneError(ControlPlaneReason.PORTFOLIO_UNAVAILABLE)


def _task(*, version: int = 1, status: str = "active") -> TaskView:
    return TaskView(
        task_id="task-a",
        name="Task A",
        leader_wallet="0x1111111111111111111111111111111111111111",
        follower_account="account-a",
        status=status,
        monitoring_mode="normal",
        sizing_mode="proportional",
        version=version,
        notifications_muted=False,
    )


def _auth(*permissions: ControlPlanePermission) -> ControlPlaneAuthenticator:
    return ControlPlaneAuthenticator(
        credentials=(
            BearerCredential(
                token=_TOKEN,
                subject="operator-a",
                permissions=frozenset(permissions),
            ),
        )
    )


async def _client(
    *,
    dependencies: ControlPlaneDependencies,
    permissions: tuple[ControlPlanePermission, ...],
    limiter: InProcessActionRateLimiter | None = None,
    cors_allowed_origins: tuple[str, ...] = (),
    correlation_ids: CorrelationIdFactory | None = None,
) -> TestClient:
    app = create_control_plane_app(
        config=ControlPlaneHttpConfig(
            host="127.0.0.1",
            port=0,
            cors_allowed_origins=cors_allowed_origins,
        ),
        authenticator=_auth(*permissions),
        permission_policy=ControlPlanePermissionPolicy(),
        dependencies=dependencies,
        rate_limiter=limiter,
        correlation_ids=_CorrelationIds() if correlation_ids is None else correlation_ids,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    return client


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_TOKEN}"}


async def test_server_runner_lifecycle_is_idempotent_and_loopback_default() -> None:
    config = ControlPlaneHttpConfig(port=0)
    server = ControlPlaneHttpServer(
        config=config,
        authenticator=_auth(ControlPlanePermission.SYSTEM_READ),
        dependencies=ControlPlaneDependencies(),
    )

    assert config.host == "127.0.0.1"
    assert server.running is False
    await server.start()
    assert server.running is True
    await server.start()
    await server.close()
    assert server.running is False
    await server.close()


class _AllCapabilities:
    def snapshot(self, *, limit: int = 100) -> dict[str, int]:
        return {"limit": limit}

    def decode(self, payload: object, *, task_id: str | None) -> object:
        del payload, task_id
        return object()

    def subscribe(self, *, capacity: int | None = None) -> object:
        del capacity
        return object()

    def unsubscribe(self, subscription: object) -> None:
        del subscription

    async def get(self, *args: object) -> object:
        del args
        return object()

    async def get_status(self) -> object:
        return object()

    async def get_capabilities(self) -> object:
        return object()

    async def list(self, *args: object) -> object:
        del args
        return object()

    async def list_candidates(self, *args: object) -> object:
        del args
        return object()

    async def list_roster(self, *args: object) -> object:
        del args
        return object()

    async def create(self, *args: object) -> object:
        del args
        return object()

    async def update(self, *args: object) -> object:
        del args
        return object()

    async def pause(self, *args: object) -> object:
        del args
        return object()

    async def resume(self, *args: object) -> object:
        del args
        return object()

    async def duplicate(self, *args: object) -> object:
        del args
        return object()

    async def delete(self, *args: object) -> object:
        del args
        return object()

    async def apply(self, *args: object) -> object:
        del args
        return object()

    async def request(self, *args: object) -> object:
        del args
        return object()

    async def reset(self, *args: object) -> object:
        del args
        return object()

    async def write(self, *args: object) -> object:
        del args
        return object()

    async def pause_entries(self, *args: object) -> object:
        del args
        return object()

    async def resume_entries(self, *args: object) -> object:
        del args
        return object()

    async def full_stop(self, *args: object) -> object:
        del args
        return object()


def test_complete_injected_capabilities_register_the_versioned_endpoint_matrix() -> None:
    service = _AllCapabilities()
    dependencies = ControlPlaneDependencies(
        system=service,
        tasks=service,
        task_commands=service,
        task_bulk_commands=service,
        task_decoder=service,
        positions=service,
        orders=service,
        activity=service,
        discovery=service,
        discovery_commands=service,
        locks=service,
        lock_commands=service,
        reservations=service,
        settings=service,
        settings_commands=service,
        portfolio=service,
        runtime=service,
        runtime_commands=service,
        telemetry_query=service,
        telemetry_hub=service,
        settings_environment="test",
    )
    app = create_control_plane_app(
        config=ControlPlaneHttpConfig(),
        authenticator=_auth(*ControlPlanePermission),
        dependencies=dependencies,
    )

    routes = {
        (route.method, route.resource.canonical)
        for route in app.router.routes()
        if route.method != "HEAD"
    }

    assert {
        ("GET", "/api/v1/system"),
        ("GET", "/api/v1/system/status"),
        ("GET", "/api/v1/system/readiness"),
        ("GET", "/api/v1/system/capabilities"),
        ("GET", "/api/v1/tasks"),
        ("POST", "/api/v1/tasks"),
        ("POST", "/api/v1/tasks/bulk"),
        ("GET", "/api/v1/tasks/{task_id}"),
        ("PUT", "/api/v1/tasks/{task_id}"),
        ("PATCH", "/api/v1/tasks/{task_id}"),
        ("DELETE", "/api/v1/tasks/{task_id}"),
        ("POST", "/api/v1/tasks/{task_id}/pause"),
        ("POST", "/api/v1/tasks/{task_id}/resume"),
        ("POST", "/api/v1/tasks/{task_id}/duplicate"),
        ("GET", "/api/v1/positions"),
        ("GET", "/api/v1/positions/{position_id}"),
        ("GET", "/api/v1/orders"),
        ("GET", "/api/v1/orders/{order_id}"),
        ("GET", "/api/v1/activity"),
        ("GET", "/api/v1/discovery/status"),
        ("GET", "/api/v1/discovery/candidates"),
        ("GET", "/api/v1/discovery/roster"),
        ("POST", "/api/v1/discovery/run"),
        ("GET", "/api/v1/locks"),
        ("POST", "/api/v1/locks/reset"),
        ("GET", "/api/v1/reentry-locks"),
        ("POST", "/api/v1/reentry-locks/{lock_id}/reset"),
        ("GET", "/api/v1/reservations"),
        ("GET", "/api/v1/settings"),
        ("GET", "/api/v1/settings/{section}"),
        ("PUT", "/api/v1/settings/{section}"),
        ("GET", "/api/v1/settings/{environment}/{section}"),
        ("PUT", "/api/v1/settings/{environment}/{section}"),
        ("GET", "/api/v1/portfolio"),
        ("GET", "/api/v1/runtime/safety"),
        ("POST", "/api/v1/runtime/safety/pause-entries"),
        ("POST", "/api/v1/runtime/safety/resume-entries"),
        ("POST", "/api/v1/runtime/safety/full-stop"),
        ("GET", "/api/v1/telemetry/snapshot"),
        ("GET", "/api/v1/stream"),
    } <= routes


async def test_authentication_permissions_correlation_and_no_wildcard_cors() -> None:
    client = await _client(
        dependencies=ControlPlaneDependencies(system=_SystemService()),
        permissions=(ControlPlanePermission.SYSTEM_READ,),
    )
    try:
        missing = await client.get("/api/v1/system")
        wrong = await client.get(
            "/api/v1/system",
            headers={"Authorization": "Bearer wrong"},
        )
        allowed = await client.get("/api/v1/system", headers=_headers())

        assert missing.status == 401
        assert (await missing.json())["error"] == {
            "code": "authentication_required",
            "message": "Authentication is required.",
            "correlation_id": "correlation-0001",
        }
        assert wrong.status == 401
        assert allowed.status == 200
        assert (await allowed.json())["data"]["observed_at"] == "2026-08-11T14:00:00Z"
        assert allowed.headers["X-Correlation-ID"] == "correlation-0001"
        assert "Access-Control-Allow-Origin" not in allowed.headers
        assert allowed.headers["Cache-Control"] == "no-store"
    finally:
        await client.close()


async def test_unsafe_injected_correlation_id_falls_back_without_header_injection() -> None:
    client = await _client(
        dependencies=ControlPlaneDependencies(system=_SystemService()),
        permissions=(ControlPlanePermission.SYSTEM_READ,),
        correlation_ids=_UnsafeCorrelationIds(),
    )
    try:
        response = await client.get("/api/v1/system", headers=_headers())

        assert response.status == 200
        correlation_id = response.headers["X-Correlation-ID"]
        assert len(correlation_id) == 32
        assert all(character in "0123456789abcdef" for character in correlation_id)
        assert (await response.json())["correlation_id"] == correlation_id
    finally:
        await client.close()


async def test_conditional_routes_do_not_publish_unsupported_commands() -> None:
    client = await _client(
        dependencies=ControlPlaneDependencies(system=_SystemService()),
        permissions=(
            ControlPlanePermission.SYSTEM_READ,
            ControlPlanePermission.TASK_WRITE,
        ),
    )
    try:
        unsupported = await client.post(
            "/api/v1/tasks/task-a/pause",
            headers=_headers(),
            json={"expected_version": 1},
        )
        assert unsupported.status == 404
        assert (await unsupported.json())["error"]["code"] == "not_found"
    finally:
        await client.close()


async def test_bounded_pagination_and_stable_application_error_mapping() -> None:
    task_query = _TaskQuery()
    client = await _client(
        dependencies=ControlPlaneDependencies(tasks=task_query),
        permissions=(ControlPlanePermission.TASK_READ,),
    )
    try:
        page = await client.get(
            "/api/v1/tasks?follower_account=account-a&limit=25&cursor=next",
            headers=_headers(),
        )
        invalid = await client.get(
            "/api/v1/tasks?follower_account=account-a&limit=101",
            headers=_headers(),
        )

        assert page.status == 200
        assert (await page.json())["data"]["items"][0]["task_id"] == "task-a"
        assert task_query.pages == [("account-a", PageRequest(limit=25, cursor="next"))]
        assert invalid.status == 400
        assert (await invalid.json())["error"]["code"] == "invalid_page"
        assert len(task_query.pages) == 1
    finally:
        await client.close()

    failing = await _client(
        dependencies=ControlPlaneDependencies(tasks=_FailingTaskQuery()),
        permissions=(ControlPlanePermission.TASK_READ,),
    )
    try:
        unavailable = await failing.get(
            "/api/v1/tasks?follower_account=account-a",
            headers=_headers(),
        )
        body = await unavailable.json()
        assert unavailable.status == 503
        assert body["error"]["code"] == "portfolio_unavailable"
        assert "repository" not in str(body).casefold()
    finally:
        await failing.close()


class _PositionOrderDetails:
    """Capture follower-scoped position and order detail identities."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def get(self, follower_account: str, resource_id: str) -> dict[str, str]:
        """Return one captured resource projection."""

        kind = "position" if resource_id == "token-a" else "order"
        self.calls.append((kind, follower_account, resource_id))
        return {"resource_id": resource_id, "follower_account": follower_account}


async def test_position_and_order_details_require_follower_scope() -> None:
    """Detail routes never expose another follower's durable records."""

    details = _PositionOrderDetails()
    client = await _client(
        dependencies=ControlPlaneDependencies(positions=details, orders=details),
        permissions=(
            ControlPlanePermission.POSITION_READ,
            ControlPlanePermission.ORDER_READ,
        ),
    )
    try:
        missing_scope = await client.get("/api/v1/positions/token-a", headers=_headers())
        position = await client.get(
            "/api/v1/positions/token-a?follower_account=account-a",
            headers=_headers(),
        )
        order = await client.get(
            "/api/v1/orders/intent-a?follower_account=account-a",
            headers=_headers(),
        )

        assert missing_scope.status == 400
        assert position.status == 200
        assert order.status == 200
        assert details.calls == [
            ("position", "account-a", "token-a"),
            ("order", "account-a", "intent-a"),
        ]
    finally:
        await client.close()


async def test_command_actor_is_authenticated_principal_and_rate_limited() -> None:
    commands = _TaskCommands()
    clock = _MonotonicClock()
    limiter = InProcessActionRateLimiter(
        max_actions=1,
        window_seconds=30,
        max_keys=32,
        clock=clock,
    )
    client = await _client(
        dependencies=ControlPlaneDependencies(task_commands=commands),
        permissions=(ControlPlanePermission.TASK_WRITE,),
        limiter=limiter,
    )
    try:
        first = await client.post(
            "/api/v1/tasks/task-a/pause",
            headers=_headers(),
            json={"expected_version": 4, "actor": "forged"},
        )
        second = await client.post(
            "/api/v1/tasks/task-a/pause",
            headers=_headers(),
            json={"expected_version": 5},
        )

        assert first.status == 200
        assert commands.pauses == [
            TaskVersionCommand(
                task_id="task-a",
                expected_version=4,
                actor="operator-a",
            )
        ]
        assert second.status == 429
        assert second.headers["Retry-After"] == "30"
        assert (await second.json())["error"]["code"] == "rate_limited"
    finally:
        await client.close()


async def test_readiness_and_capability_views_are_distinct() -> None:
    client = await _client(
        dependencies=ControlPlaneDependencies(system=_SystemService()),
        permissions=(ControlPlanePermission.SYSTEM_READ,),
    )
    try:
        status = await client.get("/api/v1/system/status", headers=_headers())
        readiness = await client.get("/api/v1/system/readiness", headers=_headers())
        capabilities = await client.get("/api/v1/system/capabilities", headers=_headers())

        assert (await status.json())["data"]["readiness"]["ready"] is False
        assert (await readiness.json())["data"]["failed_checks"] == ["wss_user"]
        assert (await capabilities.json())["data"]["capabilities"] == [
            "task.read",
            "task.write",
        ]
    finally:
        await client.close()


async def test_cors_is_exact_and_decorates_authorization_failures() -> None:
    origin = "https://ops.example"
    client = await _client(
        dependencies=ControlPlaneDependencies(system=_SystemService()),
        permissions=(ControlPlanePermission.SYSTEM_READ,),
        cors_allowed_origins=(origin,),
    )
    try:
        success = await client.get(
            "/api/v1/system",
            headers={**_headers(), "Origin": origin},
        )
        unauthorized = await client.get(
            "/api/v1/system",
            headers={"Origin": origin},
        )
        disallowed = await client.get(
            "/api/v1/system",
            headers={**_headers(), "Origin": "https://evil.example"},
        )
        preflight = await client.options(
            "/api/v1/system",
            headers={
                "Origin": origin,
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "authorization",
            },
        )

        assert success.headers["Access-Control-Allow-Origin"] == origin
        assert unauthorized.status == 401
        assert unauthorized.headers["Access-Control-Allow-Origin"] == origin
        assert disallowed.status == 403
        assert "Access-Control-Allow-Origin" not in disallowed.headers
        assert preflight.status == 204
        assert preflight.headers["Access-Control-Allow-Origin"] == origin
        assert preflight.headers["Vary"] == "Origin"
    finally:
        await client.close()


class _SettingsCommands:
    def __init__(self) -> None:
        self.calls: list[object] = []

    async def write(self, command: object) -> dict[str, bool]:
        self.calls.append(command)
        return {"restart_required": True, "live_applied": False}


class _BulkCommands:
    def __init__(self) -> None:
        self.calls: list[object] = []

    async def apply(self, command: object) -> dict[str, bool]:
        self.calls.append(command)
        return {"atomic": True}


class _SettingsService:
    """Capture current-environment list, read, and write delegation."""

    def __init__(self) -> None:
        self.reads: list[SettingsPath] = []
        self.writes: list[object] = []

    async def list(self) -> tuple[dict[str, str], ...]:
        """Return one bounded settings summary."""

        return ({"section": "risk"},)

    async def get(self, path: SettingsPath) -> dict[str, str]:
        """Capture one settings path."""

        self.reads.append(path)
        return {"section": path.section, "environment": path.environment}

    async def write(self, command: object) -> dict[str, bool]:
        """Capture one revisioned write command."""

        self.writes.append(command)
        return {"restart_required": True}


class _LockCommands:
    """Capture versioned lock reset delegation."""

    def __init__(self) -> None:
        self.calls: list[object] = []

    async def reset(self, command: object) -> LockResetResult:
        """Capture one command and return a deterministic reset."""

        self.calls.append(command)
        return LockResetResult(outcome=LockResetOutcome.RESET, released_at=_NOW)


async def test_current_environment_settings_aliases_delegate_typed_paths() -> None:
    """Frontend routes never accept an arbitrary environment or file path."""

    service = _SettingsService()
    client = await _client(
        dependencies=ControlPlaneDependencies(
            settings=service,
            settings_commands=service,
            settings_environment="paper",
        ),
        permissions=(
            ControlPlanePermission.SETTINGS_READ,
            ControlPlanePermission.SETTINGS_WRITE,
        ),
    )
    revision = "a" * 64
    try:
        listed = await client.get("/api/v1/settings", headers=_headers())
        read = await client.get("/api/v1/settings/risk", headers=_headers())
        written = await client.put(
            "/api/v1/settings/risk",
            headers=_headers(),
            json={"values": {"max_open_positions": 10}, "expected_revision": revision},
        )

        assert listed.status == 200
        assert (await listed.json())["data"] == [{"section": "risk"}]
        assert read.status == 200
        assert written.status == 200
        assert service.reads == [SettingsPath("paper", "risk")]
        assert len(service.writes) == 1
        assert service.writes[0].path == SettingsPath("paper", "risk")
    finally:
        await client.close()


async def test_reentry_lock_resource_id_must_match_reset_identity() -> None:
    """A path/body mismatch cannot reset a different protective lock."""

    commands = _LockCommands()
    client = await _client(
        dependencies=ControlPlaneDependencies(lock_commands=commands),
        permissions=(ControlPlanePermission.LOCK_RESET,),
    )
    follower = "account-a"
    task = "task-a"
    leader = "0x1111111111111111111111111111111111111111"
    market = "condition-a"
    lock_id = ReentryLockView.lock_id_for(
        follower_account=follower,
        task_id=task,
        leader_wallet=leader,
        market_id=market,
    )
    body = {
        "follower_account": follower,
        "task_id": task,
        "leader_wallet": leader,
        "market_id": market,
        "expected_revision": 2,
        "reason_code": "manual_review",
    }
    try:
        mismatch = await client.post(
            "/api/v1/reentry-locks/lock_wrong/reset",
            headers=_headers(),
            json=body,
        )
        valid = await client.post(
            f"/api/v1/reentry-locks/{lock_id}/reset",
            headers=_headers(),
            json=body,
        )

        assert mismatch.status == 400
        assert (await mismatch.json())["error"]["code"] == "invalid_request"
        assert valid.status == 200
        assert len(commands.calls) == 1
    finally:
        await client.close()


async def test_nonstandard_json_numbers_and_duplicate_keys_are_rejected() -> None:
    commands = _SettingsCommands()
    client = await _client(
        dependencies=ControlPlaneDependencies(settings_commands=commands),
        permissions=(ControlPlanePermission.SETTINGS_WRITE,),
    )
    try:
        nonfinite = await client.put(
            "/api/v1/settings/test/risk",
            headers={**_headers(), "Content-Type": "application/json"},
            data=(
                '{"values":{"limit":NaN},'
                '"expected_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
            ),
        )
        duplicate = await client.put(
            "/api/v1/settings/test/risk",
            headers={**_headers(), "Content-Type": "application/json"},
            data=(
                '{"values":{},'
                '"expected_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
                '"expected_revision":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}'
            ),
        )

        assert nonfinite.status == 400
        assert (await nonfinite.json())["error"]["code"] == "invalid_request"
        assert duplicate.status == 400
        assert (await duplicate.json())["error"]["code"] == "invalid_request"
        assert commands.calls == []
    finally:
        await client.close()


async def test_bulk_request_is_bounded_before_command_delegation() -> None:
    commands = _BulkCommands()
    client = await _client(
        dependencies=ControlPlaneDependencies(task_bulk_commands=commands),
        permissions=(ControlPlanePermission.TASK_WRITE,),
    )
    try:
        response = await client.post(
            "/api/v1/tasks/bulk",
            headers=_headers(),
            json={
                "items": [
                    {
                        "task_id": f"task-{index}",
                        "action": "pause",
                        "expected_version": 1,
                    }
                    for index in range(101)
                ]
            },
        )

        assert response.status == 400
        assert (await response.json())["error"]["code"] == "bulk_limit_exceeded"
        assert commands.calls == []
    finally:
        await client.close()
