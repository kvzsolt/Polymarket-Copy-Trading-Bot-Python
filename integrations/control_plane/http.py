"""Secure aiohttp adapter for the injected operator control-plane services."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import AsyncIterator, Mapping
from http import HTTPStatus
from typing import Any

from aiohttp import WSMsgType, WSCloseCode, web

from services.control_plane.contracts import (
    BulkTaskCommand,
    BulkTaskItem,
    ControlPlaneError,
    ControlPlaneReason,
    DiscoveryRunCommand,
    LockResetCommand,
    PageRequest,
    ReentryLockView,
    RuntimeControlCommand,
    SettingsPath,
    SettingsWriteCommand,
    TaskAction,
    TaskCreateCommand,
    TaskDuplicateCommand,
    TaskUpdateCommand,
    TaskVersionCommand,
)

from .contracts import (
    ControlPlaneDependencies,
    CorrelationIdFactory,
    SecureCorrelationIdFactory,
)
from .security import (
    ControlPlaneAuthenticator,
    ControlPlaneHttpConfig,
    ControlPlanePermission,
    ControlPlanePermissionPolicy,
    ControlPlanePrincipal,
    ControlPlaneTransportError,
    InProcessActionRateLimiter,
    is_loopback_host,
)
from .serialization import ControlPlaneErrorMapper, ControlPlaneJsonSerializer


API_PREFIX = "/api/v1"

_CORRELATION_ID_KEY = web.RequestKey("control_plane_correlation_id", str)
_PRINCIPAL_KEY = web.RequestKey("control_plane_principal", ControlPlanePrincipal)
_ADAPTER_KEY = web.AppKey("control_plane_adapter", object)
_SAFE_CORRELATION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CORS_METHODS = "DELETE, GET, PATCH, POST, PUT"
_CORS_HEADERS = "Authorization, Content-Type, X-Correlation-ID"


class ControlPlaneHttpApplication:
    """Own route registration and thin handler delegation for one app."""

    def __init__(
        self,
        *,
        config: ControlPlaneHttpConfig,
        authenticator: ControlPlaneAuthenticator,
        dependencies: ControlPlaneDependencies,
        permission_policy: ControlPlanePermissionPolicy | None = None,
        rate_limiter: InProcessActionRateLimiter | None = None,
        serializer: ControlPlaneJsonSerializer | None = None,
        error_mapper: ControlPlaneErrorMapper | None = None,
        correlation_ids: CorrelationIdFactory | None = None,
    ) -> None:
        _validate_bind_auth(config, authenticator)
        self._config = config
        self._authenticator = authenticator
        self._dependencies = dependencies
        self._permission_policy = (
            permission_policy if permission_policy is not None else ControlPlanePermissionPolicy()
        )
        self._rate_limiter = (
            rate_limiter
            if rate_limiter is not None
            else InProcessActionRateLimiter(
                max_actions=config.command_rate_limit,
                window_seconds=config.command_rate_window_seconds,
                max_keys=config.rate_limit_max_keys,
            )
        )
        self._serializer = serializer if serializer is not None else ControlPlaneJsonSerializer()
        self._error_mapper = (
            error_mapper if error_mapper is not None else ControlPlaneErrorMapper(self._serializer)
        )
        self._correlation_ids = (
            correlation_ids if correlation_ids is not None else SecureCorrelationIdFactory()
        )
        self._websockets: set[web.WebSocketResponse] = set()
        self._shutting_down = False
        self.app = web.Application(
            middlewares=(
                self._error_middleware,
                self._cors_middleware,
                self._authentication_middleware,
            ),
            client_max_size=config.request_max_bytes,
        )
        self.app[_ADAPTER_KEY] = self
        self._register_routes()
        self.app.on_shutdown.append(self._on_shutdown)
        self.app.cleanup_ctx.append(self._cleanup_context)

    # ------------------------------------------------------------------
    # Middleware and response envelope
    # ------------------------------------------------------------------
    @web.middleware
    async def _error_middleware(
        self,
        request: web.Request,
        handler: Any,
    ) -> web.StreamResponse:
        try:
            correlation_id = self._correlation_id(request)
        except Exception:
            correlation_id = SecureCorrelationIdFactory().new()
        request[_CORRELATION_ID_KEY] = correlation_id
        try:
            response = await handler(request)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            response = self._error_mapper.response(error, correlation_id=correlation_id)
        if not response.prepared:
            self._apply_cors_headers(request, response)
            self._apply_security_headers(response, correlation_id=correlation_id)
        return response

    @web.middleware
    async def _cors_middleware(
        self,
        request: web.Request,
        handler: Any,
    ) -> web.StreamResponse:
        origin = request.headers.get("Origin")
        if origin is not None and origin not in self._config.cors_allowed_origins:
            raise ControlPlaneTransportError(
                code="origin_not_allowed",
                public_message="The request origin is not allowed.",
                status=HTTPStatus.FORBIDDEN,
            )
        if request.method == "OPTIONS" and request.path.startswith(API_PREFIX):
            response = web.Response(status=HTTPStatus.NO_CONTENT)
        else:
            response = await handler(request)
        if not response.prepared:
            self._apply_cors_headers(request, response)
        return response

    def _apply_cors_headers(
        self,
        request: web.Request,
        response: web.StreamResponse,
    ) -> None:
        origin = request.headers.get("Origin")
        if origin is None or origin not in self._config.cors_allowed_origins:
            return
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Methods"] = _CORS_METHODS
        response.headers["Access-Control-Allow-Headers"] = _CORS_HEADERS
        response.headers["Vary"] = "Origin"

    @web.middleware
    async def _authentication_middleware(
        self,
        request: web.Request,
        handler: Any,
    ) -> web.StreamResponse:
        if request.method == "OPTIONS":
            return await handler(request)
        principal = self._authenticator.authenticate_header(
            request.headers.get("Authorization"),
            remote_address=request.remote,
        )
        request[_PRINCIPAL_KEY] = principal
        return await handler(request)

    def _correlation_id(self, request: web.Request) -> str:
        proposed = request.headers.get("X-Correlation-ID")
        if proposed is not None and _SAFE_CORRELATION.fullmatch(proposed):
            return proposed
        generated = self._correlation_ids.new()
        if not isinstance(generated, str) or not _SAFE_CORRELATION.fullmatch(generated):
            raise RuntimeError("correlation id factory returned an unsafe value")
        return generated

    @staticmethod
    def _apply_security_headers(response: web.StreamResponse, *, correlation_id: str) -> None:
        response.headers.setdefault("X-Correlation-ID", correlation_id)
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")

    def _websocket_headers(self, request: web.Request) -> dict[str, str]:
        headers = {
            "X-Correlation-ID": request[_CORRELATION_ID_KEY],
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
        }
        origin = request.headers.get("Origin")
        if origin is not None:
            headers["Access-Control-Allow-Origin"] = origin
            headers["Vary"] = "Origin"
        return headers

    def _ok(
        self,
        request: web.Request,
        value: object,
        *,
        status: int = HTTPStatus.OK,
    ) -> web.Response:
        return web.json_response(
            {
                "data": self._serializer.to_jsonable(value),
                "correlation_id": request[_CORRELATION_ID_KEY],
            },
            status=int(status),
        )

    def _authorize(
        self,
        request: web.Request,
        permission: ControlPlanePermission,
        *,
        action: str | None = None,
    ) -> ControlPlanePrincipal:
        principal = request[_PRINCIPAL_KEY]
        self._permission_policy.require(principal, permission)
        if action is not None:
            decision = self._rate_limiter.check(f"{principal.subject}:{action}")
            if not decision.allowed:
                raise ControlPlaneTransportError(
                    code="rate_limited",
                    public_message="Too many control-plane actions were requested.",
                    status=HTTPStatus.TOO_MANY_REQUESTS,
                    headers={"Retry-After": str(decision.retry_after_seconds)},
                )
        return principal

    # ------------------------------------------------------------------
    # Route registration: capability is exactly the injected callable.
    # ------------------------------------------------------------------
    def _register_routes(self) -> None:
        router = self.app.router
        system = self._dependencies.system
        if _supports(system, "get"):
            router.add_get(f"{API_PREFIX}/system", self._get_system)
        if _supports(system, "get_status"):
            router.add_get(f"{API_PREFIX}/status", self._get_system_status)
            router.add_get(f"{API_PREFIX}/readiness", self._get_readiness)
            router.add_get(f"{API_PREFIX}/system/status", self._get_system_status)
            router.add_get(f"{API_PREFIX}/system/readiness", self._get_readiness)
        if _supports(system, "get_capabilities"):
            router.add_get(f"{API_PREFIX}/capabilities", self._get_capabilities)
            router.add_get(f"{API_PREFIX}/system/capabilities", self._get_capabilities)

        tasks = self._dependencies.tasks
        task_commands = self._dependencies.task_commands
        if _supports(tasks, "list"):
            router.add_get(f"{API_PREFIX}/tasks", self._list_tasks)
        if _supports(task_commands, "create") and self._dependencies.task_decoder is not None:
            router.add_post(f"{API_PREFIX}/tasks", self._create_task)
        if _supports(self._dependencies.task_bulk_commands, "apply"):
            router.add_post(f"{API_PREFIX}/tasks/bulk", self._bulk_tasks)
        if _supports(tasks, "get"):
            router.add_get(f"{API_PREFIX}/tasks/{{task_id}}", self._get_task)
        if _supports(task_commands, "update") and self._dependencies.task_decoder is not None:
            router.add_put(f"{API_PREFIX}/tasks/{{task_id}}", self._update_task)
            router.add_patch(f"{API_PREFIX}/tasks/{{task_id}}", self._update_task)
        if _supports(task_commands, "delete"):
            router.add_delete(f"{API_PREFIX}/tasks/{{task_id}}", self._delete_task)
        if _supports(task_commands, "pause"):
            router.add_post(f"{API_PREFIX}/tasks/{{task_id}}/pause", self._pause_task)
        if _supports(task_commands, "resume"):
            router.add_post(f"{API_PREFIX}/tasks/{{task_id}}/resume", self._resume_task)
        if _supports(task_commands, "duplicate"):
            router.add_post(f"{API_PREFIX}/tasks/{{task_id}}/duplicate", self._duplicate_task)

        positions = self._dependencies.positions
        if _supports(positions, "list"):
            router.add_get(f"{API_PREFIX}/positions", self._list_positions)
        if _supports(positions, "get"):
            router.add_get(
                f"{API_PREFIX}/positions/{{position_id}}",
                self._get_position,
            )
        orders = self._dependencies.orders
        if _supports(orders, "list"):
            router.add_get(f"{API_PREFIX}/orders", self._list_orders)
        if _supports(orders, "get"):
            router.add_get(f"{API_PREFIX}/orders/{{order_id}}", self._get_order)
        if _supports(self._dependencies.activity, "list"):
            router.add_get(f"{API_PREFIX}/activity", self._list_activity)

        discovery = self._dependencies.discovery
        if _supports(discovery, "list"):
            router.add_get(f"{API_PREFIX}/discovery/cycles", self._list_discovery_cycles)
        if _supports(discovery, "get_status"):
            router.add_get(f"{API_PREFIX}/discovery/status", self._get_discovery_status)
        if _supports(discovery, "list_candidates"):
            router.add_get(f"{API_PREFIX}/discovery/candidates", self._list_discovery_candidates)
        if _supports(discovery, "list_roster"):
            router.add_get(f"{API_PREFIX}/discovery/roster", self._list_discovery_roster)
        if _supports(self._dependencies.discovery_commands, "request"):
            router.add_post(f"{API_PREFIX}/discovery/run", self._run_discovery)

        if _supports(self._dependencies.locks, "list"):
            router.add_get(f"{API_PREFIX}/locks", self._list_locks)
            router.add_get(f"{API_PREFIX}/reentry-locks", self._list_locks)
        if _supports(self._dependencies.lock_commands, "reset"):
            router.add_post(f"{API_PREFIX}/locks/reset", self._reset_lock)
            router.add_post(
                f"{API_PREFIX}/reentry-locks/{{lock_id}}/reset",
                self._reset_lock,
            )
        if _supports(self._dependencies.reservations, "list"):
            router.add_get(f"{API_PREFIX}/reservations", self._list_reservations)

        settings = self._dependencies.settings
        if _supports(settings, "list"):
            router.add_get(f"{API_PREFIX}/settings", self._list_settings)
        if _supports(settings, "get"):
            router.add_get(
                f"{API_PREFIX}/settings/{{environment}}/{{section}}",
                self._get_settings,
            )
            if self._dependencies.settings_environment is not None:
                router.add_get(
                    f"{API_PREFIX}/settings/{{section}}",
                    self._get_settings,
                )
        if _supports(self._dependencies.settings_commands, "write"):
            router.add_put(
                f"{API_PREFIX}/settings/{{environment}}/{{section}}",
                self._put_settings,
            )
            if self._dependencies.settings_environment is not None:
                router.add_put(
                    f"{API_PREFIX}/settings/{{section}}",
                    self._put_settings,
                )

        if _supports(self._dependencies.portfolio, "get"):
            router.add_get(f"{API_PREFIX}/portfolio", self._get_portfolio)
        if _supports(self._dependencies.runtime, "get"):
            router.add_get(f"{API_PREFIX}/runtime", self._get_runtime)
            router.add_get(f"{API_PREFIX}/runtime/safety", self._get_runtime)
        runtime_commands = self._dependencies.runtime_commands
        if _supports(runtime_commands, "pause_entries"):
            router.add_post(f"{API_PREFIX}/runtime/pause", self._pause_runtime)
            router.add_post(
                f"{API_PREFIX}/runtime/safety/pause-entries",
                self._pause_runtime,
            )
        if _supports(runtime_commands, "resume_entries"):
            router.add_post(f"{API_PREFIX}/runtime/resume", self._resume_runtime)
            router.add_post(
                f"{API_PREFIX}/runtime/safety/resume-entries",
                self._resume_runtime,
            )
        if _supports(runtime_commands, "full_stop"):
            router.add_post(f"{API_PREFIX}/runtime/full-stop", self._full_stop_runtime)
            router.add_post(
                f"{API_PREFIX}/runtime/safety/full-stop",
                self._full_stop_runtime,
            )

        if _supports(self._dependencies.accounts, "list"):
            router.add_get(f"{API_PREFIX}/accounts", self._list_accounts)
        if _supports(self._dependencies.detection, "get"):
            router.add_get(f"{API_PREFIX}/detection", self._get_detection)
        if _supports(self._dependencies.fills, "list"):
            router.add_get(f"{API_PREFIX}/fills", self._list_fills)
        if _supports(self._dependencies.pnl, "get"):
            router.add_get(f"{API_PREFIX}/pnl", self._get_pnl)
        if _supports(self._dependencies.risk, "get"):
            router.add_get(f"{API_PREFIX}/risk", self._get_risk)
        if _supports(self._dependencies.leaders, "list"):
            router.add_get(f"{API_PREFIX}/leaders", self._list_leaders)
        if _supports(self._dependencies.metrics, "snapshot"):
            router.add_get(f"{API_PREFIX}/metrics/snapshot", self._get_metrics_snapshot)

        if _supports(self._dependencies.telemetry_query, "snapshot"):
            router.add_get(f"{API_PREFIX}/telemetry", self._get_telemetry)
            router.add_get(f"{API_PREFIX}/telemetry/snapshot", self._get_telemetry)
        if self._dependencies.telemetry_hub is not None:
            router.add_get(f"{API_PREFIX}/stream", self._stream_telemetry)

    # ------------------------------------------------------------------
    # System and query handlers
    # ------------------------------------------------------------------
    async def _get_system(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.SYSTEM_READ)
        return self._ok(request, await self._dependencies.system.get())  # type: ignore[union-attr]

    async def _get_system_status(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.SYSTEM_READ)
        return self._ok(request, await self._dependencies.system.get_status())  # type: ignore[union-attr]

    async def _get_readiness(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.SYSTEM_READ)
        status = await self._dependencies.system.get_status()  # type: ignore[union-attr]
        return self._ok(request, status.readiness)

    async def _get_capabilities(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.SYSTEM_READ)
        return self._ok(
            request,
            await self._dependencies.system.get_capabilities(),  # type: ignore[union-attr]
        )

    async def _list_tasks(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.TASK_READ)
        return self._ok(
            request,
            await self._dependencies.tasks.list(  # type: ignore[union-attr]
                _required_query(request, "follower_account"),
                _page(request),
            ),
        )

    async def _get_task(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.TASK_READ)
        return self._ok(
            request,
            await self._dependencies.tasks.get(  # type: ignore[union-attr]
                _required_path(request, "task_id")
            ),
        )

    async def _list_positions(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.POSITION_READ)
        return self._ok(
            request,
            await self._dependencies.positions.list(  # type: ignore[union-attr]
                _required_query(request, "follower_account"), _page(request)
            ),
        )

    async def _list_orders(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.ORDER_READ)
        return self._ok(
            request,
            await self._dependencies.orders.list(  # type: ignore[union-attr]
                _required_query(request, "follower_account"), _page(request)
            ),
        )

    async def _get_position(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.POSITION_READ)
        return self._ok(
            request,
            await self._dependencies.positions.get(  # type: ignore[union-attr]
                _required_query(request, "follower_account"),
                _required_path(request, "position_id"),
            ),
        )

    async def _get_order(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.ORDER_READ)
        return self._ok(
            request,
            await self._dependencies.orders.get(  # type: ignore[union-attr]
                _required_query(request, "follower_account"),
                _required_path(request, "order_id"),
            ),
        )

    async def _list_activity(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.ACTIVITY_READ)
        follower = request.query.get("follower_account")
        if follower is not None:
            follower = _required_text(follower)
        return self._ok(
            request,
            await self._dependencies.activity.list(follower, _page(request)),  # type: ignore[union-attr]
        )

    async def _list_discovery_cycles(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.DISCOVERY_READ)
        return self._ok(
            request,
            await self._dependencies.discovery.list(_page(request)),  # type: ignore[union-attr]
        )

    async def _get_discovery_status(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.DISCOVERY_READ)
        return self._ok(
            request,
            await self._dependencies.discovery.get_status(),  # type: ignore[union-attr]
        )

    async def _list_discovery_candidates(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.DISCOVERY_READ)
        return self._ok(
            request,
            await self._dependencies.discovery.list_candidates(_page(request)),  # type: ignore[union-attr]
        )

    async def _list_discovery_roster(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.DISCOVERY_READ)
        return self._ok(
            request,
            await self._dependencies.discovery.list_roster(_page(request)),  # type: ignore[union-attr]
        )

    async def _list_locks(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.LOCK_READ)
        return self._ok(
            request,
            await self._dependencies.locks.list(  # type: ignore[union-attr]
                _required_query(request, "follower_account"), _page(request)
            ),
        )

    async def _list_reservations(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.RESERVATION_READ)
        return self._ok(
            request,
            await self._dependencies.reservations.list(  # type: ignore[union-attr]
                _required_query(request, "follower_account"), _page(request)
            ),
        )

    async def _get_settings(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.SETTINGS_READ)
        path = self._settings_path(request)
        return self._ok(
            request,
            await self._dependencies.settings.get(path),  # type: ignore[union-attr]
        )

    async def _list_settings(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.SETTINGS_READ)
        return self._ok(
            request,
            await self._dependencies.settings.list(),  # type: ignore[union-attr]
        )

    async def _get_portfolio(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.PORTFOLIO_READ)
        return self._ok(
            request,
            await self._dependencies.portfolio.get(  # type: ignore[union-attr]
                _required_query(request, "follower_account")
            ),
        )

    async def _get_runtime(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.RUNTIME_READ)
        return self._ok(request, await self._dependencies.runtime.get())  # type: ignore[union-attr]

    async def _list_accounts(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.SYSTEM_READ)
        return self._ok(request, await self._dependencies.accounts.list())  # type: ignore[union-attr]

    async def _get_detection(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.RUNTIME_READ)
        return self._ok(request, await self._dependencies.detection.get())  # type: ignore[union-attr]

    async def _list_fills(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.FILL_READ)
        return self._ok(
            request,
            await self._dependencies.fills.list(  # type: ignore[union-attr]
                _required_query(request, "follower_account"), _page(request)
            ),
        )

    async def _get_pnl(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.PORTFOLIO_READ)
        return self._ok(
            request,
            await self._dependencies.pnl.get(  # type: ignore[union-attr]
                _required_query(request, "follower_account")
            ),
        )

    async def _get_risk(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.RISK_READ)
        return self._ok(
            request,
            await self._dependencies.risk.get(  # type: ignore[union-attr]
                _required_query(request, "follower_account")
            ),
        )

    async def _list_leaders(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.LEADER_READ)
        return self._ok(request, await self._dependencies.leaders.list())  # type: ignore[union-attr]

    async def _get_metrics_snapshot(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.TELEMETRY_READ)
        return self._ok(request, self._dependencies.metrics.snapshot())  # type: ignore[union-attr]

    async def _get_telemetry(self, request: web.Request) -> web.Response:
        self._authorize(request, ControlPlanePermission.TELEMETRY_READ)
        raw_limit = request.query.get("limit", str(self._config.telemetry_snapshot_limit))
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE) from None
        if not 0 <= limit <= 1000:
            raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE)
        return self._ok(
            request,
            self._dependencies.telemetry_query.snapshot(limit=limit),  # type: ignore[union-attr]
        )

    # ------------------------------------------------------------------
    # Command handlers
    # ------------------------------------------------------------------
    async def _create_task(self, request: web.Request) -> web.Response:
        principal = self._authorize(
            request, ControlPlanePermission.TASK_WRITE, action="task.create"
        )
        body = await self._json_body(request)
        task_payload = _optional_mapping(body.get("task"), fallback=body)
        task = self._dependencies.task_decoder.decode(task_payload, task_id=None)  # type: ignore[union-attr]
        result = await self._dependencies.task_commands.create(  # type: ignore[union-attr]
            TaskCreateCommand(task=task, actor=principal.subject)
        )
        return self._ok(request, result, status=HTTPStatus.CREATED)

    async def _update_task(self, request: web.Request) -> web.Response:
        principal = self._authorize(
            request, ControlPlanePermission.TASK_WRITE, action="task.update"
        )
        task_id = _required_path(request, "task_id")
        body = await self._json_body(request)
        task_payload = _required_mapping(body.get("task"))
        task = self._dependencies.task_decoder.decode(task_payload, task_id=task_id)  # type: ignore[union-attr]
        if task.task_id != task_id:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        result = await self._dependencies.task_commands.update(  # type: ignore[union-attr]
            TaskUpdateCommand(
                task=task,
                expected_version=_required_int(body.get("expected_version")),
                actor=principal.subject,
            )
        )
        return self._ok(request, result)

    async def _task_version_action(
        self,
        request: web.Request,
        *,
        method_name: str,
        action: str,
    ) -> web.Response:
        principal = self._authorize(request, ControlPlanePermission.TASK_WRITE, action=action)
        body = await self._json_body(request)
        command = TaskVersionCommand(
            task_id=_required_path(request, "task_id"),
            expected_version=_required_int(body.get("expected_version")),
            actor=principal.subject,
        )
        method = getattr(self._dependencies.task_commands, method_name)
        return self._ok(request, await method(command))

    async def _pause_task(self, request: web.Request) -> web.Response:
        return await self._task_version_action(request, method_name="pause", action="task.pause")

    async def _resume_task(self, request: web.Request) -> web.Response:
        return await self._task_version_action(request, method_name="resume", action="task.resume")

    async def _delete_task(self, request: web.Request) -> web.Response:
        return await self._task_version_action(request, method_name="delete", action="task.delete")

    async def _duplicate_task(self, request: web.Request) -> web.Response:
        principal = self._authorize(
            request, ControlPlanePermission.TASK_WRITE, action="task.duplicate"
        )
        body = await self._json_body(request)
        result = await self._dependencies.task_commands.duplicate(  # type: ignore[union-attr]
            TaskDuplicateCommand(
                source_task_id=_required_path(request, "task_id"),
                new_task_id=_required_text(body.get("new_task_id")),
                new_name=_required_text(body.get("new_name")),
                actor=principal.subject,
            )
        )
        return self._ok(request, result, status=HTTPStatus.CREATED)

    async def _bulk_tasks(self, request: web.Request) -> web.Response:
        principal = self._authorize(request, ControlPlanePermission.TASK_WRITE, action="task.bulk")
        body = await self._json_body(request)
        raw_items = body.get("items")
        if not isinstance(raw_items, list):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        if not 1 <= len(raw_items) <= 100:
            raise ControlPlaneError(ControlPlaneReason.BULK_LIMIT_EXCEEDED)
        items: list[BulkTaskItem] = []
        for raw_item in raw_items:
            item = _required_mapping(raw_item)
            try:
                action = TaskAction(_required_text(item.get("action")))
            except ValueError:
                raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST) from None
            items.append(
                BulkTaskItem(
                    task_id=_required_text(item.get("task_id")),
                    action=action,
                    expected_version=_required_int(item.get("expected_version")),
                )
            )
        result = await self._dependencies.task_bulk_commands.apply(  # type: ignore[union-attr]
            BulkTaskCommand(items=tuple(items), actor=principal.subject)
        )
        return self._ok(request, result)

    async def _run_discovery(self, request: web.Request) -> web.Response:
        principal = self._authorize(
            request, ControlPlanePermission.DISCOVERY_RUN, action="discovery.run"
        )
        body = await self._json_body(request, required=False)
        force = body.get("force", False)
        if not isinstance(force, bool):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        result = await self._dependencies.discovery_commands.request(  # type: ignore[union-attr]
            DiscoveryRunCommand(actor=principal.subject, force=force)
        )
        return self._ok(request, result, status=HTTPStatus.ACCEPTED)

    async def _reset_lock(self, request: web.Request) -> web.Response:
        principal = self._authorize(request, ControlPlanePermission.LOCK_RESET, action="lock.reset")
        body = await self._json_body(request)
        follower_account = _required_text(body.get("follower_account"))
        task_id = _required_text(body.get("task_id"))
        leader_wallet = _required_text(body.get("leader_wallet"))
        market_id = _required_text(body.get("market_id"))
        resource_id = request.match_info.get("lock_id")
        if resource_id is not None and resource_id != ReentryLockView.lock_id_for(
            follower_account=follower_account,
            task_id=task_id,
            leader_wallet=leader_wallet,
            market_id=market_id,
        ):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        result = await self._dependencies.lock_commands.reset(  # type: ignore[union-attr]
            LockResetCommand(
                follower_account=follower_account,
                task_id=task_id,
                leader_wallet=leader_wallet,
                market_id=market_id,
                expected_revision=_required_int(body.get("expected_revision")),
                actor=principal.subject,
                reason_code=_required_text(body.get("reason_code")),
            )
        )
        return self._ok(request, result)

    async def _put_settings(self, request: web.Request) -> web.Response:
        principal = self._authorize(
            request, ControlPlanePermission.SETTINGS_WRITE, action="settings.write"
        )
        body = await self._json_body(request)
        path = self._settings_path(request)
        command = SettingsWriteCommand.from_mapping(
            path=path,
            values=_required_mapping(body.get("values")),
            expected_revision=_required_text(body.get("expected_revision")),
            actor=principal.subject,
        )
        return self._ok(
            request,
            await self._dependencies.settings_commands.write(command),  # type: ignore[union-attr]
        )

    def _settings_path(self, request: web.Request) -> SettingsPath:
        """Return an explicit or composed-current environment settings path."""

        environment = request.match_info.get("environment")
        if environment is None:
            environment = self._dependencies.settings_environment
        if environment is None:
            raise ControlPlaneError(ControlPlaneReason.SETTINGS_PATH_FORBIDDEN)
        return SettingsPath(
            environment=environment,
            section=_required_path(request, "section"),
        )

    async def _runtime_action(
        self,
        request: web.Request,
        *,
        method_name: str,
        action: str,
    ) -> web.Response:
        principal = self._authorize(request, ControlPlanePermission.RUNTIME_WRITE, action=action)
        body = await self._json_body(request)
        command = RuntimeControlCommand(
            expected_revision=_required_int(body.get("expected_revision")),
            actor=principal.subject,
            reason_code=_required_text(body.get("reason_code")),
        )
        method = getattr(self._dependencies.runtime_commands, method_name)
        return self._ok(request, await method(command))

    async def _pause_runtime(self, request: web.Request) -> web.Response:
        return await self._runtime_action(
            request, method_name="pause_entries", action="runtime.pause"
        )

    async def _resume_runtime(self, request: web.Request) -> web.Response:
        return await self._runtime_action(
            request, method_name="resume_entries", action="runtime.resume"
        )

    async def _full_stop_runtime(self, request: web.Request) -> web.Response:
        return await self._runtime_action(
            request, method_name="full_stop", action="runtime.full_stop"
        )

    # ------------------------------------------------------------------
    # Authenticated telemetry stream and lifecycle
    # ------------------------------------------------------------------
    async def _stream_telemetry(self, request: web.Request) -> web.StreamResponse:
        principal = self._authorize(request, ControlPlanePermission.TELEMETRY_STREAM)
        if self._shutting_down:
            raise ControlPlaneTransportError(
                code="telemetry_unavailable",
                public_message="The telemetry stream is unavailable.",
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
        hub = self._dependencies.telemetry_hub
        if hub is None:
            raise web.HTTPNotFound()
        try:
            subscription = hub.subscribe(capacity=self._config.websocket_queue_capacity)
        except Exception:
            raise ControlPlaneTransportError(
                code="telemetry_unavailable",
                public_message="The telemetry stream is unavailable.",
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            ) from None

        ws = web.WebSocketResponse(
            heartbeat=30.0,
            autoping=True,
            max_msg_size=4096,
        )
        ws.headers.update(self._websocket_headers(request))
        try:
            await ws.prepare(request)
        except Exception:
            hub.unsubscribe(subscription)
            raise

        self._websockets.add(ws)
        delivery_task: asyncio.Task[Any] | None = None
        receive_task: asyncio.Task[Any] | None = None
        try:
            await ws.send_json({"type": "stream_ready"})
            delivery_task = asyncio.create_task(
                subscription.get(), name="control-plane-telemetry-delivery"
            )
            receive_task = asyncio.create_task(ws.receive(), name="control-plane-websocket-receive")
            while not ws.closed:
                done, _ = await asyncio.wait(
                    {delivery_task, receive_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if receive_task in done:
                    message = receive_task.result()
                    if not await self._handle_stream_message(
                        ws,
                        message,
                        principal=principal,
                    ):
                        break
                    receive_task = asyncio.create_task(
                        ws.receive(), name="control-plane-websocket-receive"
                    )
                if delivery_task in done:
                    delivery = delivery_task.result()
                    if delivery.stream_closed:
                        await ws.close(
                            code=WSCloseCode.GOING_AWAY,
                            message=b"telemetry stream closed",
                        )
                        break
                    if delivery.resync_required:
                        await ws.send_json(
                            {
                                "type": "resync_required",
                                "dropped_count": delivery.dropped_count,
                            }
                        )
                        await self._send_snapshot(ws)
                    elif delivery.event is not None:
                        await ws.send_json(
                            {
                                "type": "telemetry_event",
                                "data": self._serializer.to_jsonable(delivery.event),
                            }
                        )
                    delivery_task = asyncio.create_task(
                        subscription.get(), name="control-plane-telemetry-delivery"
                    )
        finally:
            for task in (delivery_task, receive_task):
                if task is not None and not task.done():
                    task.cancel()
            pending = [task for task in (delivery_task, receive_task) if task is not None]
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            hub.unsubscribe(subscription)
            self._websockets.discard(ws)
            if not ws.closed:
                await ws.close(code=WSCloseCode.GOING_AWAY)
        return ws

    async def _handle_stream_message(
        self,
        ws: web.WebSocketResponse,
        message: Any,
        *,
        principal: ControlPlanePrincipal,
    ) -> bool:
        if message.type in {WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED}:
            return False
        if message.type is WSMsgType.ERROR:
            return False
        if message.type is WSMsgType.BINARY:
            await ws.close(
                code=WSCloseCode.UNSUPPORTED_DATA,
                message=b"text messages only",
            )
            return False
        if message.type is not WSMsgType.TEXT:
            return True
        decision = self._rate_limiter.check(f"{principal.subject}:telemetry.stream.message")
        if not decision.allowed:
            await ws.send_json(
                {
                    "type": "error",
                    "code": "rate_limited",
                    "retry_after_seconds": decision.retry_after_seconds,
                }
            )
            await ws.close(
                code=WSCloseCode.POLICY_VIOLATION,
                message=b"stream message rate limit exceeded",
            )
            return False
        try:
            payload = json.loads(message.data)
        except (json.JSONDecodeError, TypeError):
            await ws.send_json({"type": "error", "code": "invalid_stream_message"})
            return True
        if not isinstance(payload, dict):
            await ws.send_json({"type": "error", "code": "invalid_stream_message"})
            return True
        message_type = payload.get("type")
        if message_type == "ping":
            await ws.send_json({"type": "pong"})
        elif message_type == "resync":
            await self._send_snapshot(ws)
        else:
            await ws.send_json({"type": "error", "code": "unsupported_stream_message"})
        return True

    async def _send_snapshot(self, ws: web.WebSocketResponse) -> None:
        query = self._dependencies.telemetry_query
        if query is None:
            await ws.send_json({"type": "telemetry_snapshot", "data": None})
            return
        snapshot = query.snapshot(limit=self._config.telemetry_snapshot_limit)
        await ws.send_json(
            {
                "type": "telemetry_snapshot",
                "data": self._serializer.to_jsonable(snapshot),
            }
        )

    async def _on_shutdown(self, app: web.Application) -> None:
        del app
        await self.close_streams()

    async def _cleanup_context(self, app: web.Application) -> AsyncIterator[None]:
        del app
        yield
        await self.close_streams()

    async def close_streams(self) -> None:
        """Close every owned WebSocket without closing the shared hub."""

        self._shutting_down = True
        sockets = tuple(self._websockets)
        if not sockets:
            return
        close_calls = [
            socket.close(
                code=WSCloseCode.GOING_AWAY,
                message=b"control plane shutting down",
            )
            for socket in sockets
        ]
        try:
            async with asyncio.timeout(self._config.shutdown_timeout_seconds):
                await asyncio.gather(*close_calls, return_exceptions=True)
        except TimeoutError:
            pass

    # ------------------------------------------------------------------
    # Request parsing
    # ------------------------------------------------------------------
    async def _json_body(
        self,
        request: web.Request,
        *,
        required: bool = True,
    ) -> dict[str, object]:
        if (
            request.content_length is not None
            and request.content_length > self._config.request_max_bytes
        ):
            raise web.HTTPRequestEntityTooLarge(
                max_size=self._config.request_max_bytes,
                actual_size=request.content_length,
            )
        raw = await request.read()
        if not raw:
            if required:
                raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
            return {}
        if request.content_type != "application/json":
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        try:
            value = json.loads(
                raw,
                object_pairs_hook=_unique_json_object,
                parse_constant=_reject_json_constant,
            )
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST) from None
        if not isinstance(value, dict):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        _validate_input(value)
        return value


class ControlPlaneHttpServer:
    """AppRunner/TCPSite lifecycle owner for the control-plane adapter."""

    def __init__(
        self,
        *,
        config: ControlPlaneHttpConfig,
        authenticator: ControlPlaneAuthenticator,
        dependencies: ControlPlaneDependencies,
        permission_policy: ControlPlanePermissionPolicy | None = None,
        rate_limiter: InProcessActionRateLimiter | None = None,
        serializer: ControlPlaneJsonSerializer | None = None,
        error_mapper: ControlPlaneErrorMapper | None = None,
        correlation_ids: CorrelationIdFactory | None = None,
    ) -> None:
        _validate_bind_auth(config, authenticator)
        self._config = config
        self._adapter = ControlPlaneHttpApplication(
            config=config,
            authenticator=authenticator,
            dependencies=dependencies,
            permission_policy=permission_policy,
            rate_limiter=rate_limiter,
            serializer=serializer,
            error_mapper=error_mapper,
            correlation_ids=correlation_ids,
        )
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None

    @property
    def app(self) -> web.Application:
        """Return the configured aiohttp application."""

        return self._adapter.app

    @property
    def running(self) -> bool:
        """Return whether the network runner currently owns resources."""

        return self._runner is not None

    async def start(self) -> None:
        """Start the loopback-default aiohttp runner exactly once."""

        if self._runner is not None:
            return
        runner = web.AppRunner(self.app, handle_signals=False)
        await runner.setup()
        site = web.TCPSite(runner, self._config.host, self._config.port)
        try:
            await site.start()
        except Exception:
            await runner.cleanup()
            raise
        self._runner = runner
        self._site = site

    async def close(self) -> None:
        """Close WebSockets, sites, and application resources idempotently."""

        runner = self._runner
        self._runner = None
        self._site = None
        if runner is None:
            return
        await self._adapter.close_streams()
        await runner.cleanup()

    async def __aenter__(self) -> ControlPlaneHttpServer:
        await self.start()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()


def create_control_plane_app(
    *,
    config: ControlPlaneHttpConfig,
    authenticator: ControlPlaneAuthenticator,
    dependencies: ControlPlaneDependencies,
    permission_policy: ControlPlanePermissionPolicy | None = None,
    rate_limiter: InProcessActionRateLimiter | None = None,
    serializer: ControlPlaneJsonSerializer | None = None,
    error_mapper: ControlPlaneErrorMapper | None = None,
    correlation_ids: CorrelationIdFactory | None = None,
) -> web.Application:
    """Build one aiohttp application from explicit injected capabilities."""

    return ControlPlaneHttpApplication(
        config=config,
        authenticator=authenticator,
        dependencies=dependencies,
        permission_policy=permission_policy,
        rate_limiter=rate_limiter,
        serializer=serializer,
        error_mapper=error_mapper,
        correlation_ids=correlation_ids,
    ).app


def _validate_bind_auth(
    config: ControlPlaneHttpConfig,
    authenticator: ControlPlaneAuthenticator,
) -> None:
    if not is_loopback_host(config.host) and not authenticator.has_credentials:
        raise ValueError("non-loopback control-plane binds require bearer authentication")
    if config.require_authentication and not authenticator.has_credentials:
        raise ValueError("control-plane configuration requires a bearer credential")


def _supports(value: object | None, method_name: str) -> bool:
    return value is not None and callable(getattr(value, method_name, None))


def _required_text(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
    return value.strip()


def _required_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
    return value


def _required_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
    if any(not isinstance(key, str) for key in value):
        raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
    return value


def _optional_mapping(
    value: object,
    *,
    fallback: Mapping[str, object],
) -> Mapping[str, object]:
    if value is None:
        return fallback
    return _required_mapping(value)


def _required_query(request: web.Request, name: str) -> str:
    return _required_text(request.query.get(name))


def _required_path(request: web.Request, name: str) -> str:
    return _required_text(request.match_info.get(name))


def _page(request: web.Request) -> PageRequest:
    raw_limit = request.query.get("limit", "50")
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        raise ControlPlaneError(ControlPlaneReason.INVALID_PAGE) from None
    cursor = request.query.get("cursor")
    return PageRequest(limit=limit, cursor=cursor)


def _validate_input(value: object, *, depth: int = 0, budget: list[int] | None = None) -> None:
    if budget is None:
        budget = [10_000]
    if depth > 20:
        raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
    if value is None or isinstance(value, (bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        return
    if isinstance(value, str):
        if len(value) > 65_536:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            budget[0] -= 1
            if budget[0] < 0 or not isinstance(key, str) or len(key) > 256:
                raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
            _validate_input(item, depth=depth + 1, budget=budget)
        return
    if isinstance(value, list):
        for item in value:
            budget[0] -= 1
            if budget[0] < 0:
                raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
            _validate_input(item, depth=depth + 1, budget=budget)
        return
    raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)
        result[key] = value
    return result


def _reject_json_constant(value: str) -> object:
    del value
    raise ControlPlaneError(ControlPlaneReason.INVALID_REQUEST)


__all__ = [
    "API_PREFIX",
    "ControlPlaneHttpApplication",
    "ControlPlaneHttpServer",
    "create_control_plane_app",
]
