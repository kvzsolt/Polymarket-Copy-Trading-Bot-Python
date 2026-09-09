#!/usr/bin/env python3
"""Hand-written declarations behind the exported control-plane API contract.

Everything ``scripts/export_api_contract.py`` cannot recover from the running
code lives here, and nothing else does. A route's permission, capability,
query contract, request body, rate-limit bucket, and success status are not
recoverable from an aiohttp ``UrlDispatcher``; which view fields carry money is
not recoverable from a type annotation; the transport error vocabulary is
raised from literals rather than an enum.

Every declaration here is cross-checked before the contract is written:
:data:`ROUTES` in both directions against the routes an all-capabilities
``ControlPlaneHttpApplication`` really registers, :data:`DECIMAL_FIELDS`
against the ``ContractValidation`` calls each view really runs, and every error
code and restated literal against the transport source. An added, renamed, or
removed route therefore fails the export instead of silently publishing a lie.

Several endpoints are registered under two paths. They are declared once as a
:class:`RouteGroup`; the alias summaries are derived so the two spellings can
never describe different behaviour.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from integrations.control_plane.http import API_PREFIX  # noqa: E402


WEBSOCKET_HEARTBEAT_SECONDS = 30.0

WEBSOCKET_MAX_INBOUND_MESSAGE_BYTES = 4096
"""Cap on one **client-to-server** WebSocket message.

``web.WebSocketResponse(max_msg_size=...)`` reaches only the inbound
``WebSocketReader``; ``send_str``/``send_json`` never consult it, so
server-to-client frames — including a ``telemetry_snapshot`` carrying up to
``telemetry_limit_default`` events — are not bounded by this number. The reader
rejects a frame whose projected size *reaches* the limit, so the usable inbound
payload is one byte smaller.
"""

TELEMETRY_LIMIT_MAX = 1000
PAGE_LIMIT_DEFAULT = 50
PAGE_LIMIT_MIN = 1
PAGE_LIMIT_MAX = 100
CURSOR_MAX_CHARS = 512
BULK_ITEMS_MAX = 100

HTTP_MODULE = "integrations/control_plane/http.py"

SECURITY_MODULE = "integrations/control_plane/security.py"

WEBSOCKET_SUBPROTOCOL_MARKER = "bearer"
"""First token of the browser credential carrier on the stream upgrade.

A browser cannot set request headers on a ``WebSocket`` upgrade, so
``GET /api/v1/stream`` also accepts ``Sec-WebSocket-Protocol: bearer,
<credential>`` and echoes this marker alone on the 101.
"""

WEBSOCKET_SUBPROTOCOL_CREDENTIAL_CHARSET = "A-Za-z0-9._~+/=-"
"""Characters a credential may use to survive a subprotocol token.

Subprotocol tokens carry no whitespace and no comma and are transmitted
verbatim, so a credential outside this class disables the carrier for the whole
deployment.
"""

# Literals this exporter restates, and therefore has to prove still exist in
# the transport source, so a rename cannot leave a lie in the contract.
SOURCE_GUARDS: tuple[tuple[str, str], ...] = (
    ("heartbeat=30.0", HTTP_MODULE),
    ("max_msg_size=4096", HTTP_MODULE),
    ("protocols=self._websocket_credentials.echo_protocols", HTTP_MODULE),
    ('WEBSOCKET_BEARER_SUBPROTOCOL = "bearer"', SECURITY_MODULE),
    (f"[{WEBSOCKET_SUBPROTOCOL_CREDENTIAL_CHARSET}]{{1,1024}}", SECURITY_MODULE),
    ('request.query.get("limit", "50")', HTTP_MODULE),
    ("0 <= limit <= 1000", HTTP_MODULE),
    ("1 <= len(raw_items) <= 100", HTTP_MODULE),
    ("retry_after_seconds", HTTP_MODULE),
)

FIELD_VOCABULARIES: Mapping[str, Mapping[str, str]] = {
    "PnlView": {"unrealized_basis": "services.control_plane.contracts_reads:PNL_VALUATION_BASES"},
    "TaskPriceView": {
        "offset_mode": "services.copy_tasks.domain:PriceOffsetMode",
        "offset_direction": "services.copy_tasks.domain:PriceOffsetDirection",
    },
    "TaskReactionView": {"copy_side": "services.copy_tasks.domain:CopySideFilter"},
    "TaskSizingView": {"mode": "services.copy_tasks.domain:TaskSizingMode"},
    "TaskView": {
        "status": "services.copy_tasks.domain:CopyTaskStatus",
        "monitoring_mode": "services.copy_tasks.domain:MonitoringMode",
        "sizing_mode": "services.copy_tasks.domain:TaskSizingMode",
    },
}
"""Closed vocabularies behind view fields that are typed as plain strings.

These views project a domain enum (or a validated literal tuple) through
``.value``, so the annotation says ``str`` and the vocabulary is invisible to
type introspection. Naming the source here — ``module:symbol`` — lets the
exporter resolve the real members, so the published contract is self-sufficient
without anyone retyping a list. The exporter rejects a name that does not
resolve, a field that does not exist, and a field that is not a string.
"""

DECIMAL_FIELDS: Mapping[str, tuple[str, ...]] = {
    "DiscoveryCandidateView": ("score",),
    "DiscoveryRosterView": ("score",),
    "FillView": ("size", "price", "fee"),
    "LeaderView": ("score",),
    "OrderView": ("amount", "price_limit", "matched_total"),
    "PnlPositionView": (
        "size",
        "avg_entry_price",
        "cost_basis_usdc",
        "mark_price",
        "market_value_usdc",
        "unrealized_pnl_usdc",
    ),
    "PnlSummaryView": (
        "realized_pnl_usdc",
        "total_invested_usdc",
        "total_returned_usdc",
        "max_drawdown_usdc",
        "net_realized_pnl_usdc",
    ),
    "PnlView": ("cost_basis_usdc", "unrealized_pnl_usdc"),
    "PortfolioView": (
        "cash_usdc",
        "reserved_usdc",
        "positions_value_usdc",
        "equity_usdc",
        "copyable_balance_usdc",
        "unrealized_pnl_usdc",
        "positions_cost_usdc",
    ),
    "PositionView": ("size", "avg_entry_price"),
    "ReservationView": ("amount",),
    "RiskHeadroomView": (
        "cap_notional_usdc",
        "equity_usdc",
        "free_cash_usdc",
        "daily_loss_usdc",
    ),
    "RiskPolicyView": (
        "max_market_exposure_usdc",
        "max_category_exposure_usdc",
        "max_leader_exposure_usdc",
        "max_daily_drawdown_pct",
        "reserve_pct",
        "reserve_floor_usdc",
        "hard_cap_pct",
        "hard_cap_ceiling_usdc",
    ),
    "RiskStateView": ("day_start_equity_usdc",),
    "TaskMarketFilterView": (
        "min_liquidity_usdc",
        "min_volume_24h_usdc",
        "max_days_until_close",
    ),
    "TaskPriceView": ("min_price", "max_price", "offset", "max_slippage_percent"),
    "TaskSizingView": (
        "percentage",
        "fixed_amount",
        "trade_multiplier",
        "weighted_multiplier",
        "buy_min_amount",
        "buy_max_amount",
        "copyable_balance_percent",
    ),
    "TaskSpendView": ("max_total", "max_daily", "max_per_market"),
    "TaskTriggerView": ("min_notional", "max_notional"),
}
"""Fields that carry money, prices, shares, or ratios as decimal strings.

Curated deliberately, so adding a money field to a view shows up as a diff
here, then cross-checked against the validators each view really runs — see
:meth:`ApiContractExporter.derived_decimal_fields`.
"""

TRANSPORT_ERRORS: Mapping[str, tuple[int | None, str]] = {
    "authentication_required": (401, "No Authorization header on an authenticated deployment."),
    "invalid_bearer_token": (401, "Malformed or unknown bearer credential."),
    "permission_denied": (403, "Authenticated principal lacks this route's permission."),
    "origin_not_allowed": (403, "Origin header outside the configured CORS allowlist."),
    "rate_limited": (429, "Per subject:action command budget exhausted; see Retry-After."),
    "payload_too_large": (413, "Request body exceeds request_max_bytes."),
    "not_found": (404, "No such endpoint, or that capability is not composed."),
    "method_not_allowed": (405, "Endpoint exists but not for this method."),
    "telemetry_unavailable": (503, "Telemetry stream unavailable or shutting down."),
    "conflicting_bearer_credentials": (
        401,
        "Upgrade carried both an Authorization header and a bearer subprotocol "
        "offer that do not present the same credential.",
    ),
    "unsupported_subprotocol": (
        400,
        "Upgrade offered a Sec-WebSocket-Protocol value this endpoint will not "
        "echo; refused before the handshake.",
    ),
    "http_error": (None, "Generic transport failure; the HTTP status carries the meaning."),
}
"""Transport-level error codes that never reach ``ControlPlaneReason``."""

WEBSOCKET_ERROR_CODES: Mapping[str, str] = {
    "invalid_stream_message": "Client frame was not a JSON object.",
    "rate_limited": "Client frame budget exhausted; the socket then closes.",
    "unsupported_stream_message": "Client frame type is neither ping nor resync.",
}
"""Codes carried by the WebSocket ``error`` frame."""

PAYLOAD_FIELD_PLACEHOLDERS: Mapping[str, object] = {"stream": "leader"}


@dataclass(frozen=True, slots=True)
class RouteSpec:
    """One registered control-plane route and the contract around it.

    Attributes:
        method: Uppercase HTTP method.
        path: Full canonical aiohttp path including ``/api/v1``.
        permission: Exact ``ControlPlanePermission`` value the handler requires.
        capability: Capability name that decides whether this route is composed
            at all, or ``None`` for routes present whenever the control plane
            is enabled.
        response: Response projection name, ``Page<View>`` for a page, ``View[]``
            for a plain array, or a documented pseudo-type.
        summary: One-line description for a human reading the contract.
        required_query: Query parameters the handler rejects the request
            without.
        optional_query: Query parameters the handler accepts.
        body: Top-level JSON body keys the handler reads.
        action: Rate-limit bucket suffix (``subject:action``), or ``None`` for
            an unthrottled read.
        status: HTTP status of the success response.
    """

    method: str
    path: str
    permission: str
    capability: str | None
    response: str
    summary: str
    required_query: tuple[str, ...] = ()
    optional_query: tuple[str, ...] = ()
    body: tuple[str, ...] = ()
    action: str | None = None
    status: int = 200

    def to_json(self) -> dict[str, Any]:
        """Return the deterministic JSON projection of this route."""

        return {
            "action": self.action,
            "body": list(self.body),
            "capability": self.capability,
            "method": self.method,
            "optional_query": list(self.optional_query),
            "path": self.path,
            "permission": self.permission,
            "required_query": list(self.required_query),
            "response": self.response,
            "status": self.status,
            "summary": self.summary,
        }


@dataclass(frozen=True, slots=True)
class RouteGroup:
    """One handler published under one or more equivalent paths.

    Attributes:
        method: Uppercase HTTP method shared by every path.
        paths: Canonical paths, the first being the documented spelling.
        permission: Permission the shared handler requires.
        capability: Capability that composes the shared handler.
        response: Shared response projection name.
        summary: Description of the first path.
        required_query: Query parameters the handler rejects the request
            without.
        optional_query: Query parameters the handler accepts.
        body: Top-level JSON body keys the handler reads.
        action: Rate-limit bucket suffix, or ``None`` for an unthrottled read.
        status: HTTP status of the success response.
    """

    method: str
    paths: tuple[str, ...]
    permission: str
    capability: str | None
    response: str
    summary: str
    required_query: tuple[str, ...] = ()
    optional_query: tuple[str, ...] = ()
    body: tuple[str, ...] = ()
    action: str | None = None
    status: int = 200

    def expand(self) -> tuple[RouteSpec, ...]:
        """Return one :class:`RouteSpec` per path, with derived alias summaries."""

        primary = self.paths[0]
        specs: list[RouteSpec] = []
        for index, path in enumerate(self.paths):
            summary = (
                self.summary
                if index == 0
                else f"Alias of {self.method} {primary.removeprefix(API_PREFIX)}."
            )
            specs.append(
                RouteSpec(
                    method=self.method,
                    path=path,
                    permission=self.permission,
                    capability=self.capability,
                    response=self.response,
                    summary=summary,
                    required_query=self.required_query,
                    optional_query=self.optional_query,
                    body=self.body,
                    action=self.action,
                    status=self.status,
                )
            )
        return tuple(specs)


_PAGE = ("limit", "cursor")
_FOLLOWER = ("follower_account",)
_VERSION_BODY = ("expected_version",)
_REVISION_BODY = ("expected_revision", "reason_code")
_LOCK_BODY = (
    "follower_account",
    "task_id",
    "leader_wallet",
    "market_id",
    "expected_revision",
    "reason_code",
)


ROUTE_GROUPS: tuple[RouteGroup, ...] = (
    # -- system ------------------------------------------------------------
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/system",),
        permission="system.read",
        capability=None,
        response="SystemView",
        summary="Process identity, environment, trading mode, and uptime.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/status", f"{API_PREFIX}/system/status"),
        permission="system.read",
        capability=None,
        response="SystemStatusView",
        summary="Process identity paired with authoritative readiness.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/readiness", f"{API_PREFIX}/system/readiness"),
        permission="system.read",
        capability=None,
        response="ReadinessView",
        summary="Readiness half of the system status projection.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/capabilities", f"{API_PREFIX}/system/capabilities"),
        permission="system.read",
        capability=None,
        response="CapabilitiesView",
        summary="Capabilities this exact composition backs; drives what the UI shows.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/accounts",),
        permission="system.read",
        capability="account_read",
        response="AccountView[]",
        summary="Follower accounts this process composed, projected per request.",
    ),
    # -- tasks -------------------------------------------------------------
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/tasks",),
        permission="task.read",
        capability="task_read",
        response="Page<TaskView>",
        summary="Copy tasks for one follower account.",
        required_query=_FOLLOWER,
        optional_query=_PAGE,
    ),
    RouteGroup(
        method="POST",
        paths=(f"{API_PREFIX}/tasks",),
        permission="task.write",
        capability="task_create",
        response="TaskView",
        summary="Create one paused copy task.",
        body=("task",),
        action="task.create",
        status=201,
    ),
    RouteGroup(
        method="POST",
        paths=(f"{API_PREFIX}/tasks/bulk",),
        permission="task.write",
        capability="task_bulk",
        response="BulkTaskResult",
        summary="Apply one all-or-none batch of 1..100 versioned task actions.",
        body=("items",),
        action="task.bulk",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/tasks/{{task_id}}",),
        permission="task.read",
        capability="task_read",
        response="TaskDetailView",
        summary="One task summary plus its complete typed configuration.",
    ),
    RouteGroup(
        method="PUT",
        paths=(f"{API_PREFIX}/tasks/{{task_id}}",),
        permission="task.write",
        capability="task_update",
        response="TaskView",
        summary="Replace one task configuration under its expected version.",
        body=("task", "expected_version"),
        action="task.update",
    ),
    RouteGroup(
        method="PATCH",
        paths=(f"{API_PREFIX}/tasks/{{task_id}}",),
        permission="task.write",
        capability="task_update",
        response="TaskView",
        summary="Same handler and same complete body as PUT /tasks/{task_id}.",
        body=("task", "expected_version"),
        action="task.update",
    ),
    RouteGroup(
        method="DELETE",
        paths=(f"{API_PREFIX}/tasks/{{task_id}}",),
        permission="task.write",
        capability="task_delete",
        response="TaskView",
        summary="Soft-delete one task; positions are never closed implicitly.",
        body=_VERSION_BODY,
        action="task.delete",
    ),
    RouteGroup(
        method="POST",
        paths=(f"{API_PREFIX}/tasks/{{task_id}}/pause",),
        permission="task.write",
        capability="task_pause",
        response="TaskView",
        summary="Pause one task under its expected version.",
        body=_VERSION_BODY,
        action="task.pause",
    ),
    RouteGroup(
        method="POST",
        paths=(f"{API_PREFIX}/tasks/{{task_id}}/resume",),
        permission="task.write",
        capability="task_resume",
        response="TaskView",
        summary="Resume one task; fails closed without a live execution binding.",
        body=_VERSION_BODY,
        action="task.resume",
    ),
    RouteGroup(
        method="POST",
        paths=(f"{API_PREFIX}/tasks/{{task_id}}/duplicate",),
        permission="task.write",
        capability="task_duplicate",
        response="TaskView",
        summary="Duplicate one task into a new paused task.",
        body=("new_task_id", "new_name"),
        action="task.duplicate",
        status=201,
    ),
    # -- trading reads -----------------------------------------------------
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/positions",),
        permission="position.read",
        capability="position_read",
        response="Page<PositionView>",
        summary="Open positions for one follower account.",
        required_query=_FOLLOWER,
        optional_query=_PAGE,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/positions/{{position_id}}",),
        permission="position.read",
        capability="position_read",
        response="PositionView",
        summary="One open position; still follower-scoped by query parameter.",
        required_query=_FOLLOWER,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/orders",),
        permission="order.read",
        capability="order_read",
        response="Page<OrderView>",
        summary="Durable orders for one follower account.",
        required_query=_FOLLOWER,
        optional_query=_PAGE,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/orders/{{order_id}}",),
        permission="order.read",
        capability="order_read",
        response="OrderView",
        summary="One durable order; still follower-scoped by query parameter.",
        required_query=_FOLLOWER,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/fills",),
        permission="fill.read",
        capability="fill_read",
        response="Page<FillView>",
        summary="Durable fills, newest first, for one follower account.",
        required_query=_FOLLOWER,
        optional_query=_PAGE,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/activity",),
        permission="activity.read",
        capability="activity_read",
        response="Page<ActivityView>",
        summary="Sanitized operator activity, optionally follower-scoped.",
        optional_query=("follower_account", *_PAGE),
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/portfolio",),
        permission="portfolio.read",
        capability="portfolio_read",
        response="PortfolioView",
        summary="Cached portfolio snapshot; never triggers a venue call.",
        required_query=_FOLLOWER,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/pnl",),
        permission="portfolio.read",
        capability="pnl_read",
        response="PnlView",
        summary="Session realized ledger plus honestly marked open positions.",
        required_query=_FOLLOWER,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/risk",),
        permission="risk.read",
        capability="risk_read",
        response="RiskStateView",
        summary="Configured policy, active blocks, recent triggers, and headroom.",
        required_query=_FOLLOWER,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/reservations",),
        permission="reservation.read",
        capability="reservation_read",
        response="Page<ReservationView>",
        summary="Collateral reservations for one follower account.",
        required_query=_FOLLOWER,
        optional_query=_PAGE,
    ),
    # -- detection, leaders, discovery -------------------------------------
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/detection",),
        permission="runtime.read",
        capability="detection_read",
        response="DetectionStatusView",
        summary="Local detection-source counters and observable WSS health.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/leaders",),
        permission="leader.read",
        capability="leader_read",
        response="LeaderView[]",
        summary="Merged leader identities, capped at 500 rows.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/discovery/cycles",),
        permission="discovery.read",
        capability="discovery_read",
        response="Page<DiscoveryCycleView>",
        summary="Completed discovery cycles.",
        optional_query=_PAGE,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/discovery/status",),
        permission="discovery.read",
        capability="discovery_read",
        response="DiscoveryStatusView",
        summary="Current discovery scheduler state.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/discovery/candidates",),
        permission="discovery.read",
        capability="discovery_read",
        response="Page<DiscoveryCandidateView>",
        summary="Evaluated discovery candidates and their scores.",
        optional_query=_PAGE,
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/discovery/roster",),
        permission="discovery.read",
        capability="discovery_read",
        response="Page<DiscoveryRosterView>",
        summary="Active and inactive discovery roster entries.",
        optional_query=_PAGE,
    ),
    RouteGroup(
        method="POST",
        paths=(f"{API_PREFIX}/discovery/run",),
        permission="discovery.run",
        capability="discovery_manual_run",
        response="DiscoveryRunReceipt",
        summary="Queue at most one discovery cycle and return immediately.",
        body=("force",),
        action="discovery.run",
        status=202,
    ),
    # -- reentry locks -----------------------------------------------------
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/locks", f"{API_PREFIX}/reentry-locks"),
        permission="lock.read",
        capability="reentry_lock_read",
        response="Page<ReentryLockView>",
        summary="Protective-exit reentry locks for one follower account.",
        required_query=_FOLLOWER,
        optional_query=_PAGE,
    ),
    RouteGroup(
        method="POST",
        paths=(
            f"{API_PREFIX}/locks/reset",
            f"{API_PREFIX}/reentry-locks/{{lock_id}}/reset",
        ),
        permission="lock.reset",
        capability="reentry_lock_reset",
        response="LockResetResult",
        summary=(
            "Reset one reentry lock by its complete identity; on the "
            "{lock_id} spelling that path segment must equal the body identity."
        ),
        body=_LOCK_BODY,
        action="lock.reset",
    ),
    # -- settings ----------------------------------------------------------
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/settings",),
        permission="settings.read",
        capability="settings_read",
        response="SettingsDocument[]",
        summary="Every operator-editable settings section and its revision.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/settings/{{environment}}/{{section}}",),
        permission="settings.read",
        capability="settings_read",
        response="SettingsDocument",
        summary="One settings section in an explicitly named environment.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/settings/{{section}}",),
        permission="settings.read",
        capability="settings_read",
        response="SettingsDocument",
        summary="One settings section in this process's own environment.",
    ),
    RouteGroup(
        method="PUT",
        paths=(f"{API_PREFIX}/settings/{{environment}}/{{section}}",),
        permission="settings.write",
        capability="settings_write",
        response="SettingsWriteResult",
        summary="Replace one settings section in an explicitly named environment.",
        body=("values", "expected_revision"),
        action="settings.write",
    ),
    RouteGroup(
        method="PUT",
        paths=(f"{API_PREFIX}/settings/{{section}}",),
        permission="settings.write",
        capability="settings_write",
        response="SettingsWriteResult",
        summary="Replace one section in this process's environment; restart-required.",
        body=("values", "expected_revision"),
        action="settings.write",
    ),
    # -- runtime safety ----------------------------------------------------
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/runtime", f"{API_PREFIX}/runtime/safety"),
        permission="runtime.read",
        capability="runtime_read",
        response="RuntimeView",
        summary="Entry-control safety state, worker count, and queue depth.",
    ),
    RouteGroup(
        method="POST",
        paths=(
            f"{API_PREFIX}/runtime/pause",
            f"{API_PREFIX}/runtime/safety/pause-entries",
        ),
        permission="runtime.write",
        capability="runtime_pause_entries",
        response="EntryControlState",
        summary="Pause new entries while protective exits keep running.",
        body=_REVISION_BODY,
        action="runtime.pause",
    ),
    RouteGroup(
        method="POST",
        paths=(
            f"{API_PREFIX}/runtime/resume",
            f"{API_PREFIX}/runtime/safety/resume-entries",
        ),
        permission="runtime.write",
        capability="runtime_resume_entries",
        response="EntryControlState",
        summary="Resume entries; cannot override an active full stop.",
        body=_REVISION_BODY,
        action="runtime.resume",
    ),
    RouteGroup(
        method="POST",
        paths=(
            f"{API_PREFIX}/runtime/full-stop",
            f"{API_PREFIX}/runtime/safety/full-stop",
        ),
        permission="runtime.write",
        capability="runtime_full_stop",
        response="EntryControlState",
        summary="Halt entries and protective exits, then cancel resting orders.",
        body=_REVISION_BODY,
        action="runtime.full_stop",
    ),
    # -- telemetry ---------------------------------------------------------
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/metrics/snapshot",),
        permission="telemetry.read",
        capability="metrics_snapshot",
        response="MetricsSnapshot",
        summary="Counters, gauges, and histograms without the event ring.",
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/telemetry", f"{API_PREFIX}/telemetry/snapshot"),
        permission="telemetry.read",
        capability="telemetry_snapshot",
        response="TelemetrySnapshot",
        summary="Bounded newest-first event history plus the metrics block.",
        optional_query=("limit",),
    ),
    RouteGroup(
        method="GET",
        paths=(f"{API_PREFIX}/stream",),
        permission="telemetry.stream",
        capability="live_stream",
        response="WebSocket",
        summary=(
            "Authenticated telemetry WebSocket upgrade; the credential may come "
            "from the Authorization header or the bearer subprotocol carrier."
        ),
    ),
)


def build_routes(groups: tuple[RouteGroup, ...] = ROUTE_GROUPS) -> tuple[RouteSpec, ...]:
    """Expand every group into its individual registered routes.

    Args:
        groups: Declared route groups.

    Returns:
        One :class:`RouteSpec` per registered method/path pair.
    """

    return tuple(spec for group in groups for spec in group.expand())


ROUTES: tuple[RouteSpec, ...] = build_routes()
"""Every method/path pair the control-plane application registers."""


__all__ = [
    "BULK_ITEMS_MAX",
    "CURSOR_MAX_CHARS",
    "DECIMAL_FIELDS",
    "FIELD_VOCABULARIES",
    "HTTP_MODULE",
    "PAGE_LIMIT_DEFAULT",
    "PAGE_LIMIT_MAX",
    "PAGE_LIMIT_MIN",
    "PAYLOAD_FIELD_PLACEHOLDERS",
    "ROUTES",
    "ROUTE_GROUPS",
    "RouteGroup",
    "RouteSpec",
    "SECURITY_MODULE",
    "SOURCE_GUARDS",
    "TELEMETRY_LIMIT_MAX",
    "TRANSPORT_ERRORS",
    "WEBSOCKET_ERROR_CODES",
    "WEBSOCKET_HEARTBEAT_SECONDS",
    "WEBSOCKET_MAX_INBOUND_MESSAGE_BYTES",
    "WEBSOCKET_SUBPROTOCOL_CREDENTIAL_CHARSET",
    "WEBSOCKET_SUBPROTOCOL_MARKER",
    "build_routes",
]
