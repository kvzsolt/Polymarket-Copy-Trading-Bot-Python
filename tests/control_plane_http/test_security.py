"""Security and boundary contracts for the aiohttp control-plane adapter."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from aiohttp.client_exceptions import WSServerHandshakeError
from aiohttp.test_utils import TestClient, TestServer

from integrations.control_plane import (
    ALL_CONTROL_PLANE_PERMISSIONS,
    BearerCredential,
    ControlPlaneAuthCarrier,
    ControlPlaneAuthenticator,
    ControlPlaneDependencies,
    ControlPlaneHttpConfig,
    ControlPlaneJsonSerializer,
    ControlPlanePermission,
    ControlPlanePermissionPolicy,
    ControlPlaneTransportError,
    InProcessActionRateLimiter,
    READ_ONLY_CONTROL_PLANE_PERMISSIONS,
    WebSocketCredentialExtractor,
    create_control_plane_app,
)
from services.control_plane.contracts import Page, PositionView


class _MonotonicClock:
    def __init__(self) -> None:
        self.value = 10.0

    def monotonic(self) -> float:
        return self.value


def _credential(token: str, subject: str) -> BearerCredential:
    return BearerCredential(
        token=token,
        subject=subject,
        permissions=frozenset(
            {
                ControlPlanePermission.SYSTEM_READ,
                ControlPlanePermission.TASK_WRITE,
            }
        ),
    )


def test_authenticator_compares_every_credential_with_constant_time_primitive() -> None:
    calls: list[tuple[str, str]] = []

    def compare(left: str, right: str) -> bool:
        calls.append((left, right))
        return left == right

    authenticator = ControlPlaneAuthenticator(
        credentials=(
            _credential("token-a", "operator-a"),
            _credential("token-b", "operator-b"),
        ),
        compare_digest=compare,
    )

    principal = authenticator.authenticate_header(
        "Bearer token-a",
        remote_address="127.0.0.1",
    )

    assert principal.subject == "operator-a"
    assert calls == [("token-a", "token-a"), ("token-a", "token-b")]
    assert "token-a" not in repr(authenticator)
    assert "token-a" not in repr(authenticator.credentials[0])


def test_invalid_bearer_and_missing_permission_fail_with_stable_public_codes() -> None:
    authenticator = ControlPlaneAuthenticator(credentials=(_credential("token-a", "operator-a"),))
    policy = ControlPlanePermissionPolicy()

    with pytest.raises(ControlPlaneTransportError) as missing:
        authenticator.authenticate_header(None, remote_address="127.0.0.1")
    with pytest.raises(ControlPlaneTransportError) as invalid:
        authenticator.authenticate_header("Basic token-a", remote_address="127.0.0.1")
    principal = authenticator.authenticate_header("Bearer token-a", remote_address="127.0.0.1")
    with pytest.raises(ControlPlaneTransportError) as forbidden:
        policy.require(principal, ControlPlanePermission.RUNTIME_WRITE)

    assert missing.value.code == "authentication_required"
    assert invalid.value.code == "invalid_bearer_token"
    assert forbidden.value.code == "permission_denied"


def test_the_subprotocol_carrier_reads_the_credential_in_either_token_order() -> None:
    extractor = WebSocketCredentialExtractor()

    for offered in ("bearer, token-a", "token-a,bearer", "  bearer ,  token-a  "):
        offer = extractor.resolve(authorization=None, subprotocol=offered)

        assert offer.carrier is ControlPlaneAuthCarrier.SUBPROTOCOL, offered
        assert offer.authorization == "Bearer token-a", offered
        assert offer.unsupported_subprotocol is False, offered


def test_an_absent_subprotocol_leaves_the_header_carrier_untouched() -> None:
    extractor = WebSocketCredentialExtractor()

    header = extractor.resolve(authorization="Bearer token-a", subprotocol=None)
    loopback = extractor.resolve(authorization=None, subprotocol=None)

    assert header.carrier is ControlPlaneAuthCarrier.HEADER
    assert header.authorization == "Bearer token-a"
    assert header.unsupported_subprotocol is False
    assert loopback.carrier is ControlPlaneAuthCarrier.NONE
    assert loopback.authorization is None
    assert loopback.unsupported_subprotocol is False


@pytest.mark.parametrize(
    "offered",
    (
        "",
        "bearer",
        "bearer, token-a, token-b",
        "chat",
        "Bearer, token-a",
        "bearer, tok en",
        "bearer, tok\x00en",
        "bearer, ",
        "bearer, " + "t" * 1025,
    ),
)
def test_an_offer_outside_the_bearer_carrier_form_never_authenticates(offered: str) -> None:
    offer = WebSocketCredentialExtractor().resolve(authorization=None, subprotocol=offered)

    assert offer.carrier is ControlPlaneAuthCarrier.NONE
    assert offer.authorization is None
    assert offer.unsupported_subprotocol is True


def test_the_authorization_header_takes_precedence_over_an_agreeing_offer() -> None:
    offer = WebSocketCredentialExtractor().resolve(
        authorization="Bearer token-a",
        subprotocol="bearer, token-a",
    )

    assert offer.carrier is ControlPlaneAuthCarrier.HEADER
    assert offer.authorization == "Bearer token-a"
    assert offer.unsupported_subprotocol is False


@pytest.mark.parametrize(
    "authorization",
    ("Bearer token-b", "Basic token-a", "Bearer  token-a", "Bearer tökén"),
)
def test_a_header_disagreeing_with_the_offer_is_rejected_without_echoing_either(
    authorization: str,
) -> None:
    with pytest.raises(ControlPlaneTransportError) as conflict:
        WebSocketCredentialExtractor().resolve(
            authorization=authorization,
            subprotocol="bearer, token-a",
        )

    assert conflict.value.status == 401
    assert conflict.value.code == "conflicting_bearer_credentials"
    assert "token-a" not in conflict.value.public_message
    assert "token-b" not in conflict.value.public_message


def test_an_unsafe_configured_credential_disables_the_carrier_with_one_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test.websocket.credentials.unsafe")

    with caplog.at_level(logging.WARNING, logger=logger.name):
        extractor = WebSocketCredentialExtractor.for_credentials(
            (
                _credential("safe-token", "operator-a"),
                _credential("unsafe token", "viewer-a"),
            ),
            logger=logger,
        )

    records = [record for record in caplog.records if record.name == logger.name]
    assert len(records) == 1
    assert records[0].getMessage() == "control_plane_websocket_subprotocol_auth_disabled"
    assert records[0].subjects == ["viewer-a"]
    assert "unsafe token" not in str(records[0].__dict__)
    assert extractor.enabled is False
    assert extractor.echo_protocols == ()

    offer = extractor.resolve(authorization=None, subprotocol="bearer, safe-token")

    assert offer.carrier is ControlPlaneAuthCarrier.NONE
    assert offer.authorization is None
    assert offer.unsupported_subprotocol is True


def test_safe_configured_credentials_enable_the_carrier_silently(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger("test.websocket.credentials.safe")

    with caplog.at_level(logging.WARNING, logger=logger.name):
        extractor = WebSocketCredentialExtractor.for_credentials(
            (_credential("safe-token", "operator-a"),),
            logger=logger,
        )

    assert [record for record in caplog.records if record.name == logger.name] == []
    assert extractor.enabled is True
    assert extractor.echo_protocols == ("bearer",)


def test_the_carrier_is_disabled_when_no_credential_is_configured() -> None:
    extractor = WebSocketCredentialExtractor.for_credentials(())

    assert extractor.enabled is False
    assert extractor.echo_protocols == ()
    assert (
        extractor.resolve(
            authorization=None,
            subprotocol="bearer, token-a",
        ).unsupported_subprotocol
        is True
    )


def test_non_loopback_bind_without_credentials_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-loopback"):
        ControlPlaneHttpConfig(
            host="0.0.0.0",
            port=9103,
            require_authentication=False,
        )


def test_cors_configuration_requires_explicit_origin_values() -> None:
    for origin in ("*", "https://ops.example/path", "https://user@ops.example"):
        with pytest.raises(ValueError, match=r"CORS origins"):
            ControlPlaneHttpConfig(cors_allowed_origins=(origin,))


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("require_authentication", "yes"),
        ("request_max_bytes", True),
        ("websocket_queue_capacity", True),
        ("telemetry_snapshot_limit", True),
        ("command_rate_limit", True),
        ("command_rate_window_seconds", True),
        ("rate_limit_max_keys", True),
        ("shutdown_timeout_seconds", True),
    ),
)
def test_http_config_rejects_boolean_numeric_and_nonboolean_flags(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        ControlPlaneHttpConfig(**{field_name: value})


def test_rate_limiter_is_bounded_and_uses_injected_monotonic_clock() -> None:
    clock = _MonotonicClock()
    limiter = InProcessActionRateLimiter(
        max_actions=2,
        window_seconds=10.0,
        max_keys=2,
        clock=clock,
    )

    assert limiter.check("operator-a:task.pause").allowed is True
    assert limiter.check("operator-a:task.pause").allowed is True
    limited = limiter.check("operator-a:task.pause")
    assert limited.allowed is False
    assert limited.retry_after_seconds == 10

    limiter.check("operator-b:task.pause")
    limiter.check("operator-c:task.pause")
    assert limiter.tracked_key_count == 2

    clock.value = 21.0
    assert limiter.check("operator-a:task.pause").allowed is True


def test_serializer_emits_exact_decimal_strings_and_utc_timestamps() -> None:
    @dataclass(frozen=True)
    class Payload:
        amount: Decimal
        observed_at: datetime

    serializer = ControlPlaneJsonSerializer()
    result = serializer.to_jsonable(
        Payload(
            amount=Decimal("12.3400"),
            observed_at=datetime(2026, 8, 11, 14, 0, tzinfo=timezone.utc),
        )
    )

    assert result == {"amount": "12.3400", "observed_at": "2026-08-11T14:00:00Z"}


def test_serializer_bounds_aggregate_text_and_rejects_sensitive_key_variants() -> None:
    serializer = ControlPlaneJsonSerializer(
        max_collection_items=10,
        max_string_chars=16,
        max_total_string_chars=8,
    )

    with pytest.raises(ValueError, match="total string"):
        serializer.to_jsonable({"a": "1234", "b": "5678"})
    with pytest.raises(ValueError, match="sensitive"):
        ControlPlaneJsonSerializer().to_jsonable({"api-key": "sv7"})


def test_read_only_permission_set_covers_every_read_and_excludes_mutations() -> None:
    """A viewer credential can observe everything and change nothing."""

    reads = {
        permission for permission in ControlPlanePermission if permission.value.endswith(".read")
    }

    assert ControlPlanePermission.FILL_READ.value == "fill.read"
    assert ControlPlanePermission.RISK_READ.value == "risk.read"
    assert ControlPlanePermission.LEADER_READ.value == "leader.read"
    assert reads <= READ_ONLY_CONTROL_PLANE_PERMISSIONS
    assert ControlPlanePermission.TELEMETRY_STREAM in READ_ONLY_CONTROL_PLANE_PERMISSIONS
    assert READ_ONLY_CONTROL_PLANE_PERMISSIONS == reads | {ControlPlanePermission.TELEMETRY_STREAM}
    assert READ_ONLY_CONTROL_PLANE_PERMISSIONS < ALL_CONTROL_PLANE_PERMISSIONS
    assert {
        ControlPlanePermission.FILL_READ,
        ControlPlanePermission.RISK_READ,
        ControlPlanePermission.LEADER_READ,
    } <= ALL_CONTROL_PLANE_PERMISSIONS


class _ViewerPositions:
    """Return one deterministic follower-scoped position page."""

    async def list(self, follower_account: str, page: object) -> object:
        """Return a single-row page for the requested follower."""

        del page
        return Page(
            items=(
                PositionView(
                    follower_account=follower_account,
                    condition_id="0xcondition",
                    token_id="12345",
                    leader_wallet="0x" + "1" * 40,
                    size="10.5",
                    avg_entry_price="0.4200",
                    updated_at=datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc),
                ),
            ),
            next_cursor=None,
        )


class _RecordingRuntimeCommands:
    """Fail if a read-only principal ever reaches a runtime mutation."""

    def __init__(self) -> None:
        self.calls: list[object] = []

    async def pause_entries(self, command: object) -> object:
        """Record a call that must never happen for a viewer credential."""

        self.calls.append(command)
        raise AssertionError("a read-only credential must not reach pause_entries")


class _ViewerStreamHub:
    """Minimal bounded telemetry hub for the stream upgrade check."""

    def __init__(self) -> None:
        self.boot_id = "test-stream-boot"
        self.queue: asyncio.Queue[object] = asyncio.Queue(maxsize=2)
        self.unsubscribed = 0

    def subscribe(self, *, capacity: int | None = None) -> "_ViewerStreamHub":
        """Return self as the bounded subscription."""

        del capacity
        return self

    def unsubscribe(self, subscription: object) -> None:
        """Record the subscription teardown."""

        del subscription
        self.unsubscribed += 1

    async def get(self) -> object:
        """Block until an event is published; the test never publishes one."""

        return await self.queue.get()


async def test_viewer_credential_reads_and_streams_but_cannot_mutate_runtime() -> None:
    """End-to-end proof of the read-only role over the real aiohttp adapter."""

    operator_token = "operator-credential-value"
    viewer_token = "viewer-credential-value"
    runtime_commands = _RecordingRuntimeCommands()
    hub = _ViewerStreamHub()
    app = create_control_plane_app(
        config=ControlPlaneHttpConfig(host="127.0.0.1", port=0, websocket_queue_capacity=2),
        authenticator=ControlPlaneAuthenticator(
            credentials=(
                BearerCredential(
                    token=operator_token,
                    subject="operator",
                    permissions=ALL_CONTROL_PLANE_PERMISSIONS,
                ),
                BearerCredential(
                    token=viewer_token,
                    subject="viewer",
                    permissions=READ_ONLY_CONTROL_PLANE_PERMISSIONS,
                ),
            )
        ),
        dependencies=ControlPlaneDependencies(
            positions=_ViewerPositions(),
            runtime_commands=runtime_commands,
            telemetry_hub=hub,
        ),
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    viewer_headers = {"Authorization": f"Bearer {viewer_token}"}
    try:
        forbidden = await client.post(
            "/api/v1/runtime/pause",
            headers=viewer_headers,
            json={"expected_revision": 1, "reason_code": "maintenance"},
        )
        assert forbidden.status == 403
        assert (await forbidden.json())["error"]["code"] == "permission_denied"
        assert runtime_commands.calls == []

        readable = await client.get(
            "/api/v1/positions?follower_account=account-a",
            headers=viewer_headers,
        )
        assert readable.status == 200
        assert (await readable.json())["data"]["items"][0]["size"] == "10.5"

        websocket = await client.ws_connect("/api/v1/stream", headers=viewer_headers)
        assert (await websocket.receive_json())["type"] == "stream_ready"
        await websocket.close()

        with pytest.raises(WSServerHandshakeError) as unauthenticated:
            await client.ws_connect("/api/v1/stream")
        assert unauthenticated.value.status == 401
    finally:
        await client.close()


async def test_stream_events_use_z_while_the_snapshot_uses_an_explicit_offset() -> None:
    """Both timestamp renderings are ISO-8601 UTC, and they differ on purpose.

    A WS ``telemetry_event`` frame serializes the ``TelemetryEvent`` dataclass
    through :class:`ControlPlaneJsonSerializer`, which renders ``...Z``. The
    ``/telemetry`` snapshot is pre-rendered by ``TelemetryQueryService`` with
    ``datetime.isoformat()``, so it keeps ``+00:00`` and the serializer passes
    the already-built string through untouched. Clients must parse both.
    """

    from services.metrics import MetricsRegistry
    from services.telemetry import TelemetryHub, TelemetryQueryService, TelemetryService
    from services.telemetry.contracts import LeaderEvent, LeaderOutcome, TelemetryProducer

    moment = datetime(2026, 8, 18, 14, 0, tzinfo=timezone.utc)

    class _FixedClock:
        def now(self) -> datetime:
            return moment

    metrics = MetricsRegistry()
    hub = TelemetryHub(metrics=metrics)
    service = TelemetryService(hub=hub, metrics=metrics, clock=_FixedClock())
    await service.start()
    try:
        query = TelemetryQueryService(service=service, metrics=metrics, clock=_FixedClock())
        service.emit(
            producer=TelemetryProducer.DETECTION,
            payload=LeaderEvent(
                outcome=LeaderOutcome.RECEIVED,
                leader_ref="h:0123456789abcdef",
                source="polling",
            ),
        )
        serializer = ControlPlaneJsonSerializer()
        snapshot = serializer.to_jsonable(query.snapshot(limit=10))
        event = serializer.to_jsonable(service.history_snapshot(limit=1)[0])

        assert event["timestamp"] == "2026-08-18T14:00:00Z"
        assert snapshot["generated_at"] == "2026-08-18T14:00:00+00:00"
        assert snapshot["events"][0]["timestamp"] == "2026-08-18T14:00:00+00:00"
    finally:
        await hub.close()


async def test_non_ascii_bearer_header_is_rejected_as_an_invalid_token() -> None:
    """A non-ASCII credential must answer ``401``, never crash the comparison.

    ``secrets.compare_digest`` raises ``TypeError`` when either ``str`` operand
    carries a non-ASCII character, so an unauthenticated caller could otherwise
    turn a header of their own choosing into a ``500`` on every REST route.
    """

    app = create_control_plane_app(
        config=ControlPlaneHttpConfig(host="127.0.0.1", port=0),
        authenticator=ControlPlaneAuthenticator(
            credentials=(
                BearerCredential(
                    token="operator-credential-value",
                    subject="operator",
                    permissions=ALL_CONTROL_PLANE_PERMISSIONS,
                ),
            )
        ),
        dependencies=ControlPlaneDependencies(positions=_ViewerPositions()),
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    try:
        response = await client.get(
            "/api/v1/positions?follower_account=account-a",
            headers={"Authorization": "Bearer ünicode"},
        )

        assert response.status == 401
        assert (await response.json())["error"]["code"] == "invalid_bearer_token"
    finally:
        await client.close()
