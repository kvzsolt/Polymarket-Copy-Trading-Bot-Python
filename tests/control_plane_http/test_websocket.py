"""Authenticated bounded telemetry WebSocket contracts."""

from __future__ import annotations

import asyncio
import base64
import logging

import pytest
from aiohttp import WSMsgType
from aiohttp.client_exceptions import WSServerHandshakeError
from aiohttp.test_utils import TestClient, TestServer

from integrations.control_plane import (
    READ_ONLY_CONTROL_PLANE_PERMISSIONS,
    BearerCredential,
    ControlPlaneAuthenticator,
    ControlPlaneDependencies,
    ControlPlaneHttpConfig,
    ControlPlanePermission,
    InProcessActionRateLimiter,
    create_control_plane_app,
)
from services.telemetry.hub import TelemetryDelivery

pytestmark = pytest.mark.asyncio

_TOKEN = "local-test-control-plane-token"
_VIEWER_TOKEN = "local-test-control-plane-viewer-token"


class _Subscription:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[TelemetryDelivery] = asyncio.Queue(maxsize=2)

    async def get(self) -> TelemetryDelivery:
        return await self.queue.get()


class _Hub:
    def __init__(self) -> None:
        self.boot_id = "test-stream-boot"
        self.subscription: _Subscription | None = None
        self.capacities: list[int] = []
        self.unsubscribed: list[_Subscription] = []

    def subscribe(self, *, capacity: int | None = None) -> _Subscription:
        assert capacity is not None
        self.capacities.append(capacity)
        self.subscription = _Subscription()
        return self.subscription

    def unsubscribe(self, subscription: _Subscription) -> None:
        self.unsubscribed.append(subscription)


class _TelemetryQuery:
    def snapshot(self, *, limit: int = 100) -> dict[str, object]:
        return {"generated_at": "2026-08-11T14:00:00Z", "events": [], "limit": limit}


class _Clock:
    def monotonic(self) -> float:
        return 10.0


def _auth(operator_token: str = _TOKEN) -> ControlPlaneAuthenticator:
    return ControlPlaneAuthenticator(
        credentials=(
            BearerCredential(
                token=operator_token,
                subject="operator-a",
                permissions=frozenset(
                    {
                        ControlPlanePermission.TELEMETRY_READ,
                        ControlPlanePermission.TELEMETRY_STREAM,
                    }
                ),
            ),
            BearerCredential(
                token=_VIEWER_TOKEN,
                subject="viewer-a",
                permissions=READ_ONLY_CONTROL_PLANE_PERMISSIONS,
            ),
        )
    )


async def _client(
    hub: _Hub,
    *,
    rate_limiter: InProcessActionRateLimiter | None = None,
    authenticator: ControlPlaneAuthenticator | None = None,
) -> TestClient:
    app = create_control_plane_app(
        config=ControlPlaneHttpConfig(
            host="127.0.0.1",
            port=0,
            websocket_queue_capacity=2,
        ),
        authenticator=authenticator if authenticator is not None else _auth(),
        dependencies=ControlPlaneDependencies(
            telemetry_query=_TelemetryQuery(),
            telemetry_hub=hub,
        ),
        rate_limiter=rate_limiter,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    return client


async def test_stream_requires_authentication_and_uses_bounded_subscription() -> None:
    hub = _Hub()
    client = await _client(hub)
    try:
        with pytest.raises(WSServerHandshakeError) as missing_auth:
            await client.ws_connect("/api/v1/stream")
        assert missing_auth.value.status == 401

        ws = await client.ws_connect(
            "/api/v1/stream",
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        ready = await ws.receive_json()
        assert ready["type"] == "stream_ready"
        assert hub.capacities == [2]

        assert hub.subscription is not None
        await hub.subscription.queue.put(TelemetryDelivery(resync_required=True, dropped_count=3))
        notice = await ws.receive_json()
        snapshot = await ws.receive_json()
        assert notice == {"type": "resync_required", "dropped_count": 3}
        assert snapshot["type"] == "telemetry_snapshot"
        assert snapshot["data"]["limit"] == 100

        await ws.close()
        for _ in range(20):
            if hub.unsubscribed:
                break
            await asyncio.sleep(0)
        assert hub.unsubscribed == [hub.subscription]
    finally:
        await client.close()


async def test_application_cleanup_closes_open_websocket_and_unsubscribes() -> None:
    hub = _Hub()
    client = await _client(hub)
    ws = await client.ws_connect(
        "/api/v1/stream",
        headers={"Authorization": f"Bearer {_TOKEN}"},
    )
    await ws.receive_json()

    await client.close()

    message = await ws.receive()
    assert message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING}
    assert hub.subscription is not None
    assert hub.unsubscribed == [hub.subscription]


async def test_inbound_stream_messages_are_rate_limited_per_principal() -> None:
    hub = _Hub()
    limiter = InProcessActionRateLimiter(
        max_actions=1,
        window_seconds=60,
        max_keys=8,
        clock=_Clock(),
    )
    client = await _client(hub, rate_limiter=limiter)
    try:
        ws = await client.ws_connect(
            "/api/v1/stream",
            headers={"Authorization": f"Bearer {_TOKEN}"},
        )
        await ws.receive_json()

        await ws.send_json({"type": "ping"})
        assert await ws.receive_json() == {"type": "pong"}
        await ws.send_json({"type": "ping"})
        limited = await ws.receive_json()

        assert limited == {
            "type": "error",
            "code": "rate_limited",
            "retry_after_seconds": 60,
        }
        assert (await ws.receive()).type in {
            WSMsgType.CLOSE,
            WSMsgType.CLOSED,
            WSMsgType.CLOSING,
        }
    finally:
        await client.close()


async def test_the_subprotocol_carrier_authenticates_and_echoes_the_bearer_token(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A browser sends the credential as the second subprotocol token.

    The 101 response must echo exactly ``bearer`` or the browser fails the
    handshake, and the two tokens are order-insensitive. The open is recorded
    by carrier and subject, never by credential.
    """

    hub = _Hub()
    client = await _client(hub)
    try:
        with caplog.at_level(logging.INFO, logger="integrations.control_plane.http"):
            operator = await client.ws_connect("/api/v1/stream", protocols=("bearer", _TOKEN))
            assert operator.protocol == "bearer"
            assert (await operator.receive_json())["type"] == "stream_ready"
            await operator.close()

        opened = [
            record for record in caplog.records if record.name == "integrations.control_plane.http"
        ]
        assert len(opened) == 1
        assert opened[0].getMessage() == "control_plane_stream_opened"
        assert opened[0].auth_carrier == "subprotocol"
        assert opened[0].principal == "operator-a"
        assert _TOKEN not in str(opened[0].__dict__)

        viewer = await client.ws_connect("/api/v1/stream", protocols=(_VIEWER_TOKEN, "bearer"))
        assert viewer.protocol == "bearer"
        assert (await viewer.receive_json())["type"] == "stream_ready"
        await viewer.close()
    finally:
        await client.close()


async def test_a_wrong_subprotocol_credential_never_reaches_the_upgrade() -> None:
    hub = _Hub()
    client = await _client(hub)
    try:
        with pytest.raises(WSServerHandshakeError) as rejected:
            await client.ws_connect("/api/v1/stream", protocols=("bearer", "not-the-token"))

        assert rejected.value.status == 401
        assert hub.capacities == []
        assert hub.subscription is None
    finally:
        await client.close()


async def test_the_authorization_header_outranks_the_subprotocol_offer() -> None:
    hub = _Hub()
    client = await _client(hub)
    headers = {"Authorization": f"Bearer {_TOKEN}"}
    try:
        agreeing = await client.ws_connect(
            "/api/v1/stream",
            headers=headers,
            protocols=("bearer", _TOKEN),
        )
        assert agreeing.protocol == "bearer"
        assert (await agreeing.receive_json())["type"] == "stream_ready"
        await agreeing.close()

        with pytest.raises(WSServerHandshakeError) as conflicting:
            await client.ws_connect(
                "/api/v1/stream",
                headers=headers,
                protocols=("bearer", _VIEWER_TOKEN),
            )
        assert conflicting.value.status == 401

        with pytest.raises(WSServerHandshakeError) as unrescued:
            await client.ws_connect(
                "/api/v1/stream",
                headers={"Authorization": "Bearer wrong-token"},
                protocols=("bearer", _TOKEN),
            )
        assert unrescued.value.status == 401
    finally:
        await client.close()


async def test_rest_routes_never_read_the_subprotocol_credential() -> None:
    hub = _Hub()
    client = await _client(hub)
    try:
        ignored = await client.get(
            "/api/v1/telemetry",
            headers={"Sec-WebSocket-Protocol": f"bearer, {_TOKEN}"},
        )
        assert ignored.status == 401

        authenticated = await client.get(
            "/api/v1/telemetry",
            headers={
                "Authorization": f"Bearer {_TOKEN}",
                "Sec-WebSocket-Protocol": f"bearer, {_VIEWER_TOKEN}",
            },
        )
        assert authenticated.status == 200
    finally:
        await client.close()


async def test_an_offer_the_server_cannot_echo_is_refused_before_the_handshake(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """aiohttp logs the offered tokens when no subprotocol overlaps.

    ``web_ws._handshake`` emits a WARNING containing the client's raw
    subprotocol list, which would publish a credential to the log. Refusing an
    offer this endpoint will not echo keeps that branch unreachable.
    """

    hub = _Hub()
    client = await _client(hub)
    try:
        with caplog.at_level(logging.WARNING, logger="aiohttp.websocket"):
            with pytest.raises(WSServerHandshakeError) as refused:
                await client.ws_connect(
                    "/api/v1/stream",
                    headers={"Authorization": f"Bearer {_TOKEN}"},
                    protocols=("graphql-ws",),
                )

        assert refused.value.status == 400
        assert [record for record in caplog.records if record.name == "aiohttp.websocket"] == []
    finally:
        await client.close()


async def test_an_unsafe_configured_credential_disables_the_carrier_with_one_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    unsafe_token = "local test control plane token"
    hub = _Hub()
    with caplog.at_level(logging.WARNING, logger="integrations.control_plane.security"):
        client = await _client(hub, authenticator=_auth(unsafe_token))

    warnings = [
        record for record in caplog.records if record.name == "integrations.control_plane.security"
    ]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == "control_plane_websocket_subprotocol_auth_disabled"
    assert unsafe_token not in str(warnings[0].__dict__)
    try:
        header_only = await client.ws_connect(
            "/api/v1/stream",
            headers={"Authorization": f"Bearer {unsafe_token}"},
        )
        assert header_only.protocol is None
        assert (await header_only.receive_json())["type"] == "stream_ready"
        await header_only.close()

        with pytest.raises(WSServerHandshakeError) as refused:
            await client.ws_connect("/api/v1/stream", protocols=("bearer", _VIEWER_TOKEN))
        assert refused.value.status == 401
    finally:
        await client.close()


async def test_a_head_upgrade_cannot_slip_an_offer_past_the_subprotocol_guard(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``add_get`` also registers HEAD, which must not reach the handshake.

    A method-gated guard would let ``HEAD`` carry an offer straight into
    ``ws.prepare``, where aiohttp logs the raw token list. The stream route
    registers GET alone, and the guard reads the path rather than the method.
    """

    hub = _Hub()
    client = await _client(hub)
    try:
        with caplog.at_level(logging.WARNING, logger="aiohttp.websocket"):
            response = await client.request(
                "HEAD",
                "/api/v1/stream",
                headers={
                    "Authorization": f"Bearer {_TOKEN}",
                    "Connection": "Upgrade",
                    "Upgrade": "websocket",
                    "Sec-WebSocket-Version": "13",
                    "Sec-WebSocket-Key": base64.b64encode(b"0123456789abcdef").decode(),
                    "Sec-WebSocket-Protocol": f"graphql-ws, {_TOKEN}",
                },
            )

        assert response.status in {400, 405}
        assert [record for record in caplog.records if record.name == "aiohttp.websocket"] == []
        assert hub.capacities == []
    finally:
        await client.close()


async def test_stream_declares_session_ordering_and_requires_authoritative_resync() -> None:
    hub = _Hub()
    hub.boot_id = "deterministic-stream-boot"
    client = await _client(hub)
    try:
        ws = await client.ws_connect(
            "/api/v1/stream", headers={"Authorization": f"Bearer {_TOKEN}"}
        )
        ready = await ws.receive_json()
        assert ready["stream_boot_id"] == hub.boot_id
        assert ready["ordering"] == "session_per_producer"
        assert ready["durable_replay"] is False
        assert ready["resync_required"] is True
        await ws.send_json({"type": "resync"})
        response = await ws.receive_json()
        assert response["type"] == "telemetry_snapshot"
        assert response["authoritative_financial_state"] is False
        assert response["stream_boot_id"] == hub.boot_id
        await ws.close()
    finally:
        await client.close()
