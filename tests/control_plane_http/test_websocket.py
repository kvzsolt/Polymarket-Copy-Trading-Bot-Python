"""Authenticated bounded telemetry WebSocket contracts."""

from __future__ import annotations

import asyncio

import pytest
from aiohttp import WSMsgType
from aiohttp.client_exceptions import WSServerHandshakeError
from aiohttp.test_utils import TestClient, TestServer

from integrations.control_plane import (
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


class _Subscription:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[TelemetryDelivery] = asyncio.Queue(maxsize=2)

    async def get(self) -> TelemetryDelivery:
        return await self.queue.get()


class _Hub:
    def __init__(self) -> None:
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


def _auth() -> ControlPlaneAuthenticator:
    return ControlPlaneAuthenticator(
        credentials=(
            BearerCredential(
                token=_TOKEN,
                subject="operator-a",
                permissions=frozenset(
                    {
                        ControlPlanePermission.TELEMETRY_READ,
                        ControlPlanePermission.TELEMETRY_STREAM,
                    }
                ),
            ),
        )
    )


async def _client(
    hub: _Hub,
    *,
    rate_limiter: InProcessActionRateLimiter | None = None,
) -> TestClient:
    app = create_control_plane_app(
        config=ControlPlaneHttpConfig(
            host="127.0.0.1",
            port=0,
            websocket_queue_capacity=2,
        ),
        authenticator=_auth(),
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
        assert ready == {"type": "stream_ready"}
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
