"""Transport limits preserve integer precision and release stalled stream writers."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from integrations.control_plane import ControlPlaneJsonSerializer


@pytest.mark.parametrize("number", [2**53, -(2**53), 10**70])
def test_large_counters_and_token_ids_cross_json_as_exact_strings(number):
    """JavaScript cannot represent these integers as JSON numbers exactly."""
    assert ControlPlaneJsonSerializer().to_jsonable({"counter": number}) == {"counter": str(number)}


@pytest.mark.asyncio
async def test_stream_send_has_a_bounded_transport_wait():
    """A never-draining transport must be cancelled, without timing assertions."""
    from integrations.control_plane.stream_sender import ControlPlaneStreamSender

    cancelled = asyncio.Event()

    async def never_drain(payload):
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    socket = AsyncMock()
    socket.send_json.side_effect = never_drain
    sender = ControlPlaneStreamSender(timeout_seconds=0.001)
    with pytest.raises(TimeoutError):
        await sender.send(socket, {"type": "stream_ready"})
    assert cancelled.is_set()


def test_telemetry_hub_owns_an_injected_session_identity():
    from services.telemetry.hub import TelemetryHub

    hub = TelemetryHub(boot_id="boot-1")
    assert hub.boot_id == "boot-1"
