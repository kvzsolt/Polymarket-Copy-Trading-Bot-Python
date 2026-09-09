"""Bounded WebSocket writes owned by the existing local control-plane adapter."""

import asyncio
from math import isfinite
from typing import Protocol

from integrations.control_plane.serialization import JsonValue


class JsonSocket(Protocol):
    """Narrow transport write contract for one already-authenticated socket."""

    async def send_json(self, data: JsonValue) -> None:
        """Serialize and drain one JSON message."""
        ...


class ControlPlaneStreamSender:
    """Cancel a stalled send so subscription cleanup can run deterministically."""

    def __init__(self, *, timeout_seconds: float) -> None:
        if (
            isinstance(timeout_seconds, bool)
            or not isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("Stream send timeout must be finite and positive")
        self._timeout_seconds = timeout_seconds

    async def send(self, socket: JsonSocket, payload: JsonValue) -> None:
        """Send one message, propagating cancellation and transport timeouts.

        Raises:
            TimeoutError: The transport failed to drain within its configured bound.
        """
        async with asyncio.timeout(self._timeout_seconds):
            await socket.send_json(payload)
