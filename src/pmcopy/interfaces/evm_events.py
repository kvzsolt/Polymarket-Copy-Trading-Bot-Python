"""Ports for EVM subscription transport and normalized event delivery."""

from __future__ import annotations

from datetime import timedelta
from typing import Protocol

from pmcopy.domain.source_transactions import EvmHead


class JsonRpcSocket(Protocol):
    """Minimal full-duplex JSON-RPC WebSocket transport."""

    async def send(self, payload: str) -> None:
        """Send one UTF-8 JSON-RPC frame."""

        ...

    async def recv(self) -> str | bytes:
        """Receive one UTF-8 JSON-RPC frame."""

        ...

    async def close(self) -> None:
        """Close the WebSocket transport."""

        ...


class EvmEventSink(Protocol):
    """Consume normalized Alchemy subscription events."""

    async def on_pending_transaction(self, transaction: dict[str, object]) -> None:
        """Handle one full pending transaction object."""

        ...

    async def on_new_head(self, head: EvmHead) -> None:
        """Handle one normalized block head."""

        ...

    async def on_log(self, log: dict[str, object]) -> None:
        """Handle one raw subscribed EVM log."""

        ...

    async def on_disconnect(self) -> None:
        """Fail closed after an unexpected transport loss."""

        ...


class ResynchronizingEvmEventSink(EvmEventSink, Protocol):
    """EVM event sink that can reconcile in-flight state after reconnect."""

    async def resynchronize(self) -> None:
        """Resolve source state before the reconnected stream is trusted."""

        ...


class JsonRpcSocketConnector(Protocol):
    """Open a connected JSON-RPC WebSocket transport."""

    async def connect(self, url: str) -> JsonRpcSocket:
        """Return a connected socket for ``url``."""

        ...


class ReconnectPolicy(Protocol):
    """Calculate bounded reconnect delays."""

    def delay_for(self, attempt: int) -> timedelta:
        """Return the delay before reconnect attempt ``attempt``."""

        ...


class AsyncSleeper(Protocol):
    """Wait asynchronously for a supplied duration."""

    async def sleep(self, delay: timedelta) -> None:
        """Wait for ``delay`` without blocking the event loop."""

        ...


class EvmConnectionManager(Protocol):
    """Own the lifecycle of a reconnecting EVM event/RPC session."""

    async def start(self) -> None:
        """Connect, subscribe, and resynchronize before returning."""

        ...

    async def stop(self) -> None:
        """Stop reconnect attempts and close the active session."""

        ...
