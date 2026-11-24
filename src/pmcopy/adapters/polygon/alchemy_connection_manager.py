"""Reconnect and resynchronize a multiplexed Alchemy WebSocket session."""

from __future__ import annotations

import asyncio
import contextlib
import random
from datetime import timedelta
from typing import Protocol, cast

from pmcopy.adapters.polygon.alchemy_json_rpc import (
    AlchemyJsonRpcSession,
    AlchemyRpcDisconnectedError,
    AlchemySubscriptionOptions,
)
from pmcopy.domain.source_transactions import EvmReceipt
from pmcopy.interfaces.evm_events import (
    AsyncSleeper,
    JsonRpcSocket,
    JsonRpcSocketConnector,
    ReconnectPolicy,
    ResynchronizingEvmEventSink,
)
from pmcopy.interfaces.source_transactions import Clock
from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("AlchemyConnectionManager")


class RandomSource(Protocol):
    """Generate reconnect jitter through an injected deterministic seam."""

    def uniform(self, lower: float, upper: float) -> float:
        """Return a value in the inclusive ``lower``/``upper`` range."""

        ...


class SystemRandomSource:
    """Production random source for non-security reconnect jitter."""

    def __init__(self) -> None:
        """Initialize an operating-system-seeded generator."""

        self._random = random.SystemRandom()

    def uniform(self, lower: float, upper: float) -> float:
        """Return reconnect jitter in the requested range."""

        return self._random.uniform(lower, upper)


class ExponentialReconnectPolicy:
    """Bounded exponential reconnect delay with injected jitter."""

    def __init__(
        self,
        random_source: RandomSource,
        minimum: timedelta = timedelta(seconds=1),
        maximum: timedelta = timedelta(seconds=30),
        jitter: timedelta = timedelta(milliseconds=500),
    ) -> None:
        """Initialize the reconnect policy.

        Args:
            random_source: Injected jitter source.
            minimum: Delay for the first reconnect attempt.
            maximum: Maximum exponential component.
            jitter: Maximum additional randomized delay.

        Raises:
            ValueError: If any duration is invalid.
        """

        if minimum <= timedelta(0):
            raise ValueError("minimum reconnect delay must be positive")
        if maximum < minimum:
            raise ValueError("maximum reconnect delay must not be less than minimum")
        if jitter < timedelta(0):
            raise ValueError("reconnect jitter must be non-negative")
        self._random_source = random_source
        self._minimum = minimum
        self._maximum = maximum
        self._jitter = jitter

    def delay_for(self, attempt: int) -> timedelta:
        """Return a bounded exponential delay for ``attempt``.

        Args:
            attempt: Zero-based consecutive reconnect attempt.

        Returns:
            Exponential delay plus bounded jitter.

        Raises:
            ValueError: If ``attempt`` is negative.
        """

        if attempt < 0:
            raise ValueError("attempt must be non-negative")
        exponential_seconds = min(
            self._minimum.total_seconds() * (2**attempt),
            self._maximum.total_seconds(),
        )
        jitter_seconds = self._random_source.uniform(0.0, self._jitter.total_seconds())
        return timedelta(seconds=exponential_seconds + jitter_seconds)


class AsyncioSleeper:
    """Production non-blocking reconnect waiter."""

    async def sleep(self, delay: timedelta) -> None:
        """Wait for the reconnect delay.

        Args:
            delay: Non-negative duration returned by the reconnect policy.
        """

        await asyncio.sleep(delay.total_seconds())


class WebsocketsJsonRpcConnector:
    """Open a bounded, keepalive-enabled WebSocket using ``websockets``."""

    async def connect(self, url: str) -> JsonRpcSocket:
        """Open one Alchemy WebSocket connection.

        Args:
            url: Secret-bearing provider URL; never logged by this adapter.

        Returns:
            Connected WebSocket implementing the narrow JSON-RPC transport.
        """

        from pmcopy.clients.ws.alchemy_multiplex_ws import (
            _NoRedirectConnect,
            _SILENT_WS_LOGGER,
        )

        socket = await _NoRedirectConnect(
            url,
            proxy=None,
            ping_interval=10,
            ping_timeout=10,
            close_timeout=5,
            max_queue=1000,
            logger=_SILENT_WS_LOGGER,
        )
        return cast(JsonRpcSocket, socket)


class AlchemyConnectionManager:
    """Keep one warm Alchemy session and resynchronize after reconnect."""

    def __init__(
        self,
        url: str,
        connector: JsonRpcSocketConnector,
        options: AlchemySubscriptionOptions,
        sink: ResynchronizingEvmEventSink,
        clock: Clock,
        reconnect_policy: ReconnectPolicy,
        sleeper: AsyncSleeper,
    ) -> None:
        """Initialize the connection lifecycle.

        Args:
            url: Alchemy WebSocket URL, retained privately and never logged.
            connector: Socket connection port.
            options: Subscription configuration.
            sink: Source event pipeline with reconnect reconciliation.
            clock: Deterministic event observation clock.
            reconnect_policy: Bounded exponential delay calculation.
            sleeper: Injectable asynchronous wait boundary.

        Raises:
            ValueError: If ``url`` is blank.
        """

        if not str(url or "").strip():
            raise ValueError("Alchemy WebSocket URL is required")
        self._url = url
        self._connector = connector
        self._options = options
        self._sink = sink
        self._clock = clock
        self._reconnect_policy = reconnect_policy
        self._sleeper = sleeper
        self._active_session: AlchemyJsonRpcSession | None = None
        self._task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()
        self._running = False

    @property
    def connected(self) -> bool:
        """Return whether a subscribed session currently serves RPC."""

        session = self._active_session
        return session is not None and session.running

    async def start(self) -> None:
        """Connect, subscribe, and resynchronize before returning."""

        if self._task is not None and not self._task.done():
            await self._ready.wait()
            return
        self._running = True
        self._ready.clear()
        self._task = asyncio.create_task(
            self._connection_loop(),
            name="alchemy-connection-manager",
        )
        await self._ready.wait()

    async def stop(self) -> None:
        """Stop reconnect attempts and close the active warm session."""

        self._running = False
        self._ready.set()
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        session = self._active_session
        self._active_session = None
        if session is not None:
            await session.stop()

    async def get_transaction_receipt(
        self,
        chain_id: int,
        transaction_hash: str,
    ) -> EvmReceipt | None:
        """Delegate receipt lookup to the current warm session.

        Args:
            chain_id: Expected Polygon chain id.
            transaction_hash: Source transaction hash.

        Returns:
            Mined receipt or ``None`` while unknown.

        Raises:
            AlchemyRpcDisconnectedError: If reconnect is in progress.
        """

        session = self._active_session
        if session is None or not session.running:
            raise AlchemyRpcDisconnectedError("Alchemy session is not connected")
        return await session.get_transaction_receipt(chain_id, transaction_hash)

    async def get_confirmed_nonce(self, chain_id: int, address: str) -> int:
        """Delegate confirmed nonce lookup to the current warm session.

        Args:
            chain_id: Expected Polygon chain id.
            address: Transaction sender address.

        Returns:
            Confirmed sender nonce.

        Raises:
            AlchemyRpcDisconnectedError: If reconnect is in progress.
        """

        session = self._active_session
        if session is None or not session.running:
            raise AlchemyRpcDisconnectedError("Alchemy session is not connected")
        return await session.get_confirmed_nonce(chain_id, address)

    async def _connection_loop(self) -> None:
        attempt = 0
        while self._running:
            session: AlchemyJsonRpcSession | None = None
            try:
                socket = await self._connector.connect(self._url)
                session = AlchemyJsonRpcSession(
                    socket=socket,
                    options=self._options,
                    sink=self._sink,
                    clock=self._clock,
                )
                await session.start()
                self._active_session = session
                await self._sink.resynchronize()
                self._ready.set()
                attempt = 0
                await session.wait_disconnected()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "alchemy_connection_cycle_failed | error=%s",
                    redact_error_detail(exc),
                )
            finally:
                if self._active_session is session:
                    self._active_session = None
                if session is not None:
                    await session.stop()
            if not self._running:
                break
            delay = self._reconnect_policy.delay_for(attempt)
            attempt += 1
            await self._sleeper.sleep(delay)


__all__ = [
    "AlchemyConnectionManager",
    "AsyncioSleeper",
    "ExponentialReconnectPolicy",
    "SystemRandomSource",
    "WebsocketsJsonRpcConnector",
]
