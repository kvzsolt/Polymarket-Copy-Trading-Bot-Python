"""Own the database-to-local-spool worker independently of remote delivery."""

import asyncio
from contextlib import suppress
from typing import Protocol


class FillEvidenceDrain(Protocol):
    """Transfer a bounded set of committed rows without remote I/O."""

    async def drain_once(self) -> int:
        """Retain original rows on failure and return the number durably copied."""
        ...


class DrainWaiter(Protocol):
    """Inject retry scheduling independently of domain and database work."""

    async def wait(self, stop: asyncio.Event, seconds: float) -> None:
        """Wait interruptibly until retry or shutdown."""
        ...


class AsyncDrainWaiter:
    """Production scheduler; tests inject explicit event barriers."""

    async def wait(self, stop: asyncio.Event, seconds: float) -> None:
        """Yield for shutdown or the configured bounded retry interval."""
        try:
            await asyncio.wait_for(stop.wait(), timeout=seconds)
        except TimeoutError:
            pass


class EvidenceDrainRuntime:
    """Keep database failures visible without coupling risk exits to reporting."""

    def __init__(self, drain: FillEvidenceDrain, *, waiter: DrainWaiter | None = None) -> None:
        """Inject the finite drain operation and optional deterministic scheduler."""
        self._drain, self._waiter = drain, waiter or AsyncDrainWaiter()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._status = "not_started"

    @property
    def status(self) -> str:
        """Expose delivery progress without exception text or account identifiers."""
        return self._status

    async def start(self) -> None:
        """Schedule the background worker without a database or network handshake."""
        if self._task is not None or self._stop.is_set():
            raise RuntimeError("evidence drain already started or closed")
        self._task = asyncio.create_task(self._run(), name="fill-evidence-drain")

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._status = "draining"
                count = await self._drain.drain_once()
                self._status = "copied" if count else "idle"
            except Exception:
                self._status = "retry"
                count = 0
            if count:
                await asyncio.sleep(0)
            else:
                await self._waiter.wait(self._stop, 1.0)

    async def aclose(self) -> None:
        """Release all database operations before the application closes its pool."""
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        self._status = "closed"
