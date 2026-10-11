"""Own process signals until asynchronous application cleanup has completed."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable, Coroutine
from types import FrameType

logger = logging.getLogger(__name__)

_SignalHandler = Callable[[int, FrameType | None], object] | int | None


class ApplicationRunner:
    """Cancel the application once and drain its event loop before restoring signals."""

    def __init__(self, *, loop_factory: Callable[[], asyncio.AbstractEventLoop]) -> None:
        """Inject the event-loop factory used by the synchronous process entrypoint."""
        self._loop_factory = loop_factory
        self._loop: asyncio.AbstractEventLoop | None = None
        self._task: asyncio.Task[int] | None = None
        self._shutdown_requested = False

    def run(self, application: Coroutine[object, object, int]) -> int:
        """Run an application on the main thread with cancellation-safe signal handling.

        Args:
            application: Application coroutine, including its resource cleanup.

        Returns:
            The application's exit code, or zero for an operator interrupt.

        Raises:
            Exception: Any application failure other than operator cancellation.
        """
        previous_handlers: dict[signal.Signals, _SignalHandler] = {}
        self._shutdown_requested = False
        try:
            with asyncio.Runner(loop_factory=self._loop_factory) as runner:
                self._loop = runner.get_loop()
                self._task = self._loop.create_task(application)
                for sig in (signal.SIGINT, signal.SIGTERM):
                    previous_handlers[sig] = signal.signal(sig, self._on_signal)
                return runner.run(self._await_application())
        except (asyncio.CancelledError, KeyboardInterrupt):
            return 0
        finally:
            for sig, handler in previous_handlers.items():
                if handler is not None:
                    signal.signal(sig, handler)
            self._task = None
            self._loop = None

    async def _await_application(self) -> int:
        assert self._task is not None
        return await self._task

    def _on_signal(self, signum: int, _frame: FrameType | None) -> None:
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._request_shutdown, signum)

    def _request_shutdown(self, signum: int) -> None:
        if self._shutdown_requested or self._task is None or self._task.done():
            return
        self._shutdown_requested = True
        logger.info(
            "application_shutdown_requested",
            extra={"signal": signal.Signals(signum).name},
        )
        self._task.cancel()
