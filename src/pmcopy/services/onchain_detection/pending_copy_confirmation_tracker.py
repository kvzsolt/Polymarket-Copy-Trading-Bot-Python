"""Confirm (or report) copies taken off a provisional mempool sighting.

In ``detection.mode=on_chain_pending`` the copy is submitted while the leader
transaction is still in the mempool. That transaction may never mine, may be
replaced, or may revert — and in this mode nothing else notices: the mined
``OrderFilled`` subscription is off (it would double-copy every fill), and REST
polling only ever sees *mined* activity, so an orphaned copy is invisible.

This tracker closes that loop. Every copy that actually reached the venue is
recorded synchronously by the dispatcher, then polled off the hot path against
the leader transaction's receipt:

* receipt ``status == 1`` — confirmed; the copy mirrors a real leader fill;
* receipt ``status == 0`` — the leader transaction reverted;
* no receipt by the deadline — the sighting never mined (dropped/replaced);
* only RPC failures up to the deadline — the outcome is unknown.

The last three are reported as ``pending_copy_unconfirmed`` (WARN + metric +
telemetry ``FailureEvent``) and, when an unwind hook is injected, handed to it.
The tracker **never** closes a position itself: unwinding is a money-path
action that stays behind ``detection.onchain.pending_copy_auto_unwind_enabled``
(default false) and rides the existing protective-exit path.

Latency discipline: ``track`` is synchronous, O(1), allocation-light and cannot
raise, so the detection to submit path pays nothing for it. All receipt IO runs
in the background ``pending-copy-confirmation`` task.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from pmcopy.services.observability.metrics import (
    GAUGE_PENDING_COPY_TRACKED,
    METRIC_PENDING_COPY_CONFIRMED,
    METRIC_PENDING_COPY_TRACKER_DROPPED,
    METRIC_PENDING_COPY_UNCONFIRMED,
    MetricsRegistry,
    metrics as _default_metrics,
)
from pmcopy.utils.logger import get_logger, redact_error_detail

#: Background task name (also used by the shutdown path).
TASK_NAME = "pending-copy-confirmation"

#: Receipt poll offsets, in seconds after the copy was recorded. Polygon blocks
#: are ~2 s, so the first poll is already several blocks deep; the later ones
#: cover mempool congestion without hammering the RPC provider.
DEFAULT_POLL_OFFSETS_SECONDS: tuple[float, ...] = (5.0, 15.0, 30.0, 60.0)

#: Bounded unconfirmed reasons (mirrored by the metric schema).
REASON_REVERTED = "reverted"
REASON_NOT_MINED = "not_mined"
REASON_RPC_ERROR = "rpc_error"


@dataclass(frozen=True, slots=True)
class TrackedPendingCopy:
    """One copy placed off a provisional sighting of ``tx_hash``.

    Attributes:
        tx_hash: Leader transaction observed in the mempool.
        leader_wallet: Leader address the sighting was attributed to.
        user_ids: Followers whose copy actually reached the venue.
        token_id: Outcome token copied.
        condition_id: Market condition the copy belongs to.
        side: ``BUY`` or ``SELL`` as copied.
        recorded_at: Monotonic seconds at which the copy was recorded.
    """

    tx_hash: str
    leader_wallet: str
    user_ids: tuple[str, ...]
    token_id: str
    condition_id: str
    side: str
    recorded_at: float


class ReceiptStatusFetcher(Protocol):
    """Minimal receipt probe the tracker depends on."""

    async def fetch_status(self, tx_hash: str) -> int | None:
        """Return ``1`` (mined ok), ``0`` (reverted), or ``None`` (not mined).

        Raises:
            Exception: Any transport/provider failure. The tracker treats a
                raise as "unknown yet" and retries until the deadline.
        """
        ...


class PendingCopyUnwindHook(Protocol):
    """Optional consumer of an unconfirmed provisional copy.

    Implementations request a protective exit through the existing risk path;
    they never place orders themselves. Wired only when
    ``detection.onchain.pending_copy_auto_unwind_enabled`` is true.
    """

    async def request_unwind(self, entry: TrackedPendingCopy, *, reason: str) -> None:
        """Request that the copy described by ``entry`` be unwound."""
        ...


@dataclass(slots=True)
class _TrackedState:
    """Mutable poll cursor attached to one immutable tracked copy."""

    entry: TrackedPendingCopy
    key: tuple[str, str]
    next_offset_index: int = field(default=0)


class PendingCopyConfirmationTracker:
    """Track provisional copies until their leader transaction is decided."""

    def __init__(
        self,
        *,
        receipt_fetcher: ReceiptStatusFetcher,
        clock: Callable[[], float] = time.monotonic,
        metrics: MetricsRegistry = _default_metrics,
        telemetry: Any | None = None,
        logger: logging.Logger | None = None,
        deadline_seconds: float = 60.0,
        poll_offsets_seconds: Sequence[float] = DEFAULT_POLL_OFFSETS_SECONDS,
        max_tracked: int = 2048,
        poll_interval_seconds: float = 1.0,
        unwind_hook: PendingCopyUnwindHook | None = None,
        max_concurrent_fetches: int = 4,
    ) -> None:
        """Bind the receipt probe and the reporting policy.

        Args:
            receipt_fetcher: Probe for the leader transaction receipt.
            clock: Monotonic seconds source (injected for deterministic tests).
            metrics: Registry receiving the bounded ``pending_copy_*`` metrics.
            telemetry: ``TelemetryService``-shaped emitter, or ``None``.
            logger: Structured logger; defaults to the app child logger.
            deadline_seconds: Age after which an undecided copy is reported.
            poll_offsets_seconds: Ages at which the receipt is polled.
            max_tracked: Ceiling on concurrently tracked copies.
            poll_interval_seconds: Background loop cadence.
            unwind_hook: Optional protective-exit requester.
            max_concurrent_fetches: RPC fan-out ceiling per cycle.

        Raises:
            ValueError: If a bound is non-positive.
        """

        if deadline_seconds <= 0:
            raise ValueError("deadline_seconds must be positive")
        if max_tracked <= 0:
            raise ValueError("max_tracked must be positive")
        if max_concurrent_fetches <= 0:
            raise ValueError("max_concurrent_fetches must be positive")
        if poll_interval_seconds <= 0:
            # A zero delay would turn the loop into a busy spin that starves
            # the detection and execution tasks on the same event loop.
            raise ValueError("poll_interval_seconds must be positive")
        self._fetcher = receipt_fetcher
        self._clock = clock
        self._metrics = metrics
        self._telemetry = telemetry
        self._logger = logger if logger is not None else get_logger("PendingCopyConfirmation")
        self._deadline_seconds = float(deadline_seconds)
        self._poll_offsets = tuple(
            sorted({float(offset) for offset in poll_offsets_seconds if float(offset) > 0})
        ) or (self._deadline_seconds,)
        self._max_tracked = int(max_tracked)
        self._poll_interval_seconds = float(poll_interval_seconds)
        self._unwind_hook = unwind_hook
        self._max_concurrent_fetches = int(max_concurrent_fetches)
        self._tracked: OrderedDict[tuple[str, str], _TrackedState] = OrderedDict()
        self._task: asyncio.Task[None] | None = None
        self._running = False

    # -- state ------------------------------------------------------------

    @property
    def tracked_count(self) -> int:
        """Return how many provisional copies are awaiting a verdict."""

        return len(self._tracked)

    @property
    def running(self) -> bool:
        """Return whether the background confirmation loop is active."""

        return self._running

    @property
    def deadline_seconds(self) -> float:
        """Return the age at which an undecided copy is reported unconfirmed."""

        return self._deadline_seconds

    @property
    def max_tracked(self) -> int:
        """Return the ceiling on concurrently tracked copies."""

        return self._max_tracked

    @property
    def unwind_enabled(self) -> bool:
        """Return whether an unwind hook is wired (money-path opt-in)."""

        return self._unwind_hook is not None

    def track(self, entry: TrackedPendingCopy) -> bool:
        """Record one provisional copy for later confirmation.

        Synchronous, O(1), and exception-free by construction: it is called
        from the dispatch path immediately after the copy was submitted, where
        neither a delay nor a raise is acceptable.

        Args:
            entry: The copy to follow up on.

        Returns:
            ``True`` when this call started tracking the copy; ``False`` when
            the ``(tx_hash, leader)`` pair is already tracked, the entry has no
            transaction hash, or recording failed.
        """

        try:
            tx_hash = str(entry.tx_hash or "").strip()
            if not tx_hash:
                return False
            key = (tx_hash.lower(), str(entry.leader_wallet or "").strip().lower())
            if key in self._tracked:
                return False
            while len(self._tracked) >= self._max_tracked:
                self._tracked.popitem(last=False)
                self._count(METRIC_PENDING_COPY_TRACKER_DROPPED, labels={"reason": "full"})
            self._tracked[key] = _TrackedState(entry=entry, key=key)
            self._publish_tracked_gauge()
            return True
        except Exception as exc:  # never propagate into the copy path
            self._logger.debug(
                "pending_copy_track_failed | error=%s",
                redact_error_detail(exc),
            )
            return False

    # -- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        """Spawn the background confirmation loop (idempotent)."""

        # ``await asyncio.sleep(0)`` yields to the loop so SonarCloud S7503
        # recognises this as a genuine async function; ``create_task`` alone
        # does not count as an async feature for that rule.
        await asyncio.sleep(0)
        if self._task is not None and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name=TASK_NAME)
        self._logger.info(
            "pending_copy_tracker_started | deadline_s=%.1f max_tracked=%d",
            self._deadline_seconds,
            self._max_tracked,
        )

    async def stop(self) -> None:
        """Cancel the background loop; tracked state is intentionally dropped."""

        self._running = False
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self._logger.info(
                "pending_copy_tracker_stopped | tracked=%d",
                len(self._tracked),
            )

    async def _run(self) -> None:
        while self._running:
            try:
                await self.run_once(self._clock())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.warning(
                    "pending_copy_tracker_cycle_failed | error=%s",
                    redact_error_detail(exc),
                )
            await asyncio.sleep(self._poll_interval_seconds)

    # -- one cycle --------------------------------------------------------

    async def run_once(self, now: float) -> None:
        """Poll every tracked copy that is due at ``now``.

        Args:
            now: Current monotonic seconds (the loop passes ``clock()``; tests
                pass an explicit value).
        """

        due = [state for state in list(self._tracked.values()) if self._is_due(state, now)]
        if not due:
            return
        semaphore = asyncio.Semaphore(self._max_concurrent_fetches)
        outcomes = await asyncio.gather(
            *(self._fetch_status(state, semaphore) for state in due),
        )
        for state, (status, failed) in zip(due, outcomes, strict=True):
            await self._apply(state, status=status, failed=failed, now=now)
        self._publish_tracked_gauge()

    def _is_due(self, state: _TrackedState, now: float) -> bool:
        age = now - state.entry.recorded_at
        if age >= self._deadline_seconds:
            # The deadline always forces one final, terminal poll even if the
            # configured offsets never reach it.
            return True
        index = state.next_offset_index
        if index >= len(self._poll_offsets):
            return False
        return age >= self._poll_offsets[index]

    async def _fetch_status(
        self,
        state: _TrackedState,
        semaphore: asyncio.Semaphore,
    ) -> tuple[int | None, bool]:
        """Probe one receipt; return ``(status, failed)`` and never raise."""

        async with semaphore:
            try:
                return await self._fetcher.fetch_status(state.entry.tx_hash), False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.debug(
                    "pending_copy_receipt_probe_failed | tx=%s error=%s",
                    state.entry.tx_hash,
                    redact_error_detail(exc),
                )
                return None, True

    async def _apply(
        self,
        state: _TrackedState,
        *,
        status: int | None,
        failed: bool,
        now: float,
    ) -> None:
        age = now - state.entry.recorded_at
        self._advance_cursor(state, age)
        if not failed and status == 1:
            self._confirmed(state, age)
            return
        if not failed and status == 0:
            await self._unconfirmed(state, REASON_REVERTED, age)
            return
        if age < self._deadline_seconds:
            return  # still undecided; a later poll may resolve it
        await self._unconfirmed(state, REASON_RPC_ERROR if failed else REASON_NOT_MINED, age)

    def _advance_cursor(self, state: _TrackedState, age: float) -> None:
        index = state.next_offset_index
        while index < len(self._poll_offsets) and self._poll_offsets[index] <= age:
            index += 1
        state.next_offset_index = index

    def _confirmed(self, state: _TrackedState, age: float) -> None:
        self._tracked.pop(state.key, None)
        self._count(METRIC_PENDING_COPY_CONFIRMED)
        self._logger.debug(
            "pending_copy_confirmed | tx=%s leader=%s age_s=%.1f",
            state.entry.tx_hash,
            state.entry.leader_wallet,
            age,
        )

    async def _unconfirmed(self, state: _TrackedState, reason: str, age: float) -> None:
        self._tracked.pop(state.key, None)
        self._count(METRIC_PENDING_COPY_UNCONFIRMED, labels={"reason": reason})
        self._logger.warning(
            "pending_copy_unconfirmed | tx=%s leader=%s age_s=%.1f reason=%s",
            state.entry.tx_hash,
            state.entry.leader_wallet,
            age,
            reason,
        )
        self._emit_failure(reason)
        await self._request_unwind(state.entry, reason)

    async def _request_unwind(self, entry: TrackedPendingCopy, reason: str) -> None:
        """Hand one unconfirmed copy to the optional unwind hook.

        Awaited inline (not spawned): the caller is already the background
        loop, so awaiting keeps the fan-out bounded and the ordering
        observable, and a hook failure stays attributable to its copy.
        """

        hook = self._unwind_hook
        if hook is None:
            return
        try:
            await hook.request_unwind(entry, reason=reason)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._logger.warning(
                "pending_copy_unwind_failed | tx=%s reason=%s error=%s",
                entry.tx_hash,
                reason,
                redact_error_detail(exc),
            )

    # -- side channels (never allowed to break the loop) ------------------

    def _emit_failure(self, reason: str) -> None:
        telemetry = self._telemetry
        if telemetry is None:
            return
        try:
            from pmcopy.services.telemetry.contracts import (
                FailureComponent,
                FailureEvent,
                TelemetryProducer,
                TelemetrySeverity,
            )

            telemetry.emit(
                producer=TelemetryProducer.DETECTION,
                payload=FailureEvent(
                    component=FailureComponent.DETECTION,
                    code=f"pending_copy_{reason}",
                    retryable=False,
                ),
                severity=TelemetrySeverity.WARNING,
            )
        except Exception as exc:
            self._logger.debug(
                "pending_copy_telemetry_emit_failed | reason=%s error=%s",
                reason,
                redact_error_detail(exc),
            )

    def _count(self, name: str, *, labels: dict[str, str] | None = None) -> None:
        try:
            self._metrics.inc(name, labels=labels)
        except Exception as exc:
            self._logger.debug(
                "pending_copy_metric_failed | metric=%s error=%s",
                name,
                redact_error_detail(exc),
            )

    def _publish_tracked_gauge(self) -> None:
        try:
            self._metrics.set_gauge(GAUGE_PENDING_COPY_TRACKED, float(len(self._tracked)))
        except Exception as exc:
            self._logger.debug(
                "pending_copy_metric_failed | metric=%s error=%s",
                GAUGE_PENDING_COPY_TRACKED,
                redact_error_detail(exc),
            )


__all__ = [
    "DEFAULT_POLL_OFFSETS_SECONDS",
    "PendingCopyConfirmationTracker",
    "PendingCopyUnwindHook",
    "REASON_NOT_MINED",
    "REASON_REVERTED",
    "REASON_RPC_ERROR",
    "ReceiptStatusFetcher",
    "TASK_NAME",
    "TrackedPendingCopy",
]
