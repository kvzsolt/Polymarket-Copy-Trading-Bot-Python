"""One finite optional pending socket, independent of mined copying and remote delivery."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Protocol

from pmcopy.clients.network.grouped_websocket import StreamAdmission, StreamConnector, StreamSocket
from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.interfaces.source_time import ReceiptClock
from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec
from pmcopy.services.pending_evaluation import PendingEvidenceSink

EXCHANGES = (
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
)


class PendingEvaluator(Protocol):
    """Read-only observation port with no order submission or mined-loop controls."""

    @property
    def stop_requested(self) -> bool:
        """Whether any finite work or provider budget bound has been reached."""
        ...

    @property
    def stop_reason(self) -> str | None:
        """Return a safe public lifecycle reason."""
        ...

    async def observe_frame(
        self,
        raw: str | bytes,
        receipt: TransportReceipt,
        *,
        expected_subscription: str | None = None,
    ) -> str:
        """Charge the original frame before validating this socket's owned subscription."""
        ...

    def expire(self) -> None:
        """Apply the injected clock's session deadline during idle periods."""
        ...

    async def aclose(self) -> None:
        """Cancel only this optional evaluator's outstanding reads."""
        ...


class PendingReceiver(Protocol):
    """Inject the idle deadline so tests never sleep or depend on wall time."""

    async def receive(self, socket: StreamSocket, timeout_s: float) -> str | bytes:
        """Receive one original frame or signal an idle deadline."""
        ...


class AsyncPendingReceiver:
    """Wake an idle observer so the finite session cannot remain connected forever."""

    async def receive(self, socket: StreamSocket, timeout_s: float) -> str | bytes:
        """Bound a single receive without hiding reconnect attempts."""
        return await asyncio.wait_for(socket.recv(), timeout_s)


class PendingSender(Protocol):
    """Bound control-message backpressure independently of body lookup deadlines."""

    async def send(self, socket: StreamSocket, message: str, timeout_s: float) -> None:
        """Send once or fail without retry."""
        ...


class AsyncPendingSender:
    """Prevent a stalled subscription write from retaining the sampler indefinitely."""

    async def send(self, socket: StreamSocket, message: str, timeout_s: float) -> None:
        """Cancel an overdue WebSocket write using the injected transport boundary."""
        await asyncio.wait_for(socket.send(message), timeout_s)


class PendingEvaluationRuntime:
    """Own exactly one filtered Alchemy sampling connection with zero automatic restarts."""

    def __init__(
        self,
        *,
        evaluator: PendingEvaluator,
        connector: StreamConnector,
        admission: StreamAdmission,
        clock: ReceiptClock,
        evidence_sink: PendingEvidenceSink,
        status_changed: Callable[[], None],
        receiver: PendingReceiver | None = None,
        sender: PendingSender | None = None,
    ) -> None:
        """Inject the shared budget, local evidence and deterministic lifecycle seams."""
        self._evaluator, self._connector, self._admission = evaluator, connector, admission
        self._clock, self._sink, self._changed = clock, evidence_sink, status_changed
        self._receiver = receiver or AsyncPendingReceiver()
        self._sender = sender or AsyncPendingSender()
        self._task: asyncio.Task[None] | None = None
        self._status = "not_started"
        self._work: set[asyncio.Task[None]] = set()
        self._aborted = False
        self._cleanup_failed = False

    @property
    def status(self) -> str:
        """Return evaluation health without representing it as pending trade execution."""
        return self._status

    @property
    def cleanup_failed(self) -> bool:
        """Expose cleanup degradation without erasing a budget or evidence stop."""
        return self._cleanup_failed

    async def start(self) -> None:
        """Schedule one attempt; starting a completed sampler requires a new process owner."""
        if self._task is not None:
            raise RuntimeError("pending evaluation already started")
        self._task = asyncio.create_task(self._run(), name="bounded-pending-evaluation")

    async def _run(self) -> None:
        socket: StreamSocket | None = None
        try:
            self._admission.admit_rpc("alchemy", "eth_subscribe", category="pending")
            socket = await self._connector.open("alchemy")
            await self._sender.send(
                socket,
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "eth_subscribe",
                        "params": [
                            "alchemy_pendingTransactions",
                            {"toAddress": list(EXCHANGES), "hashesOnly": True},
                        ],
                    }
                ),
                8.0,
            )
            subscription = ""
            while not self._evaluator.stop_requested and not self._aborted:
                try:
                    raw = await self._receiver.receive(socket, 1.0)
                except TimeoutError:
                    self._evaluator.expire()
                    continue
                receipt = self._clock.capture()
                if not subscription:
                    await self._evaluator.observe_frame(raw, receipt, expected_subscription="")
                elif len(self._work) >= 4:
                    byte_count = len(raw.encode("utf-8")) if isinstance(raw, str) else len(raw)
                    self._admission.charge_stream("alchemy", byte_count, category="pending")
                    self._set_status("capacity_stopped")
                    self._aborted = True
                else:
                    task = asyncio.create_task(self._evaluate(raw, receipt, subscription))
                    self._work.add(task)
                    task.add_done_callback(self._work.discard)
                    await asyncio.sleep(0)
                if self._evaluator.stop_requested or self._aborted:
                    break
                message = json.loads(raw)
                if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
                    raise ValueError("invalid pending envelope")
                if not subscription:
                    value = message.get("result")
                    if (
                        type(message.get("id")) is not int
                        or message["id"] != 1
                        or "error" in message
                        or not isinstance(value, str)
                        or not 1 <= len(value) <= 256
                    ):
                        raise ValueError("invalid pending acknowledgment")
                    subscription = value
                    self._set_status("evaluating")
                    self._changed()
                else:
                    params = message.get("params")
                    if (
                        message.get("method") != "eth_subscription"
                        or not isinstance(params, dict)
                        or params.get("subscription") != subscription
                    ):
                        raise ValueError("unowned pending notification")
                self._evaluator.expire()
            if not self._aborted:
                self._set_status(self._evaluator.stop_reason or "stopped")
        except asyncio.CancelledError:
            self._set_status("stopped")
        except BudgetExhausted:
            self._set_status("budget_stopped")
        except Exception:
            self._set_status("source_unavailable")
        finally:
            self._changed()
            work = tuple(self._work)
            for task in work:
                task.cancel()
            if work:
                await asyncio.gather(*work, return_exceptions=True)
            try:
                receipt = self._clock.capture()
                if receipt.wall_timestamp_ns is None:
                    raise ValueError("missing native clock")
                self._sink.append_event(
                    PmreconEvidenceCodec.encode(
                        {
                            "kind": "gap",
                            "provider": "alchemy",
                            "reason": "quota"
                            if self._status == "budget_stopped"
                            else "disconnected",
                            "received_ns": str(receipt.wall_timestamp_ns),
                            "clock_id": receipt.boot_id,
                        }
                    )
                )
            except (ValueError, OSError, RuntimeError):
                self._sink.invalidate("storage_failure")
                self._cleanup_failed = True
                if self._status != "budget_stopped":
                    self._set_status("evidence_unavailable")
            await self._evaluator.aclose()
            if socket is not None:
                try:
                    async with asyncio.timeout(3):
                        await socket.close()
                except Exception:
                    self._cleanup_failed = True
            self._changed()

    async def _evaluate(
        self, raw: str | bytes, receipt: TransportReceipt, subscription: str
    ) -> None:
        """Process at most four bodies while the owner continues reading original receipts."""
        try:
            await self._evaluator.observe_frame(raw, receipt, expected_subscription=subscription)
            if self._evaluator.stop_requested:
                self._set_status(self._evaluator.stop_reason or "stopped")
                self._changed()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._set_status("source_unavailable")
            self._aborted = True
            self._changed()

    def _set_status(self, status: str) -> None:
        """Keep the budget pause binding across concurrent reads and cleanup failures."""
        if self._status != "budget_stopped":
            self._status = status

    def request_stop(self) -> None:
        """Revoke optional observation immediately when the watched roster changes."""
        if self._task is not None and not self._task.done():
            self._task.cancel()

    async def wait(self) -> None:
        """Wait for this finite owner without joining any trading task."""
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)

    async def stop(self) -> None:
        """Release this socket and evaluator while leaving mined and risk owners independent."""
        self.request_stop()
        await self.wait()
