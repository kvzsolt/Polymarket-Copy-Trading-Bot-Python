"""Background at-least-once evidence delivery, independent of trading operations."""

import asyncio
import json

from pmcopy.domain.evidence import EvidenceError
from pmcopy.interfaces.evidence import EvidenceOutbox, EvidenceTransport


class EvidencePublisher:
    """Send retained exact payloads and trust only complete canonical acknowledgements."""

    def __init__(
        self,
        outbox: EvidenceOutbox,
        transport: EvidenceTransport,
        *,
        batch_size: int = 100,
        retry_delay: float = 5.0,
    ) -> None:
        """Inject local durability and bounded network transport; no connection opens here."""
        if type(batch_size) is not int or not 1 <= batch_size <= 100:
            raise EvidenceError("evidence_invalid_batch_limit")
        if not 0.1 <= retry_delay <= 300:
            raise EvidenceError("evidence_invalid_retry_delay")
        self._outbox, self._transport = outbox, transport
        self._batch_size, self._retry_delay = batch_size, retry_delay
        self._lock = asyncio.Lock()
        self._delivery_status = "not_started"

    @property
    def delivery_status(self) -> str:
        """Describe delivery independently of whether local observation evidence is intact."""
        return self._delivery_status

    def _result(self, status: str) -> str:
        """Retain a bounded public status without response payloads or network errors."""
        self._delivery_status = status
        return status

    async def publish_once(self) -> str:
        """Deliver one FIFO prefix; ambiguity retains exact bytes for idempotent retry."""
        async with self._lock:
            if self._outbox.health().blocked:
                return self._result("blocked")
            batch = self._outbox.peek(self._batch_size)
            if not batch:
                return self._result("idle")
            body = json.dumps(
                {"events": [row.event.wire() for row in batch]},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode()
            try:
                self._delivery_status = "publishing"
                response = await self._transport.post(body)
            except (OSError, TimeoutError, EvidenceError):
                return self._result("retry")
            if response.status in (400, 401, 403, 404, 409, 413, 422):
                reason = "remote_conflict" if response.status == 409 else "remote_rejected"
                self._outbox.quarantine(batch, reason)
                return self._result("blocked")
            if response.status != 201:
                return self._result("retry")
            try:
                self._outbox.acknowledge(batch, response.body)
            except ValueError:
                return self._result("retry")
            return self._result("acknowledged")

    async def run(self, stop: asyncio.Event) -> None:
        """Run outside the trading path, waiting interruptibly after idle/error cycles."""
        while not stop.is_set():
            result = await self.publish_once()
            if result == "acknowledged":
                await asyncio.sleep(0)
                continue
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._retry_delay)
            except TimeoutError:
                pass

    async def aclose(self) -> None:
        """Close network resources; lifecycle owner stops the run task separately."""
        await self._transport.aclose()
