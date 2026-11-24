"""Finite, injected pending evaluation with local evidence only and no execution port."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
import json
import re
from typing import Protocol

from pmcopy.domain.evidence import EvidenceEvent
from pmcopy.domain.grouped_detection import TraderDetectionPolicy
from pmcopy.domain.observed_rpc import ObservedRpcResult
from pmcopy.domain.pending_evaluation import (
    PendingEvaluationError,
    PendingEvaluationLimits,
    PendingObservation,
)
from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec
from pmcopy.integrations.pmrecon.order_verification import V2OrderVerifier
from pmcopy.integrations.pmrecon.pending_decoder import V2PendingCalldataDecoder


class PendingGateway(Protocol):
    """Use the shared admitted gateway's original pre-decode response receipt."""

    async def call_observed(
        self, provider: str, method: str, params: list[object], *, category: str = "pending"
    ) -> ObservedRpcResult:
        """Admit one body read before transport; no implicit retry."""
        ...


class PendingStreamAdmission(Protocol):
    """Charge already-delivered bytes, including irrelevant and malformed frames."""

    def charge_stream(self, provider: str, byte_count: int, *, category: str) -> None:
        """Record the full delivery cost before parsing."""
        ...


class PendingReceiptClock(Protocol):
    """Sample native receipt time rather than reconstructing nanoseconds from ms."""

    def capture(self) -> TransportReceipt:
        """Return this process's native wall/monotonic receipt pair."""
        ...


class PendingEvidenceSink(Protocol):
    """Append locally without coupling observation to Hungary network health."""

    def append_event(self, event: EvidenceEvent) -> object:
        """Durably persist the exact event before returning."""
        ...

    def invalidate(self, reason: str) -> None:
        """Latch loss if observation storage or source clock fails."""
        ...


class PendingDeadline(Protocol):
    """Inject an await boundary for deterministic timeout tests."""

    async def run(self, read: Awaitable[ObservedRpcResult], timeout_s: float) -> ObservedRpcResult:
        """Bound one in-flight body request and propagate cancellation."""
        ...


class AsyncioPendingDeadline:
    """Bound only this independent read-only request with the event-loop deadline."""

    async def run(self, read: Awaitable[ObservedRpcResult], timeout_s: float) -> ObservedRpcResult:
        """Cancel an overdue read without retry or fallback provider."""
        return await asyncio.wait_for(read, timeout_s)


@dataclass(slots=True)
class _TrackedHash:
    """Encapsulate bounded per-session state; all receipt facts are immutable."""

    transaction_hash: str
    receipt: TransportReceipt
    stage: str = "hash_observed"
    body: TransportReceipt | None = None
    source_event_id: str | None = None


class BoundedPendingEvaluator:
    """Consume one externally-owned grouped Alchemy hash feed in read-only mode."""

    def __init__(
        self,
        policies: Sequence[TraderDetectionPolicy],
        *,
        globally_enabled: bool,
        gateway: PendingGateway,
        stream_admission: PendingStreamAdmission,
        receipt_clock: PendingReceiptClock,
        evidence_sink: PendingEvidenceSink,
        limits: PendingEvaluationLimits | None = None,
        decoder: V2PendingCalldataDecoder | None = None,
        verifier: V2OrderVerifier | None = None,
        deadline: PendingDeadline | None = None,
    ) -> None:
        """Inject existing provider/account accounting and local durable evidence ownership."""
        if type(globally_enabled) is not bool:
            raise PendingEvaluationError("pending_invalid_enabled_flag")
        self._owners = frozenset(
            p.wallet
            for p in policies
            if globally_enabled and p.enabled and p.mode == "pending" and p.provider == "alchemy"
        )
        self._gateway, self._admission = gateway, stream_admission
        self._clock, self._sink = receipt_clock, evidence_sink
        self._limits = limits or PendingEvaluationLimits()
        self._decoder = decoder or V2PendingCalldataDecoder()
        self._verifier = verifier or V2OrderVerifier(self._decoder)
        self._deadline = deadline or AsyncioPendingDeadline()
        self._stop_reason: str | None = None
        self._started = self._clock.capture()
        self._native(self._started)
        self._tracked: dict[str, _TrackedHash] = {}
        self._tasks: set[asyncio.Task] = set()
        self._lookups = 0
        self._backoff_until = 0

    @property
    def stop_requested(self) -> bool:
        """The owner must close this pending-only stream when a bound is reached."""
        return self._stop_reason is not None

    @property
    def stop_reason(self) -> str | None:
        """Expose a safe bounded reason without provider errors or secrets."""
        return self._stop_reason

    @property
    def inflight(self) -> int:
        """Count work already admitted; no hidden body request queue exists."""
        return len(self._tasks)

    def observations(self) -> tuple[PendingObservation, ...]:
        """Return bounded summaries, leaving durable details in the outbox."""
        return tuple(
            PendingObservation(t.transaction_hash, t.stage, t.source_event_id)
            for t in self._tracked.values()
        )

    def _native(self, receipt: TransportReceipt, earlier: TransportReceipt | None = None) -> int:
        """Require native causal clocks rather than accepting a fabricated projection."""
        if receipt.wall_timestamp_ns is None or (
            earlier is not None
            and (
                receipt.elapsed_since(earlier) is None
                or earlier.wall_timestamp_ns is None
                or receipt.wall_timestamp_ns < earlier.wall_timestamp_ns
            )
        ):
            self._stop_reason = "evidence_unavailable"
            self._sink.invalidate("missing_native_clock")
            raise PendingEvaluationError("pending_invalid_source_clock")
        return receipt.wall_timestamp_ns

    def _append(self, value: dict | EvidenceEvent) -> EvidenceEvent:
        """Keep local write failure visible to the source owner."""
        try:
            event = (
                value if isinstance(value, EvidenceEvent) else PmreconEvidenceCodec.encode(value)
            )
            self._sink.append_event(event)
            return event
        except (ValueError, OSError, RuntimeError):
            self._sink.invalidate("storage_failure")
            self._stop_reason = "evidence_unavailable"
            raise PendingEvaluationError("pending_evidence_unavailable") from None

    def _stage(
        self,
        tracked: _TrackedHash,
        stage: str,
        receipt: TransportReceipt,
        *,
        raw: str | None = None,
        attributed: bool = False,
    ) -> str:
        """Persist an outcome using the actual later sample and separate source receipts."""
        ns = self._native(receipt, tracked.body or tracked.receipt)
        value: dict = {
            "kind": "pending_observation",
            "transaction_hash": tracked.transaction_hash,
            "stage": stage,
            "received_ns": str(ns),
            "clock_id": receipt.boot_id,
            "hash_received_ns": str(tracked.receipt.wall_timestamp_ns),
        }
        if tracked.body is not None:
            value["body_received_ns"] = str(tracked.body.wall_timestamp_ns)
        if tracked.source_event_id is not None:
            value["source_event_id"] = tracked.source_event_id
        if attributed:
            value["attributable_received_ns"] = str(ns)
        if raw is not None:
            value["raw_hash_frame"] = raw
        self._append(value)
        tracked.stage = stage
        return stage

    def _gap(self, receipt: TransportReceipt, reason: str) -> None:
        """Record stream-level coverage loss when there is no valid hash to attribute."""
        self._append(
            {
                "kind": "gap",
                "provider": "alchemy",
                "reason": reason,
                "received_ns": str(self._native(receipt)),
                "clock_id": receipt.boot_id,
            }
        )

    async def observe_frame(
        self,
        raw: str | bytes,
        receipt: TransportReceipt,
        *,
        expected_subscription: str | None = None,
    ) -> str:
        """Charge full bytes first, then perform at most one admitted lookup per distinct hash."""
        byte_count = len(raw) if isinstance(raw, bytes) else len(raw.encode("utf-8"))
        budget_failed = False
        try:
            self._admission.charge_stream("alchemy", byte_count, category="pending")
        except BudgetExhausted:
            budget_failed = True
        self._native(receipt, self._started)
        if not self._owners or self.stop_requested:
            return "stopped" if self.stop_requested else "disabled"
        if byte_count > self._limits.max_frame_bytes:
            self._stop_reason = "capacity_stopped"
            self._gap(receipt, "overflow")
            return self._stop_reason
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            payload = json.loads(raw)
            params = payload.get("params") if isinstance(payload, dict) else None
            tx = params.get("result") if isinstance(params, dict) else None
            if (
                not isinstance(payload, dict)
                or payload.get("jsonrpc") != "2.0"
                or payload.get("method") != "eth_subscription"
                or (
                    expected_subscription is not None
                    and (
                        not expected_subscription
                        or not isinstance(params, dict)
                        or params.get("subscription") != expected_subscription
                    )
                )
                or not isinstance(tx, str)
                or not re.fullmatch(r"0x[0-9a-fA-F]{64}", tx)
            ):
                if budget_failed:
                    self._stop_reason = "budget_stopped"
                    self._gap(receipt, "quota")
                return "ignored"
        except (ValueError, TypeError, RecursionError):
            self._stop_reason = "stream_invalid"
            self._gap(receipt, "disconnected")
            return "stream_invalid"
        tx = tx.lower()
        if tx in self._tracked:
            if budget_failed:
                self._stop_reason = "budget_stopped"
                return self._stage(self._tracked[tx], "budget_stopped", receipt)
            return "duplicate"
        tracked = _TrackedHash(tx, receipt)
        self._stage(tracked, "hash_observed", receipt, raw=raw)
        if budget_failed:
            self._stop_reason = "budget_stopped"
            return self._stage(tracked, "budget_stopped", receipt)
        if (
            len(self._tracked) >= self._limits.max_hashes
            or self.inflight >= self._limits.max_inflight
            or self._lookups >= self._limits.max_body_lookups
        ):
            self._stop_reason = "capacity_stopped"
            return self._stage(tracked, "capacity_stopped", receipt)
        self._tracked[tx] = tracked
        now = self._clock.capture()
        self._native(now, receipt)
        if now.monotonic_ns - self._started.monotonic_ns >= self._limits.duration_ns:
            self._stop_reason = "deadline_unknown"
            return self._stage(tracked, "deadline_unknown", now)
        if now.monotonic_ns < self._backoff_until:
            return self._stage(tracked, "body_unavailable", now)
        task = asyncio.current_task()
        assert task is not None
        self._tasks.add(task)
        self._lookups += 1
        try:
            result = await self._deadline.run(
                self._gateway.call_observed(
                    "alchemy", "eth_getTransactionByHash", [tx], category="pending"
                ),
                self._limits.body_timeout_ns / 1_000_000_000,
            )
        except asyncio.CancelledError:
            if tracked.stage == "hash_observed":
                self._stage(tracked, "deadline_unknown", self._clock.capture())
            raise
        except BudgetExhausted:
            self._stop_reason = "budget_stopped"
            return self._stage(tracked, "budget_stopped", self._clock.capture())
        except TimeoutError:
            self._backoff_until = self._clock.capture().monotonic_ns + self._limits.backoff_ns
            return self._stage(tracked, "deadline_unknown", self._clock.capture())
        except Exception:
            now = self._clock.capture()
            self._backoff_until = now.monotonic_ns + self._limits.backoff_ns
            return self._stage(tracked, "body_unavailable", now)
        else:
            return self._body(tracked, result)
        finally:
            self._tasks.discard(task)

    def _body(self, tracked: _TrackedHash, result: ObservedRpcResult) -> str:
        """Persist body evidence before decoding native owner orders and attribution."""
        self._native(result.receipt, tracked.receipt)
        tracked.body = result.receipt
        now = self._clock.capture()
        self._native(now, result.receipt)
        if result.payload is None:
            return self._stage(tracked, "body_unavailable", now)
        raw = result.payload
        try:
            if not isinstance(raw, dict) or raw.get("hash", "").lower() != tracked.transaction_hash:
                raise PendingEvaluationError("pending_invalid_body")
            nonce = raw.get("nonce")
            if not isinstance(nonce, str) or not re.fullmatch(
                r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]{0,63})", nonce
            ):
                raise PendingEvaluationError("pending_invalid_nonce")
            block_number = raw.get("blockNumber")
            if block_number is not None:
                if not isinstance(block_number, str) or not re.fullmatch(
                    r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]{0,63})", block_number
                ):
                    raise PendingEvaluationError("pending_invalid_block_number")
                block_number = int(block_number, 16)
            source = PmreconEvidenceCodec.encode(
                {
                    "kind": "pending_body_observed",
                    "transaction_hash": tracked.transaction_hash,
                    "target": raw.get("to"),
                    "sender": raw.get("from"),
                    "nonce": int(nonce, 16),
                    "calldata": raw.get("input"),
                    "hash_received_ns": str(tracked.receipt.wall_timestamp_ns),
                    "body_received_ns": str(result.receipt.wall_timestamp_ns),
                    "received_ns": str(result.receipt.wall_timestamp_ns),
                    "clock_id": result.receipt.boot_id,
                    "block_number": block_number,
                }
            )
        except (ValueError, TypeError, AttributeError):
            return self._stage(tracked, "body_invalid", now)
        self._append(source)
        tracked.source_event_id = source.event_id
        if block_number is not None:
            return self._stage(tracked, "body_invalid", now)
        if (
            tracked.stage == "deadline_unknown"
            or now.monotonic_ns - tracked.receipt.monotonic_ns >= self._limits.body_timeout_ns
        ):
            return self._stage(tracked, "deadline_unknown", now)
        try:
            self._decoder.decode(source.wire()["target"], source.wire()["calldata"])
            attributable = self._clock.capture()
            self._native(attributable, now)
            if (
                attributable.monotonic_ns - tracked.receipt.monotonic_ns
                >= self._limits.body_timeout_ns
            ):
                return self._stage(tracked, "deadline_unknown", attributable)
            verified = self._verifier.verify(source, attributable, owners=self._owners)
        except ValueError:
            return self._stage(tracked, "body_invalid", self._clock.capture())
        if not verified:
            # Body is durable; unrelated owners are not attributed to configured traders.
            tracked.stage = "untracked_owner"
            return tracked.stage
        for event in verified:
            self._append(event)
        return self._stage(tracked, "attributed", attributable, attributed=True)

    def expire(self) -> None:
        """Run from the owner's bounded heartbeat even when no more frames arrive."""
        now = self._clock.capture()
        self._native(now, self._started)
        if now.monotonic_ns - self._started.monotonic_ns >= self._limits.duration_ns:
            self._stop_reason = self._stop_reason or "deadline_unknown"
        for tracked in self._tracked.values():
            if (
                tracked.stage == "hash_observed"
                and now.monotonic_ns - tracked.receipt.monotonic_ns >= self._limits.body_timeout_ns
            ):
                self._stage(tracked, "deadline_unknown", now)

    async def aclose(self) -> None:
        """Cancel this evaluator's in-flight reads without waiting for Hungary or mined work."""
        self._stop_reason = self._stop_reason or "closed"
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
