"""Finite historical protocol evidence collection over the shared admitted gateway."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
import json
from typing import Protocol

from pmcopy.domain.evidence import EvidenceEvent
from pmcopy.domain.history_import import HistoryImportError, HistoryImportPlan
from pmcopy.domain.observed_rpc import ObservedRpcResult
from pmcopy.domain.provider_budget import BudgetExhausted
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec
from pmcopy.integrations.pmrecon.protocol_codec import ProtocolBoundary
from pmcopy.interfaces.source_time import ReceiptClock
from pmcopy.services.history_state_reads import HistoryStateReadAdapter
from pmcopy.services.provider_gateway import ProviderUnavailable


class HistoryGateway(Protocol):
    """Every original read is admitted once by the existing shared provider ledger."""

    async def call_observed(
        self, provider: str, method: str, params: list[object], *, category: str
    ) -> ObservedRpcResult:
        """Return original source availability time, never a reconstructed timestamp."""
        ...


class HistoryJournal(Protocol):
    """Durable source cache and atomic complete-chunk boundary."""

    @property
    def cursor(self) -> int: ...
    @property
    def request_count(self) -> int: ...
    @property
    def complete(self) -> bool: ...
    def reserve_request(self, cost: int) -> None: ...
    def response(self, slot: str) -> ObservedRpcResult | None: ...
    def record_response(self, slot: str, observation: ObservedRpcResult) -> None: ...
    def header(self, number: int) -> tuple[str, str] | None: ...
    def commit_chunk(
        self,
        expected_cursor: int,
        end: int,
        events: Sequence[EvidenceEvent],
        headers: Sequence[tuple[int, str, str]],
    ) -> None: ...
    def references(self) -> dict[str, list[str]]: ...
    def coverage(self, status_key: str) -> EvidenceEvent | None: ...
    def append_coverage(self, event: EvidenceEvent, status_key: str, *, complete: bool) -> None: ...


@dataclass(frozen=True, slots=True)
class HistoryImportOutcome:
    """Completion of collection is separate from independent protocol reconciliation."""

    complete: bool
    reason: str | None
    cursor: int
    coverage_event_id: str


@dataclass(frozen=True, slots=True)
class PinnedHistoryHeader:
    """Raw original header plus its normalized comparison coordinates."""

    number: int
    block_hash: str
    parent_hash: str
    event: EvidenceEvent


class HistoryImportCollector:
    """Collect whole approved-emitter ranges without any trading or remote publishing."""

    def __init__(
        self,
        plan: HistoryImportPlan,
        journal: HistoryJournal,
        gateway: HistoryGateway,
        *,
        rpc_cost: Callable[[str, str], int],
        clock: ReceiptClock,
    ) -> None:
        """Inject every I/O owner; plan preview requires none of these collaborators."""
        self._plan, self._journal, self._gateway = plan, journal, gateway
        self._price, self._clock = rpc_cost, clock

    async def _rpc(
        self, slot: str, method: str, params: list[object], *, refresh: bool = False
    ) -> ObservedRpcResult:
        if not refresh and (cached := self._journal.response(slot)) is not None:
            return cached
        if refresh:
            slot += f":attempt:{self._journal.request_count + 1}"
        self._journal.reserve_request(self._price(self._plan.provider, method))
        response = await self._gateway.call_observed(
            self._plan.provider, method, params, category="backfill"
        )
        self._journal.record_response(slot, response)
        return response

    def _event(
        self, kind: str, source_receipt: TransportReceipt, **fields: object
    ) -> EvidenceEvent:
        if source_receipt.wall_timestamp_ns is None:
            raise HistoryImportError("invalid_evidence")
        return PmreconEvidenceCodec.encode(
            {
                "kind": kind,
                "schema_version": 2,
                "provider": self._plan.provider,
                "chain_id": 137,
                "received_ns": str(source_receipt.wall_timestamp_ns),
                "clock_id": source_receipt.boot_id,
                "recovery": True,
                **fields,
            }
        )

    async def _header(
        self, number: int, slot: str, *, refresh: bool = False
    ) -> PinnedHistoryHeader:
        observed = await self._rpc(
            slot, "eth_getBlockByNumber", [hex(number), False], refresh=refresh
        )
        raw = observed.payload
        if not isinstance(raw, dict) or ProtocolBoundary.quantity(raw.get("number")) != number:
            raise HistoryImportError("invalid_evidence")
        event = self._event(
            "block_header",
            observed.receipt,
            header={key: raw.get(key) for key in ("number", "hash", "parentHash", "timestamp")},
            raw_header=raw,
        )
        return PinnedHistoryHeader(
            number,
            ProtocolBoundary.digest(raw["hash"]),
            ProtocolBoundary.digest(raw["parentHash"]),
            event,
        )

    async def _state(self, header: PinnedHistoryHeader) -> list[EvidenceEvent]:
        events = []
        for index, read in enumerate(self._plan.state_reads):
            if read.at == "boundaries" and header.number not in {
                self._plan.from_block - 1,
                self._plan.to_block,
            }:
                continue
            method, params = HistoryStateReadAdapter.request(read, header.block_hash)
            response = await self._rpc(f"state:{header.number}:{index}", method, params)
            events.append(
                self._event(
                    "contract_state",
                    response.receipt,
                    contract=read.contract,
                    block_number=header.number,
                    block_hash=header.block_hash,
                    method=read.method,
                    arguments=list(read.arguments),
                    value=HistoryStateReadAdapter.value(read, response.payload),
                )
            )
        return events

    @staticmethod
    def _log_key(log: dict[str, object]) -> tuple[str, int, str]:
        return (
            ProtocolBoundary.digest(log["blockHash"]),
            ProtocolBoundary.quantity(log["logIndex"]),
            ProtocolBoundary.digest(log["transactionHash"]),
        )

    @staticmethod
    def _log_economics(log: dict[str, object]) -> tuple[object, ...]:
        topics = log["topics"]
        if not isinstance(topics, list):
            raise HistoryImportError("invalid_evidence")
        return (
            str(log["address"]).lower(),
            tuple(str(value).lower() for value in topics),
            str(log["data"]).lower(),
            ProtocolBoundary.quantity(log["blockNumber"]),
        )

    async def _logs(
        self, start: int, end: int, headers: dict[int, PinnedHistoryHeader]
    ) -> list[EvidenceEvent]:
        response = await self._rpc(
            f"logs:{start}:{end}",
            "eth_getLogs",
            [{"address": list(self._plan.contracts), "fromBlock": hex(start), "toBlock": hex(end)}],
        )
        if not isinstance(response.payload, list) or len(response.payload) > self._plan.max_rows:
            raise HistoryImportError("row_limit")
        logs: dict[tuple[str, int, str], dict[str, object]] = {}
        events = []
        for log in response.payload:
            ProtocolBoundary.log(log)
            number = ProtocolBoundary.quantity(log["blockNumber"])
            key = self._log_key(log)
            if (
                log["address"].lower() not in self._plan.contracts
                or number not in headers
                or key[0] != headers[number].block_hash
                or log.get("removed", False)
            ):
                raise HistoryImportError("chain_changed")
            if key in logs:
                raise HistoryImportError("invalid_evidence")
            logs[key] = log
            events.append(self._event("protocol_log", response.receipt, log=log))
        for transaction in sorted({key[2] for key in logs}):
            observed = await self._rpc(
                f"receipt:{transaction}", "eth_getTransactionReceipt", [transaction]
            )
            if not isinstance(observed.payload, dict):
                raise HistoryImportError("invalid_evidence")
            receipt = ProtocolBoundary.receipt(observed.payload)
            number = ProtocolBoundary.quantity(receipt["blockNumber"])
            if (
                receipt["transactionHash"].lower() != transaction
                or number not in headers
                or receipt["blockHash"].lower() != headers[number].block_hash
            ):
                raise HistoryImportError("chain_changed")
            receipt_logs = {
                self._log_key(log): log
                for log in receipt["logs"]
                if log["address"].lower() in self._plan.contracts
            }
            expected = {key: value for key, value in logs.items() if key[2] == transaction}
            if set(receipt_logs) != set(expected) or any(
                self._log_economics(expected[key]) != self._log_economics(receipt_logs[key])
                for key in expected
            ):
                raise HistoryImportError("invalid_evidence")
            events.append(self._event("receipt", observed.receipt, receipt=receipt))
        return events

    async def collect(self) -> HistoryImportOutcome:
        """Commit only verified complete chunks; quota failure remains resumable evidence."""
        if self._journal.complete:
            return self._finish(True, None)
        for reason in ("request_limit", "row_limit", "byte_limit", "manifest_limit"):
            event = self._journal.coverage(f"{self._journal.cursor}:False:{reason}")
            if event is not None:
                return HistoryImportOutcome(False, reason, self._journal.cursor, event.event_id)
        try:
            opening = await self._header(self._plan.from_block - 1, "opening")
            while self._journal.cursor < self._plan.to_block:
                cursor = self._journal.cursor
                end = min(cursor + self._plan.chunk_size, self._plan.to_block)
                events = [opening.event] if cursor == self._plan.from_block - 1 else []
                pins = {opening.number: opening}
                if cursor == self._plan.from_block - 1:
                    events.extend(await self._state(opening))
                committed_header = self._journal.header(cursor)
                if cursor != opening.number and committed_header is None:
                    raise HistoryImportError("invalid_evidence")
                previous = (
                    opening.block_hash
                    if cursor == opening.number
                    else committed_header[0]
                    if committed_header is not None
                    else ""
                )
                headers = {}
                for number in range(cursor + 1, end + 1):
                    header = await self._header(number, f"header:{number}")
                    if header.parent_hash != previous:
                        raise HistoryImportError("chain_changed")
                    headers[number] = pins[number] = header
                    previous = header.block_hash
                    events.append(header.event)
                    events.extend(await self._state(header))
                events.extend(await self._logs(cursor + 1, end, headers))
                # Fresh anchor reads detect a changed chain even after resumed cached responses.
                for number, expected in (
                    (opening.number, opening.block_hash),
                    (end, headers[end].block_hash),
                ):
                    fresh = await self._header(number, f"verify:{cursor}:{number}", refresh=True)
                    if fresh.block_hash != expected:
                        raise HistoryImportError("chain_changed")
                    events.append(fresh.event)
                    pins[number] = fresh
                self._journal.commit_chunk(
                    cursor,
                    end,
                    events,
                    [
                        (number, pin.block_hash, pin.event.event_id)
                        for number, pin in sorted(pins.items())
                    ],
                )
            return self._finish(True, None)
        except BudgetExhausted:
            return self._finish(False, "budget_exhausted")
        except ProviderUnavailable:
            return self._finish(False, "provider_unavailable")
        except HistoryImportError as exc:
            reason = str(exc)
            if reason not in {
                "budget_exhausted",
                "request_limit",
                "row_limit",
                "byte_limit",
                "manifest_limit",
                "provider_unavailable",
                "chain_changed",
                "invalid_evidence",
            }:
                reason = "invalid_evidence"
            return self._finish(False, reason)
        except (ValueError, TypeError, KeyError):
            return self._finish(False, "invalid_evidence")

    def _finish(self, complete: bool, reason: str | None) -> HistoryImportOutcome:
        refs = self._journal.references()
        fields = {
            "job_id": self._plan.job_id,
            "plan_digest": self._plan.digest,
            "from_block": self._plan.from_block,
            "to_block": self._plan.to_block,
            "wallets": list(self._plan.wallets),
            "contracts": list(self._plan.contracts),
            "scope": self._plan.scope.wire(),
            "complete": complete,
            "reason": reason,
            **refs,
        }
        # Leave room for the base envelope and explicit ID. Partial references are
        # permitted only with an explicit failed manifest-limit claim.
        while len(json.dumps(fields, sort_keys=True).encode()) > 260000:
            complete, reason = False, "manifest_limit"
            fields.update(complete=False, reason=reason)
            longest = max(refs, key=lambda name: len(refs[name]))
            if not refs[longest]:
                raise HistoryImportError("manifest_limit")
            refs[longest].pop()
        # A retry cannot change the completed prefix. Reuse its original diagnostic
        # rather than consuming a fresh source-time envelope for identical evidence.
        key = f"{self._journal.cursor}:{complete}:{reason}"
        event = self._journal.coverage(key)
        if event is None:
            event = self._event("history_coverage", self._clock.capture(), **fields)
            self._journal.append_coverage(event, key, complete=complete)
        return HistoryImportOutcome(complete, reason, self._journal.cursor, event.event_id)
