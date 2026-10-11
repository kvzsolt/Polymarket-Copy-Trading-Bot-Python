"""Translate original provider observations into local durable v1 evidence."""

from collections.abc import Mapping

from domain.evidence import EvidenceError, EvidenceEvent
from domain.source_time import TransportReceipt
from integrations.pmrecon.codec import PmreconEvidenceCodec
from interfaces.evidence import EvidenceOutbox


class PmreconObservationSink:
    """RawObservationSink implementation; contains no HTTP client or async network work."""

    def __init__(self, outbox: EvidenceOutbox) -> None:
        """Inject already-open Finland-local persistence."""
        self._outbox = outbox

    def append(self, event: EvidenceEvent) -> int:
        """Append a pre-encoded BookTap or follower event with unchanged identity."""
        return self._outbox.append(event)

    def record(
        self,
        provider: str,
        kind: str,
        payload: object,
        receipt: TransportReceipt,
        *,
        recovered: bool,
    ) -> None:
        """Preserve native source receipt; never manufacture nanoseconds from ms.

        A raw mined log maps to chain_log. Other supported kinds accept their v1
        kind-specific fields; caller-supplied clock/recovery/provider overrides
        are rejected. Unsupported auxiliary observations require their own store.
        """
        if receipt.wall_timestamp_ns is None:
            self._outbox.invalidate("missing_native_clock")
            raise EvidenceError("evidence_missing_native_clock")
        try:
            if kind in ("mined", "chain_log"):
                value = {"kind": "chain_log", "provider": provider, "log": payload}
            elif kind == "block_header":
                if not isinstance(payload, Mapping):
                    raise EvidenceError("evidence_invalid_payload")
                keys = {"number", "hash", "parentHash", "timestamp"}
                value = {
                    "kind": "block_header",
                    "provider": provider,
                    "header": {key: payload.get(key) for key in keys},
                    "raw_header": dict(payload),
                }
            elif kind in ("gap", "pending_body", "book", "follower_fill"):
                if not isinstance(payload, Mapping):
                    raise EvidenceError("evidence_invalid_payload")
                reserved = {"kind", "received_ns", "clock_id", "recovery", "event_id"}
                if set(payload) & reserved or (
                    "provider" in payload and payload["provider"] != provider
                ):
                    raise EvidenceError("evidence_invalid_payload")
                value = dict(payload) | {"kind": kind}
                if kind in ("gap", "pending_body"):
                    value["provider"] = provider
            else:
                raise EvidenceError("evidence_unsupported_observation")
            value.update(
                received_ns=str(receipt.wall_timestamp_ns),
                clock_id=receipt.boot_id,
                recovery=recovered,
            )
            event = PmreconEvidenceCodec.encode(value)
        except ValueError:
            self._outbox.invalidate("invalid_payload")
            raise EvidenceError("evidence_invalid_payload") from None
        self._outbox.append(event)
