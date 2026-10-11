"""Strict producer encoding for the inspected aethertrade.evidence.v1 contract.

The receiver is a separate service, not a bot dependency. Defaults, decimal
serialization and hashes intentionally match pmrecon.integration.models.
"""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictBool,
    TypeAdapter,
    field_validator,
    model_validator,
)

from domain.evidence import EvidenceError, EvidenceEvent


def _money(value: object) -> Decimal:
    """Normalize decimal strings exactly as the receiver, rejecting float money."""
    if not isinstance(value, (str, Decimal)):
        raise ValueError("exact decimal string required")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError("invalid decimal") from None
    if not result.is_finite():
        raise ValueError("invalid decimal")
    return result


def _nano(value: object) -> int:
    """Require exact positive signed-64-bit epoch nanoseconds."""
    if (
        isinstance(value, bool)
        or not isinstance(value, (str, int))
        or (isinstance(value, str) and not value.isdigit())
    ):
        raise ValueError("invalid nanoseconds")
    result = int(value)
    if not 0 < result < 2**63:
        raise ValueError("invalid nanoseconds")
    return result


Money = Annotated[Decimal, BeforeValidator(_money)]
Nano = Annotated[int, BeforeValidator(_nano)]


class _Base(BaseModel):
    """Shared wire defaults; unknown fields never disappear silently."""

    model_config = ConfigDict(extra="forbid")
    event_id: str | None = Field(default=None, min_length=1, max_length=200)
    received_ns: Nano
    clock_id: str = Field(min_length=1, max_length=100)
    recovery: StrictBool = False

    @field_validator("clock_id")
    @classmethod
    def identified_clock(cls, value: str) -> str:
        """Refuse the receiver's unspecified legacy clock for new observations."""
        if value == "unspecified":
            raise ValueError("identified clock required")
        return value


class _Chain(_Base):
    """Raw Polygon log without invented economics or discarded removal evidence."""

    kind: Literal["chain_log"]
    provider: Literal["alchemy", "infura"]
    chain_id: Literal[137] = 137
    log: dict[str, Any]

    @field_validator("log")
    @classmethod
    def log_fields(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Reject extra raw fields that could contain credential-bearing metadata."""
        allowed = {
            "address",
            "topics",
            "data",
            "transactionHash",
            "transactionIndex",
            "logIndex",
            "blockHash",
            "blockNumber",
            "removed",
        }
        if set(value) - allowed or ("removed" in value and type(value["removed"]) is not bool):
            raise ValueError("invalid log fields")
        return value


class _Pending(_Base):
    """Separate hash, body and attribution clocks; estimates are never mined fills."""

    kind: Literal["pending_body"]
    provider: Literal["alchemy"] = "alchemy"
    transaction_hash: str = Field(pattern=r"^0x[0-9a-fA-F]{64}$")
    target: str = Field(pattern=r"^0x[0-9a-fA-F]{40}$")
    sender: str = Field(pattern=r"^0x[0-9a-fA-F]{40}$")
    nonce: int = Field(ge=0, strict=True)
    calldata: str = Field(max_length=1048576, pattern=r"^0x[0-9a-fA-F]*$")
    hash_received_ns: Nano
    body_received_ns: Nano
    block_number: int | None = Field(default=None, ge=0, strict=True)

    @model_validator(mode="after")
    def ordering(self) -> _Pending:
        """Do not backdate attributed evidence to an earlier hash sighting."""
        if not self.hash_received_ns <= self.body_received_ns <= self.received_ns:
            raise ValueError("invalid pending clocks")
        return self


class _Book(_Base):
    """Exact finite book levels supplied by an already-owned market connection."""

    kind: Literal["book"]
    asset_id: str = Field(pattern=r"^[0-9]+$")
    bids: tuple[tuple[Money, Money], ...] = ()
    asks: tuple[tuple[Money, Money], ...] = ()
    safe: StrictBool = True

    @model_validator(mode="after")
    def levels(self) -> _Book:
        """Match the receiver's bounded unique and non-crossed book semantics."""
        for levels in (self.bids, self.asks):
            if len(levels) > 1000 or len({p for p, _ in levels}) != len(levels):
                raise ValueError("invalid book levels")
            if any(not 0 < p < 1 or q <= 0 for p, q in levels):
                raise ValueError("invalid book amounts")
        if self.safe and self.bids and self.asks:
            if max(p for p, _ in self.bids) >= min(p for p, _ in self.asks):
                raise ValueError("crossed safe book")
        return self


class _Fill(_Base):
    """Observed follower economics, with fee omitted when unknown."""

    kind: Literal["follower_fill"]
    follower_account: str = Field(min_length=1, max_length=100)
    fill_id: str = Field(min_length=1, max_length=200)
    source_action_id: str = Field(min_length=1, max_length=200)
    asset_id: str = Field(pattern=r"^[0-9]+$")
    side: Literal["BUY", "SELL"]
    price: Money
    size: Money
    fee: Money | None = None

    @model_validator(mode="after")
    def amounts(self) -> _Fill:
        """Reject unobserved/invalid fills without inventing a zero fee."""
        if not 0 < self.price < 1 or self.size <= 0 or (self.fee is not None and self.fee < 0):
            raise ValueError("invalid fill amounts")
        return self


class _Gap(_Base):
    """Receiver-visible evidence continuity invalidation."""

    kind: Literal["gap"]
    provider: Literal["alchemy", "infura", "bot", "clob"]
    reason: Literal["disconnected", "quota", "overflow", "recovered", "reorg"]
    from_block: int | None = Field(default=None, ge=0, strict=True)
    to_block: int | None = Field(default=None, ge=0, strict=True)


class _ObservedFill(_Base):
    """Additive v2 actual fill whose leader attribution may remain unknown."""

    kind: Literal["follower_fill_observed"]
    schema_version: Literal[2] = 2
    execution_mode: Literal["paper", "live", "unknown"] = "unknown"
    follower_account: str = Field(min_length=1, max_length=100)
    fill_id: str = Field(min_length=1, max_length=200)
    source_action_id: str | None = Field(default=None, min_length=1, max_length=200)
    asset_id: str = Field(pattern=r"^[0-9]+$")
    side: Literal["BUY", "SELL"]
    price: Money
    size: Money
    fee: Money | None = None

    @model_validator(mode="after")
    def amounts(self) -> _ObservedFill:
        """Retain exact observed economics without substituting fees or source actions."""
        if not 0 < self.price < 1 or self.size <= 0 or (self.fee is not None and self.fee < 0):
            raise ValueError("invalid fill amounts")
        return self


class _Header(_Base):
    """Additive v2 pinned coordinates plus the complete bounded original RPC header."""

    kind: Literal["block_header"]
    schema_version: Literal[2] = 2
    provider: Literal["alchemy", "infura"]
    chain_id: Literal[137] = 137
    header: dict[str, Any]
    raw_header: dict[str, Any] | None = None

    @model_validator(mode="after")
    def coordinates(self) -> _Header:
        """Bind the exact raw capture to four validated canonical block coordinates."""
        if set(self.header) != {"number", "hash", "parentHash", "timestamp"}:
            raise ValueError("invalid header fields")
        for key in ("number", "timestamp"):
            value = self.header[key]
            if not isinstance(value, str) or not re.fullmatch(
                r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]{0,63})", value
            ):
                raise ValueError("invalid header quantity")
        for key in ("hash", "parentHash"):
            value = self.header[key]
            if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
                raise ValueError("invalid header hash")
        if self.raw_header is None:
            return self
        if any(self.raw_header.get(key) != value for key, value in self.header.items()):
            raise ValueError("raw header coordinate conflict")
        allowed = {
            "number",
            "hash",
            "parentHash",
            "timestamp",
            "transactions",
            "uncles",
            "nonce",
            "sha3Uncles",
            "logsBloom",
            "transactionsRoot",
            "stateRoot",
            "receiptsRoot",
            "miner",
            "difficulty",
            "totalDifficulty",
            "extraData",
            "size",
            "gasLimit",
            "gasUsed",
            "baseFeePerGas",
            "mixHash",
            "withdrawalsRoot",
            "blobGasUsed",
            "excessBlobGas",
            "parentBeaconBlockRoot",
            "requestsHash",
        }
        if set(self.raw_header) - allowed:
            raise ValueError("invalid raw header fields")
        if (
            len(json.dumps(self.raw_header, separators=(",", ":"), allow_nan=False).encode())
            > 262144
        ):
            raise ValueError("raw header too large")
        for key, value in self.raw_header.items():
            if key in {"transactions", "uncles"}:
                if (
                    not isinstance(value, list)
                    or len(value) > 4096
                    or any(
                        not isinstance(item, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", item)
                        for item in value
                    )
                ):
                    raise ValueError("invalid raw header arrays")
            elif value is not None and (
                not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]*", value)
            ):
                raise ValueError("invalid raw header scalar")
        return self


class _MarketFrame(_Base):
    """Additive v2 original market text, preserved even when JSON/book parsing fails."""

    kind: Literal["market_frame"]
    schema_version: Literal[2] = 2
    stream_id: str = Field(min_length=1, max_length=100)
    sequence: int = Field(strict=True, ge=1, le=2**63 - 1)
    frame: str = Field(min_length=1, max_length=262144)

    @field_validator("frame")
    @classmethod
    def bounded_utf8(cls, value: str) -> str:
        """Apply the receiver's original-frame UTF-8 byte limit."""
        if len(value.encode("utf-8")) > 262144:
            raise ValueError("market frame too large")
        return value


_ADAPTER = TypeAdapter(
    Annotated[
        _Chain | _Pending | _Book | _Fill | _Gap | _Header | _MarketFrame | _ObservedFill,
        Field(discriminator="kind"),
    ]
)


class PmreconEvidenceCodec:
    """Generate receiver-equivalent canonical identities without a pmrecon import."""

    @staticmethod
    def encode(value: object) -> EvidenceEvent:
        """Validate, detach and canonicalize an observation; never echo rejected input."""
        try:
            kind = value.get("kind") if isinstance(value, dict) else None
            if kind in {"pending_observation", "pending_body_observed"}:
                from integrations.pmrecon.pending_codec import (
                    PendingBodyObservedEvent,
                    PendingObservationEvent,
                )

                event = (
                    PendingObservationEvent
                    if kind == "pending_observation"
                    else PendingBodyObservedEvent
                ).model_validate(value)
            elif kind in {"protocol_log", "receipt", "contract_state", "order_verification"}:
                from integrations.pmrecon.protocol_codec import (
                    ContractStateEvent,
                    OrderVerificationEvent,
                    ProtocolLogEvent,
                    ReceiptEvent,
                )

                event = {
                    "protocol_log": ProtocolLogEvent,
                    "receipt": ReceiptEvent,
                    "contract_state": ContractStateEvent,
                    "order_verification": OrderVerificationEvent,
                }[kind].model_validate(value)
            elif kind == "history_coverage":
                from integrations.pmrecon.history_codec import HistoryCoveragePayload

                event = HistoryCoveragePayload.model_validate(value)
            else:
                event = _ADAPTER.validate_python(value)
            wire = event.model_dump(mode="json", exclude_none=True)
            for key in wire:
                if key.endswith("_ns"):
                    wire[key] = str(wire[key])
            explicit_id = wire.pop("event_id", None)
            canonical = json.dumps(
                wire, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
            ).encode()
            if len(canonical) > 900_000:
                raise ValueError("event too large")
            return EvidenceEvent(canonical, explicit_id or hashlib.sha256(canonical).hexdigest())
        except (ValueError, TypeError, OverflowError):
            raise EvidenceError("evidence_invalid_payload") from None
