"""Producer mirror of receiver v2 protocol wire contracts; checked by golden tests.

These producer claims are evidence inputs, not independent cryptographic proof.
Legacy models, canonical bytes and event identities are intentionally unchanged.
"""

from __future__ import annotations

import json
import re
from typing import Any, Literal

from pydantic import ConfigDict, Field, field_validator, model_validator

from pydantic import BaseModel as StrictModel
from pmcopy.integrations.pmrecon.codec import _Base as EventBase

UINT256_MAX = 2**256 - 1
ADDRESS_PATTERN = r"^0x[0-9a-fA-F]{40}$"
HASH_PATTERN = r"^0x[0-9a-fA-F]{64}$"


class ProtocolBoundary:
    """Bound and validate original RPC evidence without normalization loss."""

    @staticmethod
    def quantity(value: object) -> int:
        """Decode a canonical uint256 JSON-RPC quantity, rejecting bools/numbers."""
        if not isinstance(value, str) or not re.fullmatch(
            r"0x(?:0|[1-9a-fA-F][0-9a-fA-F]{0,63})", value
        ):
            raise ValueError("invalid uint256 RPC quantity")
        return int(value, 16)

    @staticmethod
    def integer(value: object) -> int:
        """Decode an exact unsigned base-unit string without floating-point input."""
        if not isinstance(value, str) or not re.fullmatch(r"(?:0|[1-9][0-9]{0,77})", value):
            raise ValueError("invalid uint256 decimal string")
        result = int(value)
        if result > UINT256_MAX:
            raise ValueError("uint256 overflow")
        return result

    @staticmethod
    def address(value: object) -> str:
        """Validate a twenty-byte address and return normalized comparison form."""
        if not isinstance(value, str) or not re.fullmatch(ADDRESS_PATTERN, value):
            raise ValueError("invalid contract or wallet address")
        return value.lower()

    @staticmethod
    def digest(value: object) -> str:
        """Validate a thirty-two-byte hash and return normalized comparison form."""
        if not isinstance(value, str) or not re.fullmatch(HASH_PATTERN, value):
            raise ValueError("invalid hash")
        return value.lower()

    @staticmethod
    def bounded(value: object, limit: int = 1048576) -> None:
        """Reject excess payloads before recursive parsing or projection work."""
        if len(json.dumps(value, separators=(",", ":"), allow_nan=False).encode()) > limit:
            raise ValueError("protocol evidence exceeds size limit")

    @classmethod
    def log(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Validate raw log coordinates and bounded ABI bytes, preserving input."""
        if not isinstance(value, dict):
            raise ValueError("raw log must be an object")
        cls.bounded(value, 262144)
        required = {
            "address",
            "topics",
            "data",
            "transactionHash",
            "logIndex",
            "blockHash",
            "blockNumber",
        }
        if not required <= value.keys() or value.keys() - required - {
            "transactionIndex",
            "removed",
        }:
            raise ValueError("unsupported or missing raw log fields")
        cls.address(value["address"])
        for key in ("transactionHash", "blockHash"):
            cls.digest(value[key])
        for key in ("logIndex", "blockNumber", "transactionIndex"):
            if key in value:
                cls.quantity(value[key])
        topics = value["topics"]
        if not isinstance(topics, list) or not 1 <= len(topics) <= 4:
            raise ValueError("invalid topic count")
        for topic in topics:
            cls.digest(topic)
        if not isinstance(value["data"], str) or not re.fullmatch(
            r"0x(?:[0-9a-fA-F]{2})*", value["data"]
        ):
            raise ValueError("invalid ABI bytes")
        if "removed" in value and type(value["removed"]) is not bool:
            raise ValueError("removed must be boolean")
        return value

    @classmethod
    def receipt(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Validate complete receipt/log inclusion consistency without guessing."""
        if not isinstance(value, dict):
            raise ValueError("raw receipt must be an object")
        cls.bounded(value)
        required = {
            "transactionHash",
            "transactionIndex",
            "blockHash",
            "blockNumber",
            "status",
            "logs",
        }
        optional = {
            "from",
            "to",
            "cumulativeGasUsed",
            "gasUsed",
            "contractAddress",
            "logsBloom",
            "type",
            "effectiveGasPrice",
            "blobGasUsed",
            "blobGasPrice",
        }
        if not required <= value.keys() or value.keys() - required - optional:
            raise ValueError("unsupported or missing receipt fields")
        for key in ("transactionHash", "blockHash"):
            cls.digest(value[key])
        for key in (
            "transactionIndex",
            "blockNumber",
            "status",
            "cumulativeGasUsed",
            "gasUsed",
            "type",
            "effectiveGasPrice",
            "blobGasUsed",
            "blobGasPrice",
        ):
            if key in value:
                cls.quantity(value[key])
        if value["status"] not in {"0x0", "0x1"}:
            raise ValueError("receipt status must be failed or successful")
        for key in ("from", "to", "contractAddress"):
            if value.get(key) is not None:
                cls.address(value[key])
        if "logsBloom" in value and not re.fullmatch(r"0x[0-9a-fA-F]{512}", value["logsBloom"]):
            raise ValueError("invalid receipt bloom")
        logs = value["logs"]
        if not isinstance(logs, list) or len(logs) > 4096 or (value["status"] == "0x0" and logs):
            raise ValueError("invalid receipt log collection")
        seen: set[int] = set()
        for log in logs:
            if not isinstance(log, dict):
                raise ValueError("invalid receipt log")
            cls.log(log)
            if any(
                log[key].lower() != value[key].lower()
                for key in ("transactionHash", "blockHash", "blockNumber")
            ):
                raise ValueError("receipt/log inclusion mismatch")
            if log.get("removed", False) or (
                "transactionIndex" in log and log["transactionIndex"] != value["transactionIndex"]
            ):
                raise ValueError("removed or inconsistent receipt log")
            index = cls.quantity(log["logIndex"])
            if index in seen:
                raise ValueError("duplicate receipt log index")
            seen.add(index)
        return value


class ProtocolLogEvent(EventBase):
    """Raw token/lifecycle log awaiting registry-qualified decoding."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal[2] = 2
    kind: Literal["protocol_log"] = "protocol_log"
    provider: Literal["alchemy", "infura"]
    chain_id: Literal[137] = 137
    log: dict[str, Any]

    @field_validator("log")
    @classmethod
    def validate_log(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Retain the exact validated original source message."""
        return ProtocolBoundary.log(value)


class ReceiptEvent(EventBase):
    """Full bounded receipt evidence for one explicit transaction inclusion."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal[2] = 2
    kind: Literal["receipt"] = "receipt"
    provider: Literal["alchemy", "infura"]
    chain_id: Literal[137] = 137
    receipt: dict[str, Any]

    @field_validator("receipt")
    @classmethod
    def validate_receipt(cls, value: dict[str, Any]) -> dict[str, Any]:
        """Require consistent inclusion and exact original receipt fields."""
        return ProtocolBoundary.receipt(value)


class ContractStateEvent(EventBase):
    """One exact block-pinned eth_call result used as a ledger boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal[2] = 2
    kind: Literal["contract_state"] = "contract_state"
    provider: Literal["alchemy", "infura"]
    chain_id: Literal[137] = 137
    contract: str = Field(pattern=ADDRESS_PATTERN)
    block_number: int = Field(ge=0, le=UINT256_MAX)
    block_hash: str = Field(pattern=HASH_PATTERN)
    method: Literal[
        "balanceOf",
        "allowance",
        "isApprovedForAll",
        "payoutNumerators",
        "payoutDenominator",
        "getPositionId",
        "getCollectionId",
        "getOutcomeSlotCount",
        "implementation",
        "codehash",
    ]
    arguments: tuple[str, ...] = ()
    value: str | bool

    @field_validator("arguments", mode="before")
    @classmethod
    def wire_arguments(cls, value: Any) -> tuple[str, ...]:
        """Accept a JSON array without coercing scalar argument values."""
        if (
            not isinstance(value, (list, tuple))
            or len(value) > 4
            or any(not isinstance(item, str) for item in value)
        ):
            raise ValueError("invalid state call arguments")
        return tuple(value)

    @model_validator(mode="after")
    def typed_result(self) -> ContractStateEvent:
        """Bind each result to a supported call signature with exact base units."""
        args = self.arguments
        if self.method == "balanceOf":
            if len(args) not in {1, 2}:
                raise ValueError("balanceOf requires owner and optional token id")
            ProtocolBoundary.address(args[0])
            if len(args) == 2:
                ProtocolBoundary.integer(args[1])
        elif self.method in {"allowance", "isApprovedForAll"}:
            if len(args) != 2:
                raise ValueError("approval state requires owner and spender/operator")
            for arg in args:
                ProtocolBoundary.address(arg)
        elif self.method in {"payoutNumerators", "payoutDenominator", "getOutcomeSlotCount"}:
            if len(args) != (2 if self.method == "payoutNumerators" else 1):
                raise ValueError("invalid payout call arguments")
            ProtocolBoundary.digest(args[0])
            if len(args) == 2:
                ProtocolBoundary.integer(args[1])
        elif self.method == "getCollectionId":
            if len(args) != 3:
                raise ValueError("collection id requires parent, condition and index set")
            ProtocolBoundary.digest(args[0])
            ProtocolBoundary.digest(args[1])
            ProtocolBoundary.integer(args[2])
        elif self.method == "getPositionId":
            if len(args) != 2:
                raise ValueError("position id requires collateral and collection")
            ProtocolBoundary.address(args[0])
            ProtocolBoundary.digest(args[1])
        elif args:
            raise ValueError("implementation/codehash reads have no arguments")
        if self.method == "isApprovedForAll":
            if type(self.value) is not bool:
                raise ValueError("operator approval must be a boolean")
        elif self.method == "implementation":
            ProtocolBoundary.address(self.value)
        elif self.method in {"codehash", "getCollectionId"}:
            ProtocolBoundary.digest(self.value)
        else:
            ProtocolBoundary.integer(self.value)
        return self


class VerificationDomain(StrictModel):
    """Explicit EIP-712 domain claimed by the producer's verifier."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    chain_id: Literal[137] = 137
    verifying_contract: str = Field(pattern=ADDRESS_PATTERN)
    name: str = Field(min_length=1, max_length=100)
    version: str = Field(min_length=1, max_length=32)


class OrderVerificationEvent(EventBase):
    """Source-bound producer verification; pmrecon does not sign or recover here."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    schema_version: Literal[2] = 2
    kind: Literal["order_verification"] = "order_verification"
    verification_basis: Literal["producer_verified"] = "producer_verified"
    source_event_id: str = Field(min_length=1, max_length=200)
    source_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    order_hash: str = Field(pattern=HASH_PATTERN)
    order_index: int = Field(ge=0, le=4096)
    order_role: Literal["maker", "taker"]
    owner: str = Field(pattern=ADDRESS_PATTERN)
    signer: str = Field(pattern=ADDRESS_PATTERN)
    signature_type: Literal[0, 1, 2, 3]
    domain: VerificationDomain
    hash_status: Literal["verified", "invalid", "unknown"]
    signature_status: Literal["verified", "invalid", "unknown"]
    authority_status: Literal["verified", "invalid", "unknown"]
    preapproval_status: Literal["verified", "invalid", "unknown"]
    verifier_version: str = Field(min_length=1, max_length=100)
    input_hashes: dict[str, str]
    block_number: int | None = Field(default=None, ge=0, le=UINT256_MAX)
    block_hash: str | None = Field(default=None, pattern=HASH_PATTERN)

    @field_validator("input_hashes")
    @classmethod
    def input_digests(cls, value: dict[str, str]) -> dict[str, str]:
        """Bind a bounded named set of exact verifier inputs to SHA-256 digests."""
        if not 1 <= len(value) <= 32 or any(
            not re.fullmatch(r"[a-z0-9_]{1,64}", key) or not re.fullmatch(r"[0-9a-f]{64}", digest)
            for key, digest in value.items()
        ):
            raise ValueError("invalid verifier input digest mapping")
        return value

    @model_validator(mode="after")
    def pinned_authority(self) -> OrderVerificationEvent:
        """Contract-wallet authority claims require a concrete historical block."""
        if (self.block_number is None) != (self.block_hash is None):
            raise ValueError("block number and hash must be supplied together")
        if (
            self.signature_type != 0
            and self.authority_status == "verified"
            and self.block_hash is None
        ):
            raise ValueError("verified contract authority requires block-pinned evidence")
        return self
