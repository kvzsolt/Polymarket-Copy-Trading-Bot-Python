"""Immutable finite historical read plans; no transports, storage, or trading."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
import re
from typing import Any

# Protocol policy is immutable and cannot be broadened by application config.
# Boundary tests compare these reviewed chain identities with the runtime ABI registry.
CTF = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
CONTRACTS = tuple(
    sorted(
        (
            CTF,
            "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb",
            "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
        )
    )
)


class HistoryImportError(ValueError):
    """Expose one bounded diagnostic reason without source payloads or endpoints."""


class HistoryBoundary:
    """Pure validators for explicit reviewed scope and exact ABI inputs."""

    @staticmethod
    def address(value: object) -> str:
        """Validate a complete address before returning comparison form."""
        if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{40}", value):
            raise HistoryImportError("invalid_evidence")
        return value.lower()

    @staticmethod
    def digest(value: object) -> str:
        """Validate a complete bytes32 value without inventing padding."""
        if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
            raise HistoryImportError("invalid_evidence")
        return value.lower()

    @staticmethod
    def uint(value: object) -> int:
        """Require canonical decimal uint256 strings for protocol base units."""
        if not isinstance(value, str) or not re.fullmatch(r"0|[1-9][0-9]{0,77}", value):
            raise HistoryImportError("invalid_evidence")
        number = int(value)
        if number >= 2**256:
            raise HistoryImportError("invalid_evidence")
        return number

    @staticmethod
    def integer(value: object, minimum: int, maximum: int) -> int:
        """Do not accept booleans or truncate numeric configuration values."""
        if type(value) is not int or not minimum <= value <= maximum:
            raise HistoryImportError("invalid_evidence")
        return value

    @staticmethod
    def keys(value: object, allowed: set[str], required: set[str]) -> dict[str, Any]:
        """Reject unknown fields rather than silently broadening a read plan."""
        if not isinstance(value, dict) or set(value) - allowed or not required <= set(value):
            raise HistoryImportError("invalid_evidence")
        return value


@dataclass(frozen=True, slots=True)
class ReviewedHistoryScope:
    """Own detached immutable JSON; wire() returns a fresh tree on every call."""

    canonical_json: str

    @classmethod
    def from_dict(cls, value: object) -> ReviewedHistoryScope:
        """Validate finite reviewed assets, deployments and explicit lifecycle mappings."""
        scope = HistoryBoundary.keys(
            value, {"assets", "approvals", "deployments", "positions"}, {"assets"}
        )
        detached = json.loads(
            json.dumps(scope, sort_keys=True, separators=(",", ":"), allow_nan=False)
        )
        for key, minimum, maximum in (
            ("assets", 1, 4096),
            ("approvals", 0, 4096),
            ("deployments", 0, 3),
            ("positions", 0, 256),
        ):
            items = detached.setdefault(key, [])
            if not isinstance(items, list) or not minimum <= len(items) <= maximum:
                raise HistoryImportError("invalid_evidence")
        for item in detached["assets"]:
            HistoryBoundary.keys(item, {"contract", "token_id"}, {"contract"})
            contract = HistoryBoundary.address(item["contract"])
            if contract not in CONTRACTS or (contract == CTF) != (item.get("token_id") is not None):
                raise HistoryImportError("invalid_evidence")
            if item.get("token_id") is not None:
                HistoryBoundary.uint(item["token_id"])
        for item in detached["approvals"]:
            HistoryBoundary.keys(
                item,
                {"contract", "owner", "spender", "operator"},
                {"contract", "owner", "spender", "operator"},
            )
            for key in ("contract", "owner", "spender"):
                HistoryBoundary.address(item[key])
            if item["contract"].lower() not in CONTRACTS or type(item["operator"]) is not bool:
                raise HistoryImportError("invalid_evidence")
            if item["operator"] != (item["contract"].lower() == CTF):
                raise HistoryImportError("invalid_evidence")
        for item in detached["deployments"]:
            HistoryBoundary.keys(
                item,
                {
                    "contract",
                    "kind",
                    "code_hash",
                    "proxy_implementation",
                    "implementation_code_hash",
                    "review_ref",
                },
                {"contract", "kind", "code_hash", "review_ref"},
            )
            contract = HistoryBoundary.address(item["contract"])
            if contract not in CONTRACTS or item["kind"] != ("ctf" if contract == CTF else "erc20"):
                raise HistoryImportError("invalid_evidence")
            HistoryBoundary.digest(item["code_hash"])
            proxy, code = item.get("proxy_implementation"), item.get("implementation_code_hash")
            if (proxy is None) != (code is None):
                raise HistoryImportError("invalid_evidence")
            if proxy is not None:
                HistoryBoundary.address(proxy)
                HistoryBoundary.digest(code)
            if (
                not isinstance(item["review_ref"], str)
                or not 1 <= len(item["review_ref"].strip()) <= 200
            ):
                raise HistoryImportError("invalid_evidence")
        for item in detached["positions"]:
            HistoryBoundary.keys(
                item,
                {
                    "condition_id",
                    "parent_collection_id",
                    "collateral",
                    "outcome_slots",
                    "positions",
                    "parent_token_id",
                    "payout_numerators",
                    "evidence_ids",
                },
                {
                    "condition_id",
                    "parent_collection_id",
                    "collateral",
                    "outcome_slots",
                    "positions",
                },
            )
            HistoryBoundary.digest(item["condition_id"])
            HistoryBoundary.digest(item["parent_collection_id"])
            if HistoryBoundary.address(item["collateral"]) not in set(CONTRACTS) - {CTF}:
                raise HistoryImportError("invalid_evidence")
            slots = HistoryBoundary.integer(item["outcome_slots"], 2, 256)
            positions = item["positions"]
            if not isinstance(positions, list) or not 1 <= len(positions) <= 256:
                raise HistoryImportError("invalid_evidence")
            for position in positions:
                HistoryBoundary.keys(
                    position,
                    {"index_set", "token_id", "collection_id"},
                    {"index_set", "token_id", "collection_id"},
                )
                if not 0 < HistoryBoundary.uint(position["index_set"]) < 2**slots:
                    raise HistoryImportError("invalid_evidence")
                HistoryBoundary.uint(position["token_id"])
                HistoryBoundary.digest(position["collection_id"])
            if item.get("parent_token_id") is not None:
                HistoryBoundary.uint(item["parent_token_id"])
            payouts = item.get("payout_numerators", [])
            if not isinstance(payouts, list) or len(payouts) not in (0, slots):
                raise HistoryImportError("invalid_evidence")
            for amount in payouts:
                HistoryBoundary.uint(amount)
            ids = item.get("evidence_ids", [])
            if (
                not isinstance(ids, list)
                or len(ids) > 512
                or any(not isinstance(ref, str) or not 1 <= len(ref) <= 200 for ref in ids)
            ):
                raise HistoryImportError("invalid_evidence")
        encoded = json.dumps(detached, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode()) > 131072:
            raise HistoryImportError("manifest_limit")
        return cls(encoded)

    def wire(self) -> dict[str, Any]:
        """Return a detached reviewed manifest; no mutable state is shared."""
        return json.loads(self.canonical_json)


@dataclass(frozen=True, slots=True)
class HistoryStateRead:
    """One approved ABI read at explicit boundaries or every pinned block."""

    contract: str
    method: str
    arguments: tuple[str, ...] = ()
    at: str = "boundaries"
    storage_slot: str | None = None

    def __post_init__(self) -> None:
        """Validate the exact method signature before any provider is constructed."""
        object.__setattr__(self, "contract", HistoryBoundary.address(self.contract))
        if self.at not in {"boundaries", "all_blocks"} or not isinstance(self.arguments, tuple):
            raise HistoryImportError("invalid_evidence")
        signatures = {
            "balanceOf": ("address", "uint") if self.contract == CTF else ("address",),
            "allowance": ("address", "address"),
            "isApprovedForAll": ("address", "address"),
            "payoutNumerators": ("digest", "uint"),
            "payoutDenominator": ("digest",),
            "getOutcomeSlotCount": ("digest",),
            "getPositionId": ("address", "digest"),
            "getCollectionId": ("digest", "digest", "uint"),
            "codehash": (),
            "implementation": (),
        }
        if self.method not in signatures or len(self.arguments) != len(signatures[self.method]):
            raise HistoryImportError("invalid_evidence")
        for value, kind in zip(self.arguments, signatures[self.method], strict=True):
            getattr(HistoryBoundary, kind)(value)
        ctf_only = {
            "isApprovedForAll",
            "payoutNumerators",
            "payoutDenominator",
            "getOutcomeSlotCount",
            "getPositionId",
            "getCollectionId",
        }
        if (self.method in ctf_only and self.contract != CTF) or (
            self.method == "allowance" and self.contract == CTF
        ):
            raise HistoryImportError("invalid_evidence")
        if self.method == "implementation":
            HistoryBoundary.digest(self.storage_slot)
        elif self.storage_slot is not None:
            raise HistoryImportError("invalid_evidence")

    @classmethod
    def from_dict(cls, value: object) -> HistoryStateRead:
        """Load only documented plan fields with immutable argument values."""
        item = HistoryBoundary.keys(
            value, {"contract", "method", "arguments", "at", "storage_slot"}, {"contract", "method"}
        )
        args = item.get("arguments", [])
        if not isinstance(args, list):
            raise HistoryImportError("invalid_evidence")
        return cls(
            contract=item["contract"],
            method=item["method"],
            arguments=tuple(args),
            at=item.get("at", "boundaries"),
            storage_slot=item.get("storage_slot"),
        )


@dataclass(frozen=True, slots=True)
class HistoryImportPlan:
    """One bounded, reviewed, read-only job whose identity survives process restart."""

    job_id: str
    provider: str
    from_block: int
    to_block: int
    wallets: tuple[str, ...]
    scope: ReviewedHistoryScope
    contracts: tuple[str, ...] = CONTRACTS
    state_reads: tuple[HistoryStateRead, ...] = ()
    chunk_size: int = 10
    max_requests: int = 1000
    max_cu: int = 100000
    max_rows: int = 5000
    max_bytes: int = 16_000_000

    def __post_init__(self) -> None:
        """Reject open-ended ranges, unsigned transactions, and unreviewed addresses."""
        if (
            not isinstance(self.job_id, str)
            or not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", self.job_id)
            or self.provider not in {"alchemy", "infura"}
        ):
            raise HistoryImportError("invalid_evidence")
        HistoryBoundary.integer(self.from_block, 1, 2**63 - 1)
        HistoryBoundary.integer(
            self.to_block, self.from_block, min(2**63 - 1, self.from_block + 999)
        )
        for value, low, high in (
            (self.chunk_size, 1, 10),
            (self.max_requests, 1, 10000),
            (self.max_cu, 1, 1_000_000),
            (self.max_rows, 1, 5000),
            (self.max_bytes, 1024, 64_000_000),
        ):
            HistoryBoundary.integer(value, low, high)
        if not isinstance(self.wallets, tuple) or not 1 <= len(self.wallets) <= 256:
            raise HistoryImportError("invalid_evidence")
        wallets = tuple(HistoryBoundary.address(wallet) for wallet in self.wallets)
        if len(set(wallets)) != len(wallets):
            raise HistoryImportError("invalid_evidence")
        object.__setattr__(self, "wallets", tuple(sorted(wallets)))
        contracts = tuple(sorted(HistoryBoundary.address(item) for item in self.contracts))
        if contracts != CONTRACTS:
            raise HistoryImportError("invalid_evidence")
        object.__setattr__(self, "contracts", contracts)
        if (
            not isinstance(self.scope, ReviewedHistoryScope)
            or not isinstance(self.state_reads, tuple)
            or len(self.state_reads) > 256
        ):
            raise HistoryImportError("invalid_evidence")
        scope = ReviewedHistoryScope.from_dict(self.scope.wire()).wire()
        proxies = {
            item["proxy_implementation"].lower()
            for item in scope["deployments"]
            if item.get("proxy_implementation")
        }
        if any(item["owner"].lower() not in wallets for item in scope["approvals"]):
            raise HistoryImportError("invalid_evidence")
        for read in self.state_reads:
            if not isinstance(read, HistoryStateRead) or (
                read.contract not in contracts
                and not (read.method == "codehash" and read.contract in proxies)
            ):
                raise HistoryImportError("invalid_evidence")
            if (
                read.method in {"balanceOf", "allowance", "isApprovedForAll"}
                and read.arguments[0].lower() not in wallets
            ):
                raise HistoryImportError("invalid_evidence")

    @classmethod
    def from_dict(cls, value: object) -> HistoryImportPlan:
        """Parse reviewed JSON without opening configuration, secrets, or a journal."""
        item = HistoryBoundary.keys(
            value,
            set(cls.__dataclass_fields__),
            {"job_id", "provider", "from_block", "to_block", "wallets", "scope"},
        )
        for key in ("wallets", "contracts", "state_reads"):
            if key in item and not isinstance(item[key], list):
                raise HistoryImportError("invalid_evidence")
        return cls(
            **(
                item
                | {
                    "wallets": tuple(item["wallets"]),
                    "contracts": tuple(item.get("contracts", CONTRACTS)),
                    "state_reads": tuple(
                        HistoryStateRead.from_dict(read) for read in item.get("state_reads", [])
                    ),
                    "scope": ReviewedHistoryScope.from_dict(item["scope"]),
                }
            )
        )

    def wire(self) -> dict[str, Any]:
        """Return deterministic public planning data, containing provider aliases only."""
        return asdict(self) | {"scope": self.scope.wire()}

    @property
    def digest(self) -> str:
        """Bind all bounds, reviewed mappings and planned reads to one SHA-256."""
        return sha256(
            json.dumps(self.wire(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def preview(self) -> dict[str, object]:
        """Pure description; creating this value never admits a provider request."""
        return {
            "read_only": True,
            "execute": False,
            "job_id": self.job_id,
            "plan_digest": self.digest,
            "provider": self.provider,
            "from_block": self.from_block,
            "to_block": self.to_block,
            "opening_block": self.from_block - 1,
            "max_requests": self.max_requests,
            "max_cu": self.max_cu,
            "max_rows": self.max_rows,
            "max_bytes": self.max_bytes,
        }
