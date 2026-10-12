"""Producer mirror of pmrecon v2 historical scope; no receiver package dependency.

Canonical bytes are checked against the separately installed receiver model.
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from pmcopy.integrations.pmrecon.codec import _Base
from pmcopy.integrations.pmrecon.protocol_codec import ADDRESS_PATTERN, HASH_PATTERN, ProtocolBoundary
from pmcopy.config.addresses import (
    POLYMARKET_CONDITIONAL_TOKENS_ADDRESS,
    POLYMARKET_PUSD_ADDRESS,
    POLYGON_USDCE_ADDRESS,
)

CTF = POLYMARKET_CONDITIONAL_TOKENS_ADDRESS.lower()
PUSD = POLYMARKET_PUSD_ADDRESS.lower()
USDCE = POLYGON_USDCE_ADDRESS.lower()


class ScopeBoundary(BaseModel):
    """Freeze wire arrays while rejecting extra fields and scalar coercion."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("*", mode="before")
    @classmethod
    def immutable_arrays(cls, value: Any) -> Any:
        """Convert JSON arrays to immutable tuples without coercing their elements."""
        return tuple(value) if isinstance(value, list) else value


class ScopeAsset(ScopeBoundary):
    """One explicitly enumerated collateral or CTF token identifier."""

    contract: str = Field(pattern=ADDRESS_PATTERN)
    token_id: str | None = None

    @field_validator("token_id")
    @classmethod
    def exact_token(cls, value: str | None) -> str | None:
        """Retain exact integer base units rather than JSON numbers."""
        if value is not None:
            ProtocolBoundary.integer(value)
        return value


class ScopeApproval(ScopeBoundary):
    """One owner/spender allowance or operator approval included in the scope."""

    contract: str = Field(pattern=ADDRESS_PATTERN)
    owner: str = Field(pattern=ADDRESS_PATTERN)
    spender: str = Field(pattern=ADDRESS_PATTERN)
    operator: bool


class ScopeDeployment(ScopeBoundary):
    """Reviewed code identities still require matching block-pinned journal evidence."""

    contract: str = Field(pattern=ADDRESS_PATTERN)
    kind: Literal["ctf", "erc20"]
    code_hash: str = Field(pattern=HASH_PATTERN)
    proxy_implementation: str | None = Field(default=None, pattern=ADDRESS_PATTERN)
    implementation_code_hash: str | None = Field(default=None, pattern=HASH_PATTERN)
    review_ref: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def proxy_pair(self) -> "ScopeDeployment":
        """A reviewed proxy implementation needs both address and code identity."""
        if (self.proxy_implementation is None) != (self.implementation_code_hash is None):
            raise ValueError("proxy implementation and code identity must be paired")
        return self


class ScopePosition(ScopeBoundary):
    """Index-set mapping requiring both collection and token derivation proofs."""

    index_set: str
    token_id: str
    collection_id: str = Field(pattern=HASH_PATTERN)

    @field_validator("index_set", "token_id")
    @classmethod
    def exact_units(cls, value: str) -> str:
        """Reject noncanonical or overflowing integers."""
        ProtocolBoundary.integer(value)
        return value


class ScopePositionSet(ScopeBoundary):
    """CTF lifecycle mapping and optional payout vector, never inferred from prices."""

    condition_id: str = Field(pattern=HASH_PATTERN)
    parent_collection_id: str = Field(pattern=HASH_PATTERN)
    collateral: str = Field(pattern=ADDRESS_PATTERN)
    outcome_slots: int = Field(ge=2, le=256)
    positions: tuple[ScopePosition, ...] = Field(min_length=1, max_length=256)
    parent_token_id: str | None = None
    payout_numerators: tuple[str, ...] = Field(default=(), max_length=256)
    evidence_ids: tuple[str, ...] = Field(default=(), max_length=512)

    @model_validator(mode="after")
    def exact_mapping(self) -> "ScopePositionSet":
        """Validate unsigned mapping values before consulting their evidence references."""
        if self.parent_token_id is not None:
            ProtocolBoundary.integer(self.parent_token_id)
        for value in self.payout_numerators:
            ProtocolBoundary.integer(value)
        if len(self.payout_numerators) not in {0, self.outcome_slots}:
            raise ValueError("payout vector length differs from outcome count")
        if any(not 1 <= len(value) <= 200 for value in self.evidence_ids):
            raise ValueError("invalid position proof reference")
        return self


class HistoryScope(ScopeBoundary):
    """Finite reviewed assets; this is never a whole-account inventory assertion."""

    assets: tuple[ScopeAsset, ...] = Field(min_length=1, max_length=4096)
    approvals: tuple[ScopeApproval, ...] = Field(default=(), max_length=4096)
    deployments: tuple[ScopeDeployment, ...] = Field(default=(), max_length=3)
    positions: tuple[ScopePositionSet, ...] = Field(default=(), max_length=256)


class HistoryCoveragePayload(_Base):
    """Collector completion claims with exact references for independent reconstruction."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    kind: Literal["history_coverage"] = "history_coverage"
    schema_version: Literal[2] = 2
    provider: Literal["alchemy", "infura"]
    chain_id: Literal[137] = 137
    recovery: Literal[True] = True
    job_id: str = Field(min_length=1, max_length=100)
    plan_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    from_block: int = Field(ge=1, le=2**63 - 1)
    to_block: int = Field(ge=1, le=2**63 - 1)
    wallets: tuple[str, ...] = Field(min_length=1, max_length=256)
    contracts: tuple[str, ...] = Field(min_length=1, max_length=3)
    header_event_ids: tuple[str, ...] = Field(max_length=1001)
    receipt_event_ids: tuple[str, ...] = Field(max_length=5000)
    state_event_ids: tuple[str, ...] = Field(max_length=5000)
    log_event_ids: tuple[str, ...] = Field(max_length=5000)
    complete: bool
    reason: (
        Literal[
            "budget_exhausted",
            "request_limit",
            "row_limit",
            "byte_limit",
            "manifest_limit",
            "provider_unavailable",
            "chain_changed",
            "invalid_evidence",
        ]
        | None
    ) = None
    scope: HistoryScope

    @field_validator("schema_version", "chain_id", mode="before")
    @classmethod
    def exact_integer_literal(cls, value: Any) -> int:
        """Pydantic literal equality must not coerce floating point values."""
        if type(value) is not int:
            raise ValueError("integer schema and chain required")
        return value

    @field_validator("recovery", mode="before")
    @classmethod
    def exact_recovery_literal(cls, value: Any) -> bool:
        """Historical evidence explicitly requires a real recovery boolean."""
        if type(value) is not bool:
            raise ValueError("boolean recovery required")
        return value

    @field_validator(
        "wallets",
        "contracts",
        "header_event_ids",
        "receipt_event_ids",
        "state_event_ids",
        "log_event_ids",
        mode="before",
    )
    @classmethod
    def wire_arrays(cls, value: Any) -> Any:
        """Accept only immutable arrays of unchanged wire scalars."""
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def bounded_claim(self) -> "HistoryCoveragePayload":
        """Reject open-ended, duplicated or internally contradictory manifests."""
        if not 0 <= self.to_block - self.from_block < 1000:
            raise ValueError("history coverage exceeds finite block bound")
        if self.complete and self.reason is not None:
            raise ValueError("complete coverage cannot contain a failure reason")
        wallets = {ProtocolBoundary.address(item) for item in self.wallets}
        contracts = {ProtocolBoundary.address(item) for item in self.contracts}
        if len(wallets) != len(self.wallets) or len(contracts) != len(self.contracts):
            raise ValueError("duplicate scope wallet or contract")
        if not contracts <= {CTF, PUSD, USDCE}:
            raise ValueError("unsupported history contract")
        if any(
            item.contract.lower() not in contracts
            for item in (*self.scope.assets, *self.scope.approvals, *self.scope.deployments)
        ):
            raise ValueError("scope contract is not covered")
        if any(item.owner.lower() not in wallets for item in self.scope.approvals):
            raise ValueError("approval owner is outside scope")
        all_ids = (
            *self.header_event_ids,
            *self.receipt_event_ids,
            *self.state_event_ids,
            *self.log_event_ids,
        )
        if len(set(all_ids)) != len(all_ids) or any(not 1 <= len(item) <= 200 for item in all_ids):
            raise ValueError("invalid or duplicated evidence reference")
        wire = self.model_dump(mode="json", exclude_none=True)
        wire["received_ns"] = str(wire["received_ns"])
        ProtocolBoundary.bounded(wire, 262144)
        return self
