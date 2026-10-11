"""Durable pending receipt stages without invented attribution or finality."""

from typing import Literal

from pydantic import Field, field_validator, model_validator

from integrations.pmrecon.codec import _Base as EventBase, Nano


class PendingBodyObservedEvent(EventBase):
    """Original body receipt alone creates no attributable candidate in v2."""

    kind: Literal["pending_body_observed"] = "pending_body_observed"
    schema_version: Literal[2] = 2
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
    def causal_order(self) -> "PendingBodyObservedEvent":
        """Retain actual body receipt without suggesting owner attribution occurred then."""
        if not self.hash_received_ns <= self.body_received_ns == self.received_ns:
            raise ValueError("invalid pending clocks")
        return self


class PendingObservationEvent(EventBase):
    """Keep incomplete evaluation outcomes replayable without creating candidates."""

    kind: Literal["pending_observation"] = "pending_observation"
    schema_version: Literal[2] = 2
    provider: Literal["alchemy"] = "alchemy"
    transaction_hash: str = Field(pattern=r"^0x[0-9a-fA-F]{64}$")
    stage: Literal[
        "hash_observed",
        "body_unavailable",
        "body_invalid",
        "attributed",
        "deadline_unknown",
        "budget_stopped",
        "capacity_stopped",
    ]
    hash_received_ns: Nano
    body_received_ns: Nano | None = None
    attributable_received_ns: Nano | None = None
    source_event_id: str | None = Field(default=None, min_length=1, max_length=200)
    raw_hash_frame: str | None = Field(default=None, max_length=16384)

    @field_validator("raw_hash_frame")
    @classmethod
    def bounded_frame(cls, value: str | None) -> str | None:
        """Measure the original frame in actual UTF-8 storage bytes."""
        if value is not None and len(value.encode("utf-8")) > 16384:
            raise ValueError("pending frame exceeds byte limit")
        return value

    @model_validator(mode="after")
    def causal_order(self) -> "PendingObservationEvent":
        """Only source-bound body observations may claim successful attribution."""
        if self.stage == "attributed" and (
            self.body_received_ns is None
            or self.attributable_received_ns is None
            or self.source_event_id is None
        ):
            raise ValueError("attribution requires body, attribution receipt and source reference")
        if self.attributable_received_ns is not None and self.body_received_ns is None:
            raise ValueError("attribution cannot precede body observation")
        samples = [self.hash_received_ns]
        samples.extend(
            value
            for value in (self.body_received_ns, self.attributable_received_ns)
            if value is not None
        )
        samples.append(self.received_ns)
        if samples != sorted(samples):
            raise ValueError("pending receipt stages must be non-decreasing")
        return self
