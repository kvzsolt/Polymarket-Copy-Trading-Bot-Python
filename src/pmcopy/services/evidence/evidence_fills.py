"""Construct evidence from one new authoritative follower accounting row."""

from decimal import Decimal
from hashlib import sha256
from typing import Protocol

from pmcopy.domain.evidence import EvidenceEvent
from pmcopy.domain.source_time import TransportReceipt
from pmcopy.integrations.pmrecon.codec import PmreconEvidenceCodec


class ObservedFollowerFill(Protocol):
    """The exact economic columns of one authoritative accounting row."""

    user_id: str
    token_id: str
    side: str
    size_tokens: Decimal
    price: Decimal
    fee: Decimal | None


class FollowerFillEvidenceBuilder:
    """Keep durable fill identity separate from order/cumulative settlement keys."""

    def build(
        self,
        fill_id: int,
        record: ObservedFollowerFill,
        receipt: TransportReceipt,
        *,
        source_action_id: str | None = None,
        recovery: bool = False,
        execution_mode: str = "unknown",
    ) -> EvidenceEvent:
        """Encode one actual fill with native observation time and explicit attribution.

        Missing source action or fees remain absent evidence. Venue fill time is
        not transport receipt time. The caller must append the returned event in
        the same transaction that owns this new fill ID.
        """
        if type(fill_id) is not int or fill_id <= 0:
            raise ValueError("Follower evidence requires a positive database fill ID")
        if receipt.wall_timestamp_ns is None:
            raise ValueError("Follower evidence requires a native receipt sample")
        identity = sha256(f"{record.user_id}|{fill_id}".encode()).hexdigest()
        return PmreconEvidenceCodec.encode(
            {
                "kind": "follower_fill_observed",
                "schema_version": 2,
                "execution_mode": execution_mode,
                "event_id": f"follower-fill:{identity}",
                "received_ns": str(receipt.wall_timestamp_ns),
                "clock_id": receipt.boot_id,
                "recovery": recovery,
                "follower_account": record.user_id,
                "fill_id": f"fill:{fill_id}",
                "source_action_id": source_action_id,
                "asset_id": record.token_id,
                "side": record.side,
                "price": str(record.price),
                "size": str(record.size_tokens),
                "fee": None if record.fee is None else str(record.fee),
            }
        )
