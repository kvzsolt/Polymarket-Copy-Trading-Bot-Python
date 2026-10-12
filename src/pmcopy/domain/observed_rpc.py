"""An original decoded read paired with its pre-decode transport receipt."""

from dataclasses import dataclass

from pmcopy.domain.source_time import TransportReceipt


@dataclass(frozen=True, slots=True)
class ObservedRpcResult:
    """Preserve body availability time separately from subsequent attribution."""

    payload: object
    receipt: TransportReceipt
