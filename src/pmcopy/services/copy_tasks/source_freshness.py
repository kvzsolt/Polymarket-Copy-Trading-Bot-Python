"""Entry freshness evaluated from source evidence and the current processing time."""

from dataclasses import dataclass
from datetime import datetime
from math import isfinite

from pmcopy.domain.detection import DetectionMode, LeaderTradeEvent, TradeSide


@dataclass(frozen=True, slots=True)
class SourceFreshnessPolicy:
    """Use the existing age limit while leaving risk-reducing SELLs eligible."""

    max_age_seconds: float = 0

    def __post_init__(self) -> None:
        if isinstance(self.max_age_seconds, bool) or not isfinite(self.max_age_seconds):
            raise ValueError("Maximum signal age must be finite")
        if self.max_age_seconds < 0:
            raise ValueError("Maximum signal age cannot be negative")

    def rejection(self, event: LeaderTradeEvent, now: datetime) -> str | None:
        """Return an entry rejection code or None for acceptable time evidence.

        Provisional pending detection explicitly uses first receipt time until
        settlement supplies a source timestamp. Polling never receives that exception.
        """
        if event.side is TradeSide.SELL:
            return None
        source_time = event.venue_timestamp
        if (
            source_time is None
            and event.is_provisional
            and event.mode is DetectionMode.ON_CHAIN_PENDING
            and event.source_receipt is not None
        ):
            source_time = event.source_receipt.received_at
        return self.entry_rejection(source_time, now)

    def entry_rejection(self, source_time: datetime | None, now: datetime) -> str | None:
        """Evaluate entry time evidence shared by task and legacy admission."""
        if source_time is None:
            return "signal_age_unknown"
        age = (now - source_time).total_seconds()
        if age < 0:
            return "signal_timestamp_future"
        if self.max_age_seconds > 0 and age > self.max_age_seconds:
            return "signal_too_old"
        return None
