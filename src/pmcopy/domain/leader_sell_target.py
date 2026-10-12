"""Venue-neutral durable leader SELL target state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True, slots=True)
class LeaderSellTarget:
    """Describe the latest follower SELL target for one leader/token scope."""

    user_id: str
    leader_wallet: str
    condition_id: str
    token_id: str
    generation: int
    source_signal_id: str
    intent_payload: Mapping[str, object]
    task_id: str | None = None


__all__ = ["LeaderSellTarget"]
