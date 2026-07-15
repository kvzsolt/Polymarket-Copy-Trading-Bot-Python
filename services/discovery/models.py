from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

import orjson


@dataclass(slots=True)
class Candidate:
    wallet: str
    hscore: dict[str, Any]
    wallet360: dict[str, dict[str, Any]] = field(default_factory=dict)
    recent_trades: list[dict[str, Any]] = field(default_factory=list)
    market_quality: dict[str, Any] = field(default_factory=dict)
    social: dict[str, Any] = field(default_factory=dict)
    sub_scores: dict[str, float] = field(default_factory=dict)
    metrics: dict[str, float | int] = field(default_factory=dict)
    base_score: float = 0.0
    longitudinal_score: float | None = None
    score: float = 0.0
    rejected_reason: str | None = None


@dataclass(frozen=True, slots=True)
class RosterEntry:
    wallet: str
    tier: str
    score: float
    cycles_in_tier: int
    tier_changed_at: float
    first_promoted_at: float | None


def discovery_config_hash(config: Any) -> str:
    """Hash only settings that change candidate evidence or score semantics.

    Transport rate limits and scheduler cadence are deliberately excluded:
    changing operational plumbing must not invalidate an otherwise identical
    roster.
    """
    payload = (
        config.model_dump(mode="json")
        if hasattr(config, "model_dump")
        else config
    )
    decision_sections = (
        "shortlist",
        "wallet360",
        "recency",
        "market",
        "social",
        "scoring",
        "longitudinal",
        "hysteresis",
    )
    if isinstance(payload, dict):
        payload = {
            section: payload.get(section)
            for section in decision_sections
        }
    return hashlib.sha256(
        orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()


__all__ = ["Candidate", "RosterEntry", "discovery_config_hash"]
