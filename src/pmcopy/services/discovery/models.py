"""Persistence-facing models and narrow ports for trader discovery."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

import orjson


@dataclass(slots=True)
class Candidate:
    """Persist normalized discovery evidence and composite score."""

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


class DiscoveryRepository(Protocol):
    """Structural port for the live discovery service's persistence backend.

    Implemented by ``storage.postgres.discovery_repo.DiscoveryRepo`` and
    consumed by ``services.discovery.falcon_discovery_service.FalconDiscoveryService``.
    """

    async def begin_cycle(self, config_hash: str) -> str:
        """Create and return a durable discovery-cycle identifier."""
        ...

    async def persist_cycle(
        self,
        cycle_id: str,
        candidates: list[Candidate],
        config_hash: str,
    ) -> None:
        """Persist the complete evaluated candidate set atomically."""
        ...

    async def complete_cycle(
        self,
        cycle_id: str,
        *,
        status: str,
        candidate_count: int,
        kept_count: int,
        error: str | None = None,
    ) -> None:
        """Mark a discovery cycle complete with sanitized outcome detail."""
        ...

    async def score_history(self, wallet: str, *, since_ts: float) -> list[Mapping[str, Any]]:
        """Return bounded historical score evidence for one wallet."""
        ...

    async def latest_candidates(
        self,
        *,
        config_hash: str | None = None,
    ) -> list[Candidate]:
        """Return the latest candidates for an optional config revision."""
        ...

    async def persist_rescore(
        self,
        candidates: list[Candidate],
        config_hash: str,
    ) -> None:
        """Persist a deterministic rescore without starting a new cycle."""
        ...


def discovery_config_hash(config: Any) -> str:
    """Hash only settings that change candidate evidence or score semantics.

    Transport rate limits and scheduler cadence are deliberately excluded:
    changing operational plumbing must not invalidate an otherwise identical
    roster.
    """
    payload = config.model_dump(mode="json") if hasattr(config, "model_dump") else config
    decision_sections = (
        "shortlist",
        "wallet360",
        "recency",
        "market",
        "social",
        "scoring",
        "longitudinal",
        "hysteresis",
        "falcon",
    )
    if isinstance(payload, dict):
        payload = {section: payload.get(section) for section in decision_sections}
    return hashlib.sha256(orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)).hexdigest()


__all__ = ["Candidate", "DiscoveryRepository", "discovery_config_hash"]
