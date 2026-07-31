"""Live discovery adapter bridging HeisenbergCandidateService to LeaderRegistry.

The new ``HeisenbergCandidateService.discover(funds)`` returns a
``DiscoveryResult`` of ``RankedTrader``/``RejectedTrader``. The live
``LeaderRegistry`` and the persisted roster, however, consume the older
``Candidate`` row shape and the ``DiscoveryRepository`` cycle protocol
(``begin_cycle``/``persist_cycle``/``complete_cycle``/``rescore``).

This adapter is the anti-corruption layer between those two shapes so the
existing orchestration (``runtime_discovery.discovery_refresh_loop`` /
``LeaderRegistry.refresh`` / ``LeaderRegistry.rescore``), the
``DiscoveryRepo`` persistence, and the database schema stay unchanged. No
DB migration is introduced.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

from services.discovery.discovery_models import (
    DiscoveryResult,
    RankedTrader,
    RejectedTrader,
)
from services.discovery.models import Candidate, discovery_config_hash

__all__ = ["HeisenbergDiscoveryService"]


def _ranked_to_candidate(item: RankedTrader) -> Candidate:
    """Map a passing ``RankedTrader`` to the persisted ``Candidate`` row.

    ``base_score`` and ``score`` carry the same composite value: the new
    service has no separate "base" vs "longitudinal" decomposition, so both
    are set to the bankroll-aware composite score. ``hscore`` keeps the
    H-Score and leaderboard fields for diagnostic persistence; ``metrics``
    carries the bankroll-fit and evidence numbers the report renders.
    """
    score = float(item.score)
    return Candidate(
        wallet=item.wallet,
        hscore={
            "h_score": str(item.h_score),
            "source": item.source,
        },
        sub_scores={key: float(value) for key, value in item.components.items()},
        metrics={key: value for key, value in item.metrics.items()},
        base_score=score,
        longitudinal_score=None,
        score=score,
        rejected_reason=None,
    )


def _rejected_to_candidate(item: RejectedTrader) -> Candidate:
    return Candidate(
        wallet=item.wallet,
        hscore={
            "h_score": str(item.h_score),
            "source": item.source,
        },
        metrics={key: value for key, value in item.metrics.items()},
        base_score=0.0,
        longitudinal_score=None,
        score=0.0,
        rejected_reason=";".join(item.reasons) if item.reasons else "rejected",
    )


class HeisenbergDiscoveryService:
    """LeaderRegistry-compatible discovery service backed by Heisenberg.

    Contract satisfied for ``orchestrators.leader_registry.LeaderRegistry``:

    - ``await service.run() -> list[Candidate]`` (accepted candidates only)
    - ``await service.rescore() -> list[Candidate]`` (cached re-rank, no API)
    - ``service.config_hash -> str`` (roster scoping key)
    - ``service.last_cycle_degraded`` (absence-demotion gate)
    """

    def __init__(
        self,
        *,
        service: Any,
        repository: Any,
        config: Any,
        get_funds: Callable[[], Awaitable[Decimal | None]],
    ) -> None:
        self._service = service
        self._repository = repository
        self._config = config
        self._get_funds = get_funds
        self._config_hash = discovery_config_hash(
            getattr(config, "discovery", config)
        )
        self.last_cycle_degraded: bool = False

    @property
    def config_hash(self) -> str:
        return self._config_hash

    async def run(self) -> list[Candidate]:
        """Run one Heisenberg-backed discovery cycle and persist it.

        Reads the operator bankroll via the injected read-only
        ``get_funds`` callable (the first runtime user's live USDC
        balance). A missing/non-positive bankroll fails closed: the cycle
        is recorded as ``failed`` and ``last_cycle_degraded`` is set so the
        roster is not demoted on missing evidence rather than bad evidence.
        """
        self.last_cycle_degraded = False
        cycle_id = await self._repository.begin_cycle(self._config_hash)
        candidates: list[Candidate] = []
        try:
            funds = await self._get_funds()
            if funds is None or Decimal(str(funds)) <= 0:
                self.last_cycle_degraded = True
                await self._repository.complete_cycle(
                    cycle_id,
                    status="failed",
                    candidate_count=0,
                    kept_count=0,
                    error="bankroll_unavailable",
                )
                return []
            result: DiscoveryResult = await self._service.discover(
                Decimal(str(funds))
            )
            accepted = [_ranked_to_candidate(item) for item in result.ranked]
            rejected = [_rejected_to_candidate(item) for item in result.rejected]
            candidates = accepted + rejected
            accepted.sort(key=lambda c: (-c.score, c.wallet))
            await self._repository.persist_cycle(
                cycle_id, candidates, self._config_hash
            )
            await self._repository.complete_cycle(
                cycle_id,
                status="completed",
                candidate_count=len(candidates),
                kept_count=len(accepted),
            )
            return accepted
        except Exception:
            self.last_cycle_degraded = True
            await self._repository.complete_cycle(
                cycle_id,
                status="failed",
                candidate_count=len(candidates),
                kept_count=0,
                error="discovery_cycle_exception",
            )
            raise

    async def rescore(self) -> list[Candidate]:
        """Cached re-rank of the last accepted roster, with no API call.

        Mirrors the prior ``DiscoveryFunnel.rescore`` contract: read the
        stored accepted candidates for the current config hash, re-sort by
        score, and persist the re-rank. Hysteresis tenure is not advanced
        for cached rescoring (handled by ``LeaderRegistry``).
        """
        stored = list(await self._repository.latest_candidates(config_hash=self._config_hash))
        stored.sort(key=lambda c: (-c.score, c.wallet))
        await self._repository.persist_rescore(stored, self._config_hash)
        return stored

    async def discover_candidates(self, limit: int | None = None) -> list[Candidate]:
        """Compatibility-neutral public name used by CLI and startup wiring."""
        candidates = await self.run()
        return candidates if limit is None else candidates[: max(0, int(limit))]