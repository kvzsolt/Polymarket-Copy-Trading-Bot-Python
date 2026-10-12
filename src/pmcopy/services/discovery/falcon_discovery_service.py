"""Live discovery adapter bridging FalconCandidateService to LeaderRegistry.

The new ``FalconCandidateService.discover(funds)`` returns a
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

from collections.abc import Awaitable, Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any, Protocol

from pmcopy.services.discovery.cycle_summary import DiscoveryCycleSummary
from pmcopy.services.discovery.discovery_models import (
    DiscoveryResult,
    RankedTrader,
    RejectedTrader,
)
from pmcopy.services.discovery.models import Candidate, discovery_config_hash
from pmcopy.utils.logger import get_logger

__all__ = ["FalconDiscoveryService", "FalconDiscoveryUnavailable"]

logger = get_logger("FalconDiscoveryService")


class FalconDiscoveryUnavailable(RuntimeError):
    """Raised when a process requests Falcon without composing its client."""


class _FalconCandidateDiscovery(Protocol):
    """Expose the one candidate-service operation required by the adapter."""

    async def discover(
        self,
        funds: Decimal,
        *,
        wallets: Sequence[Mapping[str, Any]] | None = None,
    ) -> DiscoveryResult:
        """Return ranked Falcon candidates for the supplied bankroll.

        Args:
            funds: Follower bankroll the candidates are scored against.
            wallets: Optional targeted rows that replace the leaderboard
                shortlist. ``None`` runs the normal full sweep.
        """
        ...


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


class FalconDiscoveryService:
    """LeaderRegistry-compatible discovery service backed by Falcon.

    Contract satisfied for ``orchestrators.leader_registry.LeaderRegistry``:

    - ``await service.run() -> list[Candidate]`` (accepted candidates only)
    - ``await service.rescore() -> list[Candidate]`` (cached re-rank, no API)
    - ``service.config_hash -> str`` (roster scoping key)
    - ``service.last_cycle_degraded`` (absence-demotion gate)

    ``last_cycle_summary`` is an additive diagnostic: the return value stays
    accepted-only, so the accept/reject histogram of the last cycle is
    published here and logged as ``discovery_cycle_summary``.
    """

    def __init__(
        self,
        *,
        service: _FalconCandidateDiscovery | None,
        repository: Any,
        config: Any,
        get_funds: Callable[[], Awaitable[Decimal | None]],
        logger_instance: Any | None = None,
    ) -> None:
        self._service = service
        self._repository = repository
        self._config = config
        self._get_funds = get_funds
        self._logger = logger_instance or logger
        self._config_hash = discovery_config_hash(getattr(config, "discovery", config))
        self.last_cycle_degraded: bool = False
        self.last_cycle_summary: DiscoveryCycleSummary | None = None
        self.last_cycle_id: str | None = None
        self.last_evaluations: tuple[Candidate, ...] = ()

    def with_funds(
        self, get_funds: Callable[[], Awaitable[Decimal | None]]
    ) -> FalconDiscoveryService:
        """Share the provider but evaluate copyability against one follower bankroll."""
        return FalconDiscoveryService(
            service=self._service,
            repository=self._repository,
            config=self._config,
            get_funds=get_funds,
            logger_instance=self._logger,
        )

    @property
    def config_hash(self) -> str:
        """Return the decision-config revision used to scope roster rows."""
        return self._config_hash

    async def persisted_wallets(self) -> list[str]:
        """Return the wallets accepted by the latest persisted cycle.

        Reads the durable roster only; no provider call is made. The startup
        burst uses this to re-check known-good wallets before paying for a full
        leaderboard sweep.

        Returns:
            Normalized wallet addresses, highest persisted score first, or an
            empty list when no completed cycle exists for this config revision.
        """
        stored = await self._repository.latest_candidates(config_hash=self._config_hash)
        wallets: list[str] = []
        for candidate in stored:
            wallet = str(candidate.wallet or "").strip().lower()
            if wallet and wallet not in wallets:
                wallets.append(wallet)
        return wallets

    async def run(self, *, wallets: Sequence[str] | None = None) -> list[Candidate]:
        """Run one Falcon-backed discovery cycle and persist it.

        Reads the operator bankroll via the injected read-only
        ``get_funds`` callable (the first runtime user's live USDC
        balance). A missing/non-positive bankroll fails closed: the cycle
        is recorded as ``failed`` and ``last_cycle_degraded`` is set so the
        roster is not demoted on missing evidence rather than bad evidence.

        Args:
            wallets: Optional targeted wallet list. When supplied, the cycle
                re-checks exactly those wallets against fresh evidence instead
                of sweeping the provider leaderboard, reusing the H-Score
                stored for each wallet. Persistence is unchanged either way, so
                tenure and hysteresis reconcile as they do for a sweep.

        Returns:
            The accepted candidates of this cycle, highest score first.
        """
        self.last_cycle_degraded = False
        self.last_cycle_summary = None
        self.last_evaluations = ()
        cycle_id = await self._repository.begin_cycle(self._config_hash)
        self.last_cycle_id = cycle_id
        candidates: list[Candidate] = []
        try:
            if self._service is None:
                raise FalconDiscoveryUnavailable("Falcon discovery is not enabled for this process")
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
            targeted_rows = await self._targeted_rows(wallets) if wallets else None
            if targeted_rows is None:
                result: DiscoveryResult = await self._service.discover(Decimal(str(funds)))
            else:
                result = await self._service.discover(
                    Decimal(str(funds)),
                    wallets=targeted_rows,
                )
            accepted = [_ranked_to_candidate(item) for item in result.ranked]
            rejected = [_rejected_to_candidate(item) for item in result.rejected]
            candidates = accepted + rejected
            summary = DiscoveryCycleSummary.from_reasons(
                accepted=len(accepted),
                rejected_reasons=(item.reasons for item in result.rejected),
            )
            self.last_cycle_summary = summary
            self._logger.info(
                "discovery_cycle_summary",
                extra={
                    **summary.as_log_extras(),
                    "discovery_cycle_mode": "targeted" if targeted_rows is not None else "sweep",
                },
            )
            accepted.sort(key=lambda c: (-c.score, c.wallet))
            await self._repository.persist_cycle(cycle_id, candidates, self._config_hash)
            await self._repository.complete_cycle(
                cycle_id,
                status="completed",
                candidate_count=len(candidates),
                kept_count=len(accepted),
            )
            self.last_evaluations = tuple(candidates)
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

    async def _targeted_rows(self, wallets: Sequence[str]) -> list[dict[str, Any]]:
        """Build leaderboard-shaped rows for an explicit wallet list.

        The H-Score of a stored candidate is the one shortlist signal a
        targeted re-check cannot re-derive without the leaderboard, so it is
        read back from the persisted ``hscore`` payload. A wallet with no
        stored row is still re-checked, with an absent H-Score.

        Args:
            wallets: Wallets to re-check, in caller order.

        Returns:
            One row per distinct wallet, in the tolerant leaderboard wire shape
            the candidate service parses.
        """
        reader = getattr(self._repository, "candidate_hscores", None)
        h_scores: dict[str, Any] = {}
        if reader is not None:
            h_scores.update(await reader(wallets, config_hash=self._config_hash))
        else:
            stored = await self._repository.latest_candidates(config_hash=self._config_hash)
            for candidate in stored:
                wallet = str(candidate.wallet or "").strip().lower()
                payload = candidate.hscore if isinstance(candidate.hscore, Mapping) else {}
                if wallet and wallet not in h_scores:
                    h_scores[wallet] = payload.get("h_score")
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for item in wallets:
            wallet = str(item or "").strip().lower()
            if not wallet or wallet in seen:
                continue
            seen.add(wallet)
            row: dict[str, Any] = {"wallet": wallet}
            h_score = h_scores.get(wallet)
            if h_score is not None:
                row["h_score"] = h_score
            rows.append(row)
        return rows

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
