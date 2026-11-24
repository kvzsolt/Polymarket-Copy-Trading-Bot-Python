"""Orchestrate Falcon discovery and durable leader-roster hysteresis."""

from __future__ import annotations

import time
from dataclasses import dataclass, field, replace
from typing import Any

from pmcopy.config.app_config import AppConfig
from pmcopy.config.env import is_valid_eth_address
from pmcopy.integrations.falcon.errors import FalconError, FalconPermissionError
from pmcopy.services.discovery.models import Candidate
from pmcopy.services.observability.health_server import ReadinessTracker
from pmcopy.services.observability.metrics import metrics
from pmcopy.storage.postgres.leaders_repo import LeaderRosterRecord, LeadersRepo
from pmcopy.utils.logger import get_logger, redact_error_detail

logger = get_logger("LeaderRegistry")


def apply_hysteresis(
    current_tier: str,
    score: float,
    dwell_seconds: float,
    consecutive_cycles: int,
    config: Any,
) -> str:
    """Return a tier recommendation without mutating persistence."""
    if current_tier == "active":
        if (
            score < config.demote_score
            and dwell_seconds >= config.min_dwell_seconds
            and consecutive_cycles >= config.min_tenure_cycles
        ):
            return "watchlist"
        return "active"
    if score >= config.promote_score and consecutive_cycles >= config.min_tenure_cycles:
        return "active"
    return "watchlist"


def _cycle_summary_extras(discovery_service: Any) -> dict[str, Any]:
    """Return the accept/reject extras published by the discovery service.

    The service is duck-typed, so a fake or legacy implementation that
    publishes no summary contributes no extras rather than failing the log.

    Args:
        discovery_service: The injected discovery service for this refresh.

    Returns:
        Structured logging extras, empty when no summary is available.
    """
    summary = getattr(discovery_service, "last_cycle_summary", None)
    if summary is None:
        return {}
    return {
        "accepted_count": summary.accepted,
        "rejected_count": summary.rejected,
        "rejection_reasons": dict(summary.rejection_reasons),
    }


@dataclass(frozen=True, slots=True)
class LeaderCandidate:
    """Describe one evaluated leader and its roster recommendation."""

    leader_id: str
    score: float
    recent_trades: int
    active: bool
    rejection_reason: str | None = None
    follow_tier: str = "watchlist"


@dataclass(slots=True)
class LeaderRegistryState:
    """Expose one sanitized registry refresh result."""

    configured: list[str] = field(default_factory=list)
    discovered: list[LeaderCandidate] = field(default_factory=list)
    active_monitored: list[str] = field(default_factory=list)
    mode: str = "configured"


class LeaderRegistry:
    """Coordinate Falcon discovery with the durable active-leader roster."""

    def __init__(
        self,
        discovery_service: Any,
        leaders_repo: LeadersRepo,
        config: AppConfig,
        readiness_tracker: ReadinessTracker | None = None,
        logger_instance: Any | None = None,
    ) -> None:
        self.discovery_service = discovery_service
        self.leaders_repo = leaders_repo
        self.config = config
        self.readiness = readiness_tracker or ReadinessTracker()
        self.logger = logger_instance or logger

    async def persisted_wallets(self) -> list[str]:
        """Return the wallets accepted by the latest persisted discovery cycle.

        Delegates to the duck-typed discovery service, which reads its durable
        roster without any provider call. A service that does not implement the
        port reports nothing rather than failing the caller.

        Returns:
            Normalized wallet addresses, or an empty list when none are stored.
        """
        if not callable(getattr(self.discovery_service, "persisted_wallets", None)):
            return []
        wallets = await self.discovery_service.persisted_wallets()
        return [str(wallet) for wallet in wallets]

    async def refresh(
        self,
        configured: list[str] | None = None,
        *,
        force_discovery: bool = False,
        target_wallets: list[str] | None = None,
    ) -> LeaderRegistryState:
        """Refresh discovery and return the effective monitored leader set.

        Args:
            configured: Manual roster for this process, if any.
            force_discovery: Run provider discovery even when its scheduler
                is off.
            target_wallets: Optional wallets to re-check instead of sweeping
                the provider shortlist. An empty or absent list runs the normal
                full sweep. Cycle persistence, tenure and hysteresis are
                identical in both modes.

        Returns:
            The sanitized registry state for this refresh.

        Raises:
            FalconError: If forced discovery encounters a permanent provider
                failure, before a persisted roster can be used.
        """
        configured_clean = _configured_wallets(configured or [])
        discovery = self.config.discovery
        candidates: list[Candidate] = []
        credential_configured = bool(discovery.api.has_token)
        should_run = (
            force_discovery or (discovery.scheduler.enabled and not configured_clean)
        ) and credential_configured
        if should_run:
            self.logger.info("discovery_refresh_started")
            metrics.inc("discovery_refresh_started_total")
            try:
                candidates = (
                    await self.discovery_service.run(wallets=list(target_wallets))
                    if target_wallets
                    else await self.discovery_service.run()
                )
                await self._reconcile_roster(
                    candidates,
                    # ``discovery_service`` is duck-typed, so read the flag
                    # defensively: a service that reports nothing is reporting
                    # a healthy cycle.
                    allow_absence_demotion=not getattr(
                        self.discovery_service, "last_cycle_degraded", False
                    ),
                )
            except Exception as exc:
                self.logger.warning(
                    "discovery_refresh_failed",
                    extra={
                        "error": (
                            type(exc).__name__
                            if isinstance(exc, FalconError)
                            else redact_error_detail(exc)
                        ),
                        **(
                            {"falcon_agent_id": exc.agent_id}
                            if isinstance(exc, FalconError) and exc.agent_id is not None
                            else {}
                        ),
                    },
                )
                metrics.inc("discovery_refresh_failed_total")
                if force_discovery and isinstance(exc, FalconError) and not exc.retryable:
                    message = f"Falcon discovery failed: {type(exc).__name__}"
                    if isinstance(exc, FalconPermissionError):
                        message += "; verify Falcon plan access for the denied agent"
                    raise exc.with_context(message) from None
            else:
                self.logger.info(
                    "discovery_refresh_succeeded",
                    extra={
                        "candidate_count": len(candidates),
                        **_cycle_summary_extras(self.discovery_service),
                    },
                )
                metrics.inc("discovery_refresh_succeeded_total")
                metrics.set_gauge("discovery_candidates", float(len(candidates)))
        elif force_discovery and not credential_configured:
            raise RuntimeError("Discovery was requested but discovery.api.token is not set")

        auto_active = (
            await self.leaders_repo.list_active(
                limit=discovery.scheduler.max_active_leaders,
                config_hash=str(self.discovery_service.config_hash),
            )
            if discovery.scheduler.start_from_roster and not configured_clean
            else []
        )
        active_monitored = _ordered_unique([*configured_clean, *auto_active])
        if configured_clean and auto_active:
            mode = "mixed"
        elif configured_clean:
            mode = "configured"
        elif auto_active:
            mode = "discovery_only"
        else:
            mode = "no_active_leaders"

        discovered = [
            LeaderCandidate(
                leader_id=candidate.wallet,
                score=candidate.score,
                recent_trades=len(candidate.recent_trades),
                active=candidate.wallet in auto_active,
                follow_tier=("active" if candidate.wallet in auto_active else "watchlist"),
            )
            for candidate in candidates
        ]
        metrics.set_gauge("monitored_leaders", float(len(active_monitored)))
        if not active_monitored:
            self.logger.warning("no_leaders_monitored", extra={"mode": mode})
        self.readiness.update(
            monitored_leader_count=len(active_monitored),
            mode=mode,
        )
        return LeaderRegistryState(
            configured=configured_clean,
            discovered=discovered,
            active_monitored=active_monitored,
            mode=mode,
        )

    async def rescore(
        self,
        configured: list[str] | None = None,
    ) -> LeaderRegistryState:
        """Rescore persisted candidates without provider network calls."""
        candidates = await self.discovery_service.rescore()
        if candidates:
            # Cached re-rank of stored snapshots: refresh scores and ranking,
            # but do not let it count toward promotion/demotion tenure.
            await self._reconcile_roster(candidates, advance_tenure=False)
        configured_clean = _configured_wallets(configured or [])
        active = await self.leaders_repo.list_active(
            limit=self.config.discovery.scheduler.max_active_leaders,
            config_hash=str(self.discovery_service.config_hash),
        )
        monitored = _ordered_unique([*configured_clean, *active])
        mode = (
            "mixed"
            if configured_clean and active
            else "configured"
            if configured_clean
            else "discovery_only"
            if active
            else "no_active_leaders"
        )
        self.readiness.update(monitored_leader_count=len(monitored), mode=mode)
        return LeaderRegistryState(
            configured=configured_clean,
            discovered=[
                LeaderCandidate(
                    leader_id=candidate.wallet,
                    score=candidate.score,
                    recent_trades=len(candidate.recent_trades),
                    active=candidate.wallet in active,
                    follow_tier=("active" if candidate.wallet in active else "watchlist"),
                )
                for candidate in candidates
            ],
            active_monitored=monitored,
            mode=mode,
        )

    async def reconcile_candidates(self, candidates: list[Candidate]) -> None:
        """Persist one explicitly supplied candidate evaluation set."""
        await self._reconcile_roster(candidates)

    async def _reconcile_roster(
        self,
        candidates: list[Candidate],
        *,
        advance_tenure: bool = True,
        allow_absence_demotion: bool = True,
    ) -> None:
        """Fold a candidate set into the persisted roster.

        ``advance_tenure=False`` refreshes scores and ranking but freezes the
        hysteresis counters. Use it for cached rescores, which re-rank stored
        snapshots without observing the wallet again — letting those count as
        cycles would satisfy ``min_tenure_cycles`` with no fresh evidence.

        ``allow_absence_demotion=False`` marks a *degraded* cycle whose
        candidate set is known to be incomplete (e.g. an H-Score sort mode
        failed). Wallets present in it still score, rank, and promote
        normally; active wallets missing from it are carried forward untouched
        instead of being scored 0.0, so a persistent one-mode outage cannot
        demote the leaders that mode surfaces.
        """
        now = time.time()
        config = self.config.discovery.hysteresis
        config_hash = str(self.discovery_service.config_hash)
        existing = {record.wallet: record for record in await self.leaders_repo.list_roster()}
        current = {candidate.wallet: candidate for candidate in candidates}
        updates: dict[str, LeaderRosterRecord] = {}

        # Active rows are evaluated first so valid demotions release promotion
        # slots in the same atomic roster update.
        for wallet, record in existing.items():
            if record.tier != "active":
                continue
            candidate = current.get(wallet)
            if candidate is None and not allow_absence_demotion:
                # Incomplete cycle: this wallet was never looked at, so
                # absence is missing evidence, not evidence of decay. Carry
                # the row forward verbatim — including its old ``config_hash``,
                # which ``list_active`` filters on: the frozen score was never
                # recomputed under the current config, so restamping it here
                # would re-qualify stale data the config change invalidated.
                # It stays in ``updates`` so it keeps holding its promotion
                # slot and the watchlist pass below leaves it alone — dropping
                # it would let that pass re-tier an active leader at score 0.0.
                updates[wallet] = record
                continue
            score = candidate.score if candidate is not None else 0.0
            demote_streak = (
                (record.demote_streak + 1 if score < config.demote_score else 0)
                if advance_tenure
                else record.demote_streak
            )
            next_tier = (
                apply_hysteresis(
                    "active",
                    score,
                    max(0.0, now - record.tier_changed_at),
                    demote_streak,
                    config,
                )
                if advance_tenure
                else record.tier
            )
            changed = next_tier != record.tier
            updates[wallet] = replace(
                record,
                tier=next_tier,
                score=score,
                base_score=(candidate.base_score if candidate is not None else 0.0),
                longitudinal_score=(
                    candidate.longitudinal_score
                    if candidate is not None
                    else record.longitudinal_score
                ),
                cycles_in_tier=(
                    0
                    if changed
                    else record.cycles_in_tier + 1
                    if advance_tenure
                    else record.cycles_in_tier
                ),
                promote_streak=0,
                demote_streak=0 if changed else demote_streak,
                tier_changed_at=now if changed else record.tier_changed_at,
                last_seen_at=now if candidate is not None else record.last_seen_at,
                config_hash=config_hash,
            )
            if changed:
                self._log_tier_change(wallet, "active", next_tier, score)

        active_count = sum(record.tier == "active" for record in updates.values())
        watchlist_wallets = sorted(
            set(existing) | set(current),
            key=lambda wallet: (
                -current[wallet].score if wallet in current else 0.0,
                wallet,
            ),
        )
        for wallet in watchlist_wallets:
            if wallet in updates:
                continue
            candidate = current.get(wallet)
            previous = existing.get(wallet)
            score = candidate.score if candidate is not None else 0.0
            previous_promote_streak = previous.promote_streak if previous else 0
            promote_streak = (
                (previous_promote_streak + 1 if score >= config.promote_score else 0)
                if advance_tenure
                else previous_promote_streak
            )
            recommended = (
                previous.tier
                if previous is not None and not advance_tenure
                else apply_hysteresis(
                    "watchlist",
                    score,
                    max(0.0, now - previous.tier_changed_at) if previous else 0.0,
                    promote_streak,
                    config,
                )
            )
            if (
                recommended == "active"
                and active_count >= self.config.discovery.scheduler.max_active_leaders
            ):
                recommended = "watchlist"
            changed = previous is not None and recommended != previous.tier
            newly_promoted = recommended == "active" and (
                previous is None or previous.tier != "active"
            )
            if newly_promoted:
                active_count += 1
            record = LeaderRosterRecord(
                wallet=wallet,
                tier=recommended,
                score=score,
                base_score=candidate.base_score if candidate is not None else 0.0,
                longitudinal_score=(
                    candidate.longitudinal_score
                    if candidate is not None
                    else previous.longitudinal_score
                    if previous
                    else None
                ),
                cycles_in_tier=(
                    0
                    if changed or previous is None
                    else previous.cycles_in_tier + 1
                    if advance_tenure
                    else previous.cycles_in_tier
                ),
                promote_streak=0 if newly_promoted else promote_streak,
                demote_streak=0,
                tier_changed_at=(now if changed or previous is None else previous.tier_changed_at),
                first_promoted_at=(
                    now
                    if newly_promoted and (previous is None or previous.first_promoted_at is None)
                    else previous.first_promoted_at
                    if previous
                    else None
                ),
                last_seen_at=now
                if candidate is not None
                else previous.last_seen_at
                if previous
                else None,
                config_hash=config_hash,
            )
            updates[wallet] = record
            if newly_promoted:
                self._log_tier_change(wallet, "watchlist", "active", score)

        await self.leaders_repo.save_roster(list(updates.values()))

    def _log_tier_change(
        self,
        wallet: str,
        previous: str,
        current: str,
        score: float,
    ) -> None:
        self.logger.info(
            "leader_tier_changed",
            extra={
                "leader_id": wallet,
                "from_tier": previous,
                "to_tier": current,
                "score": score,
            },
        )
        metrics.inc("leader_tier_changed_total")


def _configured_wallets(wallets: list[str]) -> list[str]:
    cleaned = _ordered_unique(
        [str(wallet).strip().lower() for wallet in wallets if str(wallet).strip()]
    )
    invalid = [wallet for wallet in cleaned if not is_valid_eth_address(wallet)]
    if invalid:
        raise ValueError("Invalid configured leader wallets: " + ", ".join(invalid))
    return cleaned


def _ordered_unique(wallets: list[str]) -> list[str]:
    return list(dict.fromkeys(wallet for wallet in wallets if wallet))


__all__ = [
    "LeaderCandidate",
    "LeaderRegistry",
    "LeaderRegistryState",
    "apply_hysteresis",
]
