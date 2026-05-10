from __future__ import annotations

import time
from dataclasses import dataclass, field

from config.app_config import AppConfig
from config.env import is_valid_eth_address
from services.discovery.permission_ladder import (
    FollowTier,
    PromotionThresholds,
    evaluate_tier,
)
from services.metrics import metrics
from storage.sqlite.leaders_repo import LeaderCandidateRecord
from utils.logger import get_logger

from services.healthServer import ReadinessTracker

logger = get_logger("LeaderRegistry")


@dataclass
class LeaderCandidate:
    leader_id: str
    score: float
    recent_trades: int
    active: bool
    rejection_reason: str | None = None
    follow_tier: str = "watchlist"


@dataclass
class LeaderRegistryState:
    configured: list[str] = field(default_factory=list)
    discovered: list[LeaderCandidate] = field(default_factory=list)
    active_monitored: list[str] = field(default_factory=list)
    mode: str = "configured"  # configured | discovery_only | mixed


class LeaderRegistry:
    def __init__(
        self,
        discovery_service,
        leaders_repo,
        config: AppConfig,
        readiness_tracker: ReadinessTracker | None = None,
        logger_instance=None,
    ):
        self.discovery_service = discovery_service
        self.leaders_repo = leaders_repo
        self.config = config
        self.readiness = readiness_tracker or ReadinessTracker()
        self.logger = logger_instance or logger

    def _evaluate_leader_tier(self, wallet: str, score: float, has_sufficient_coverage: bool) -> FollowTier:
        """Evaluate and persist tier for a discovered leader."""
        tier_rec = self.leaders_repo.get_tier(wallet)
        current_tier = FollowTier(tier_rec.follow_tier) if tier_rec.follow_tier in {t.value for t in FollowTier} else FollowTier.WATCHLIST
        seconds_in_tier = int(time.time() - tier_rec.tier_changed_at) if tier_rec.tier_changed_at else 0

        thresholds = PromotionThresholds(
            promote_to_paper=self.config.leader.tier_promote_to_paper,
            promote_to_live=self.config.leader.tier_promote_to_live,
            demote_to_paper=self.config.leader.tier_demote_to_paper,
            demote_to_watchlist=self.config.leader.tier_demote_to_watchlist,
            min_paper_seconds=self.config.leader.tier_min_paper_seconds,
        )

        decision = evaluate_tier(
            leader_id=wallet,
            current_tier=current_tier,
            aggregate_score=score,
            has_sufficient_coverage=has_sufficient_coverage,
            seconds_in_current_tier=seconds_in_tier,
            thresholds=thresholds,
        )

        if decision.changed:
            self.leaders_repo.update_tier(wallet, decision.recommended_tier.value, time.time())
            self.logger.info(
                "leader_tier_changed",
                extra={
                    "leader_id": wallet,
                    "from_tier": current_tier.value,
                    "to_tier": decision.recommended_tier.value,
                    "reason": decision.reason,
                },
            )

        return decision.recommended_tier

    async def refresh(self, configured: list[str] | None = None) -> LeaderRegistryState:
        configured = configured or []
        configured_clean = [str(w).lower() for w in configured if str(w).strip()]
        invalid = [w for w in configured_clean if not is_valid_eth_address(w)]
        if invalid:
            raise ValueError(
                "Invalid configured leader wallets: " + ", ".join(invalid)
            )

        discovery_enabled = self.config.discovery.enable_trader_discovery
        if configured_clean and not discovery_enabled:
            metrics.set_gauge("monitored_leaders", float(len(configured_clean)))
            if self.readiness is not None:
                self.readiness.update(monitored_leader_count=len(configured_clean), mode="configured")
            return LeaderRegistryState(
                configured=configured_clean,
                discovered=[],
                active_monitored=list(configured_clean),
                mode="configured",
            )

        self.logger.info("discovery_refresh_started")
        metrics.inc("discovery_refresh_started_total")

        candidates: list[LeaderCandidate] = []
        try:
            discovered_profiles = await self.discovery_service.discover_candidates()
            min_score = self.config.leader.auto_discover_min_score
            min_positions = self.config.discovery.min_leader_positions
            min_active_positions = self.config.discovery.min_active_positions

            for profile in discovered_profiles:
                reason = None
                active = True
                if profile.total_positions < min_positions:
                    active = False
                    reason = "too_few_positions"
                elif profile.active_positions < min_active_positions:
                    active = False
                    reason = "inactive"
                elif profile.composite_score < min_score:
                    active = False
                    reason = "low_score"

                # Evaluate tier via permission ladder
                has_coverage = profile.total_positions >= min_positions
                tier = self._evaluate_leader_tier(
                    wallet=str(profile.wallet).lower(),
                    score=float(profile.composite_score),
                    has_sufficient_coverage=has_coverage and active,
                )

                candidates.append(
                    LeaderCandidate(
                        leader_id=str(profile.wallet).lower(),
                        score=float(profile.composite_score),
                        recent_trades=int(profile.total_positions),
                        active=active,
                        rejection_reason=reason,
                        follow_tier=tier.value,
                    )
                )

            self.leaders_repo.upsert_candidates(
                [
                    LeaderCandidateRecord(
                        leader_id=c.leader_id,
                        score=c.score,
                        recent_trades=c.recent_trades,
                        active=c.active,
                        rejection_reason=c.rejection_reason,
                        source="discovery",
                    )
                    for c in candidates
                ]
            )

            self.logger.info("discovery_refresh_succeeded", extra={"candidate_count": len(candidates)})
            metrics.inc("discovery_refresh_succeeded_total")
            metrics.set_gauge("discovery_candidates", float(len(candidates)))
        except Exception as exc:
            self.logger.warning("discovery_refresh_failed", extra={"error": repr(exc)})
            metrics.inc("discovery_refresh_failed_total")
            candidates = []

        active_discovered = [c.leader_id for c in candidates if c.active]

        mode = "configured"
        active_monitored: list[str] = []

        if configured_clean:
            active_monitored = list(configured_clean)
            stale_configured = [w for w in configured_clean if w not in active_discovered]
            for stale in stale_configured:
                self.logger.warning("configured_leader_stale", extra={"leader_id": stale})
            if active_discovered:
                mode = "mixed"
        else:
            mode = "discovery_only"
            promote = self.config.discovery.discovery_promote_to_active
            if promote:
                top_n = self.config.leader.auto_discover_top_n
                ranked = sorted(
                    [c for c in candidates if c.active],
                    key=lambda x: (-x.score, -x.recent_trades, x.leader_id),
                )
                active_monitored = [c.leader_id for c in ranked[:top_n]]
            else:
                active_monitored = []
                self.logger.error(
                    "No leaders can be monitored: LEADER_WALLETS is empty and "
                    "DISCOVERY_PROMOTE_TO_ACTIVE is not enabled. Either set "
                    "LEADER_WALLETS=0x... in your .env or set "
                    "LEADER_AUTO=true (or DISCOVERY_PROMOTE_TO_ACTIVE=true) to "
                    "auto-promote discovered leaders."
                )

        if not active_monitored:
            self.logger.warning("no_leaders_monitored", extra={"mode": mode})
            metrics.set_gauge("monitored_leaders", 0)
        else:
            metrics.set_gauge("monitored_leaders", float(len(active_monitored)))

        if self.readiness is not None:
            self.readiness.update(monitored_leader_count=len(active_monitored), mode=mode)

        return LeaderRegistryState(
            configured=configured_clean,
            discovered=candidates,
            active_monitored=active_monitored,
            mode=mode,
        )
