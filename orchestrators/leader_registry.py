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
from storage.postgres.leaders_repo import LeaderCandidateRecord
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

    async def _evaluate_leader_tier(self, wallet: str, score: float, has_sufficient_coverage: bool) -> FollowTier:
        """Evaluate and persist tier for a discovered leader."""
        tier_rec = await self.leaders_repo.get_tier(wallet)
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
            await self.leaders_repo.update_tier(wallet, decision.recommended_tier.value, time.time())
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

        candidates: list[LeaderCandidate] = []
        if self.config.discovery.enable_trader_discovery:
            self.logger.info("discovery_refresh_started")
            metrics.inc("discovery_refresh_started_total")
            try:
                discovered_profiles = await self.discovery_service.discover_candidates()
            except Exception as exc:
                self.logger.warning("discovery_refresh_failed", extra={"error": repr(exc)})
                metrics.inc("discovery_refresh_failed_total")
                discovered_profiles = []

            for profile in discovered_profiles:
                candidates.append(
                    LeaderCandidate(
                        leader_id=str(profile.wallet).lower(),
                        score=float(profile.composite_score),
                        recent_trades=int(profile.total_positions),
                        active=False,
                        rejection_reason=None,
                        follow_tier="shadow",
                    )
                )

            await self.leaders_repo.upsert_candidates(
                [
                    LeaderCandidateRecord(
                        leader_id=c.leader_id,
                        score=c.score,
                        recent_trades=c.recent_trades,
                        active=False,
                        rejection_reason=c.rejection_reason,
                        source="discovery",
                    )
                    for c in candidates
                ]
            )

            self.logger.info("discovery_refresh_succeeded", extra={"candidate_count": len(candidates)})
            metrics.inc("discovery_refresh_succeeded_total")
            metrics.set_gauge("discovery_candidates", float(len(candidates)))

        if configured_clean:
            active_monitored = list(configured_clean)
            mode = "configured"
        else:
            active_monitored = []
            mode = "no_configured_leaders"
            self.logger.warning(
                "No LEADER_WALLETS configured. Discovery will score candidates, "
                "but live copying requires python bot.py rotate-traders or manual "
                "LEADER_WALLETS."
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
