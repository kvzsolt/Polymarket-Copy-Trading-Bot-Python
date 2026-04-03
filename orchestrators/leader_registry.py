from __future__ import annotations

from dataclasses import dataclass, field

from config.env import ENV, is_valid_eth_address
from services.healthServer import update_readiness
from services.metrics import metrics
from storage.sqlite.leaders_repo import LeaderCandidateRecord
from utils.logger import get_logger

logger = get_logger("LeaderRegistry")


@dataclass
class LeaderCandidate:
    leader_id: str
    score: float
    recent_trades: int
    active: bool
    rejection_reason: str | None = None


@dataclass
class LeaderRegistryState:
    configured: list[str] = field(default_factory=list)
    discovered: list[LeaderCandidate] = field(default_factory=list)
    active_monitored: list[str] = field(default_factory=list)
    mode: str = "configured"  # configured | discovery_only | mixed


class LeaderRegistry:
    def __init__(self, discovery_service, leaders_repo, logger_instance=None):
        self.discovery_service = discovery_service
        self.leaders_repo = leaders_repo
        self.logger = logger_instance or logger

    async def refresh(self, configured: list[str] | None = None) -> LeaderRegistryState:
        configured = configured or []
        configured_clean = [str(w).lower() for w in configured if str(w).strip()]
        invalid = [w for w in configured_clean if not is_valid_eth_address(w)]
        if invalid:
            raise ValueError(
                "Invalid configured leader wallets: " + ", ".join(invalid)
            )

        self.logger.info("discovery_refresh_started")
        await metrics.inc("discovery_refresh_started_total")

        candidates: list[LeaderCandidate] = []
        try:
            discovered_profiles = await self.discovery_service.discover_candidates()
            min_score = float(ENV.get("AUTO_DISCOVER_MIN_SCORE", 0.45) or 0.45)
            min_positions = int(ENV.get("MIN_LEADER_POSITIONS", 20) or 20)
            min_active_positions = int(ENV.get("MIN_ACTIVE_POSITIONS", 1) or 1)

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

                candidates.append(
                    LeaderCandidate(
                        leader_id=str(profile.wallet).lower(),
                        score=float(profile.composite_score),
                        recent_trades=int(profile.total_positions),
                        active=active,
                        rejection_reason=reason,
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
            await metrics.inc("discovery_refresh_succeeded_total")
            await metrics.set_gauge("discovery_candidates", float(len(candidates)))
        except Exception as exc:
            self.logger.warning("discovery_refresh_failed", extra={"error": repr(exc)})
            await metrics.inc("discovery_refresh_failed_total")
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
            promote = bool(ENV.get("DISCOVERY_PROMOTE_TO_ACTIVE", False))
            if promote:
                top_n = int(ENV.get("AUTO_DISCOVER_TOP_N", 5) or 5)
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
                    "DISCOVERY_PROMOTE_TO_ACTIVE=true to auto-promote discovered leaders."
                )

        if not active_monitored:
            self.logger.warning("no_leaders_monitored", extra={"mode": mode})
            await metrics.set_gauge("monitored_leaders", 0)
        else:
            await metrics.set_gauge("monitored_leaders", float(len(active_monitored)))

        update_readiness(monitored_leader_count=len(active_monitored))

        return LeaderRegistryState(
            configured=configured_clean,
            discovered=candidates,
            active_monitored=active_monitored,
            mode=mode,
        )
