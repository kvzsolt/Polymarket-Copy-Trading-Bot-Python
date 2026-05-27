import asyncio
import json
import os
import time
from pathlib import Path

from config.runtime_env import load_runtime_env

load_runtime_env(override_secrets=True)

from config.env import rehydrate_env
from config.db import connect_db
from config.app_config import AppConfig, load_config
from services.traderDiscoveryService import TraderDiscoveryService
from orchestrators.leader_registry import LeaderRegistry
from storage.postgres.connection import get_pool
from storage.postgres.leaders_repo import LeadersRepo
from utils.logger import get_logger

# INF-06: structured logger so a long-running daemon doesn't bypass the
# project's centralised logging configuration. The script can still be
# tailed with ``-c "import logging; logging.basicConfig(level=logging.INFO)"``
# during local development.
logger = get_logger("TraderFinder")

OUTPUT_JSON = Path("data/followable_traders.json")
OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

TARGET_COUNT = int(os.getenv("FIND_TRADERS_TARGET", "6"))
REFRESH_SECONDS = int(os.getenv("FIND_TRADERS_REFRESH_SECONDS", "60"))


def snapshot_payload(state, discovery, config: AppConfig):
    rows = []
    for cand in state.discovered:
        profile = discovery.get_cached_profile(cand.leader_id)
        rows.append(
            {
                "wallet": cand.leader_id,
                "active": cand.active,
                "score": cand.score,
                "recent_trades": cand.recent_trades,
                "rejection_reason": cand.rejection_reason,
                "overall_gain": getattr(profile, "overall_gain", None),
                "win_rate": getattr(profile, "win_rate", None),
                "total_positions": getattr(profile, "total_positions", None),
                "active_positions": getattr(profile, "active_positions", None),
                "name": getattr(profile, "name", ""),
            }
        )
    rows.sort(key=lambda r: ((not r["active"]), -(r["score"] or 0.0), -(r["recent_trades"] or 0), r["wallet"]))
    return {
        "generated_at": time.time(),
        "target_count": TARGET_COUNT,
        "refresh_seconds": REFRESH_SECONDS,
        "filters_used_exactly_as_implemented": {
            "min_pnl": config.discovery.min_leader_pnl,
            "min_win_rate": config.discovery.min_leader_win_rate,
            "min_positions": config.discovery.min_leader_positions,
            "auto_discover_min_score": config.leader.auto_discover_min_score,
            "discovery_promote_to_active": config.discovery.discovery_promote_to_active,
            "auto_discover_top_n": config.leader.auto_discover_top_n,
            "min_active_positions": config.discovery.min_active_positions,
        },
        "not_implemented_filters": [
            "max hold duration < 12h",
            "max 50 trades/day",
        ],
        "mode": state.mode,
        "active_monitored": state.active_monitored,
        "candidate_count": len(state.discovered),
        "candidates": rows,
    }


async def main():
    rehydrate_env(load_dotenv_file=False)
    await connect_db()

    config = load_config()
    discovery = TraderDiscoveryService()
    pool = await get_pool()
    repo = LeadersRepo(pool)
    registry = LeaderRegistry(discovery, repo, config)

    logger.info(
        "finder started | target=%d refresh=%ds",
        TARGET_COUNT, REFRESH_SECONDS,
    )
    logger.info(
        "finder using implemented filters only: pnl / win rate / "
        "positions / active positions / composite score"
    )

    while True:
        state = await registry.refresh([])
        payload = snapshot_payload(state, discovery, config)
        OUTPUT_JSON.write_text(json.dumps(payload, indent=2))

        active = state.active_monitored
        logger.info(
            "finder snapshot | active=%d candidates=%d mode=%s",
            len(active), len(state.discovered), state.mode,
        )
        for wallet in active[:TARGET_COUNT]:
            profile = discovery.get_cached_profile(wallet)
            if profile:
                wr = profile.win_rate if profile.win_rate <= 1.0 else profile.win_rate / 100.0
                logger.info(
                    "finder leader=%s score=%.3f pnl=%.2f wr=%.3f positions=%d",
                    wallet,
                    profile.composite_score,
                    profile.overall_gain,
                    wr,
                    profile.total_positions,
                )

        if len(active) >= TARGET_COUNT:
            logger.info(
                "finder success | found=%d active followable traders",
                len(active),
            )
            return

        await asyncio.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
