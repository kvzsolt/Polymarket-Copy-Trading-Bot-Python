import asyncio
import json
import os
import time
from pathlib import Path

from config.runtime_env import load_runtime_env

load_runtime_env(override_secrets=True)

from config.env import ENV, rehydrate_env
from config.db import connect_db, get_db_path
from services.traderDiscoveryService import TraderDiscoveryService
from orchestrators.leader_registry import LeaderRegistry
from storage.sqlite.leaders_repo import LeadersRepo

OUTPUT_JSON = Path("data/followable_traders.json")
OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

TARGET_COUNT = int(os.getenv("FIND_TRADERS_TARGET", "6"))
REFRESH_SECONDS = int(os.getenv("FIND_TRADERS_REFRESH_SECONDS", "60"))


def snapshot_payload(state, discovery):
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
        "db_path": get_db_path(),
        "target_count": TARGET_COUNT,
        "refresh_seconds": REFRESH_SECONDS,
        "filters_used_exactly_as_implemented": {
            "min_pnl": ENV.get("MIN_LEADER_PNL"),
            "min_win_rate": ENV.get("MIN_LEADER_WIN_RATE"),
            "min_positions": ENV.get("MIN_LEADER_POSITIONS"),
            "auto_discover_min_score": ENV.get("AUTO_DISCOVER_MIN_SCORE"),
            "discovery_promote_to_active": ENV.get("DISCOVERY_PROMOTE_TO_ACTIVE"),
            "auto_discover_top_n": ENV.get("AUTO_DISCOVER_TOP_N"),
            "min_active_positions": ENV.get("MIN_ACTIVE_POSITIONS", 1),
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

    discovery = TraderDiscoveryService()
    repo = LeadersRepo(get_db_path())
    registry = LeaderRegistry(discovery, repo)

    print(f"[finder] started: target={TARGET_COUNT} refresh={REFRESH_SECONDS}s")
    print("[finder] using implemented filters only: pnl / win rate / positions / active positions / composite score")

    while True:
        state = await registry.refresh([])
        payload = snapshot_payload(state, discovery)
        OUTPUT_JSON.write_text(json.dumps(payload, indent=2))

        active = state.active_monitored
        print(f"[finder] active={len(active)} candidates={len(state.discovered)} mode={state.mode}")
        for wallet in active[:TARGET_COUNT]:
            profile = discovery.get_cached_profile(wallet)
            if profile:
                wr = profile.win_rate if profile.win_rate <= 1.0 else profile.win_rate / 100.0
                print(
                    f"[finder] leader={wallet} score={profile.composite_score:.3f} "
                    f"pnl={profile.overall_gain:.2f} wr={wr:.3f} positions={profile.total_positions}"
                )

        if len(active) >= TARGET_COUNT:
            print(f"[finder] success: found {len(active)} active followable traders")
            return

        await asyncio.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
